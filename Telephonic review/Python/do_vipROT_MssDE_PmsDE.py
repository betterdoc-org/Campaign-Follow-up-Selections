#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 10:16:57 2024

@author: david.wetzels
"""

## IMPORT PACKAGES
import requests
import os  # create folder for person
import psycopg2  # connection to dataocean package
import pandas as pd  # dataframe package


# ==========================================================================================
# ==========================================================================================


## USER FILTER
filter_user = os.environ.get("USER")

# trello_credentials
if filter_user == "david.wetzels":
    filter_user_credentials = (
        "/Users/david.wetzels/Documents/Credentials/trello_credentials.txt"
    )
elif filter_user == "oumaima.el-bellam":
    filter_user_credentials = (
        "/Users/oumaima.el-bellam/Documents/Credentials/trello_credentials.txt"
    )
# Datei trello_credentials einlesen
with open(filter_user_credentials, "r") as file:
    lines = file.readlines()
    api_key = lines[0].strip()
    token = lines[1].strip()


# dataocean_credentials
if filter_user == "david.wetzels":
    dataocean_credentials = "/Users/david.wetzels/Documents/Credentials/dataozean.txt"
elif filter_user == "oumaima.el-bellam":
    dataocean_credentials = (
        "/Users/oumaima.el-bellam/Documents/Credentials/dataocean.txt"
    )

# connection to dataocean
dataocean_connection = psycopg2.connect(open(dataocean_credentials).read())
dataocean_cursor = dataocean_connection.cursor()


# ==========================================================================================
# ==========================================================================================


# dataocean cases
all_cases = pd.read_sql(
    """
                 select *
        from 
        (
        with borg_data as (
        SELECT
    		distinct stg_borg__inquiries.case_id,
    		row_number () over (partition by stg_borg__inquiries.case_id order by stg_borg__inquiries.voucher_ids[0]::int) as rn,
    		stg_borg__vouchers.client_name as voucher_client_name,
    		stg_borg__vouchers.value as first_voucher_code,
    		stg_borg__inquiry_appointments.state as app_state,
    		case when stg_borg__inquiry_appointments.state like 'call_patient' then dim_clean_borg_results.least_result_date
    			when stg_borg__inquiry_appointments.state like 'sent' then stg_borg__inquiry_appointments.scheduled_date else results_sent_at end as app_date,
    		stg_borg__inquiries.state as korb,
    		date_part('week', current_date) as current_week,
    		date_part('week', case when stg_borg__inquiry_appointments.state like 'call_patient' then dim_clean_borg_results.least_result_date
    							when stg_borg__inquiry_appointments.state like 'sent' then stg_borg__inquiry_appointments.scheduled_date 
    							else results_sent_at end) as week_appdate
    	FROM 
    		staging.stg_borg__inquiries
            left join analytics.dim_clean_borg_results on stg_borg__inquiries.service_id_key_systems = dim_clean_borg_results.service_id_key_systems
    		LEFT JOIN staging.stg_borg__inquiry_appointments on stg_borg__inquiries.id = stg_borg__inquiry_appointments.inquiry_id
    		left join staging.stg_borg__inquiry_physician_contact_entries on stg_borg__inquiry_physician_contact_entries.inquiry_id = stg_borg__inquiries.id
    		left join staging.stg_borg__vouchers on stg_borg__vouchers.id = stg_borg__inquiries.voucher_ids[0]::int
    	WHERE
    		stg_borg__inquiries.state like 'waiting_q3'
    		and stg_borg__inquiries.treatment_type like 'second_opinion_before_surgery'
    		and stg_borg__inquiry_physician_contact_entries.q3_result is null
    		and stg_borg__inquiry_appointments.appointment_type = '3'
    		and stg_borg__inquiry_appointments.state in('sent', 'call_patient')
    		and stg_borg__inquiry_appointments.inquiry_id not in (select inquiry_id 
    																from staging.stg_borg__inquiry_appointments
    																where (appointment_type = '3' 
    																		and state like 'completed'))
    		and stg_borg__vouchers.value like '%VRTC%'
        	and (stg_borg__vouchers.value not like '%-MSSNACHRSO' 
        		and stg_borg__vouchers.value not like '%-BDTC' 
        		and stg_borg__vouchers.value not like 'NUBU-7777' 
        		and stg_borg__vouchers.value not like '%AXAP-%' 
        		and stg_borg__vouchers.value not like 'ARAG-CM22')
    	ORDER BY
    		app_state, voucher_client_name
    	)
        select
    		(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
    		(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
    		case_id,
    		voucher_client_name as client_name,
    		first_voucher_code,
    		null::int as patient_id,
    		0 as control_flag, 
    		1 as count_cases_patient, 
    		null::numeric as reached_patient_flag,
    		null::date as reached_date, 
    		null::text as refusal_reason,
    		null::numeric as opt_out, 
    		null::text as new_case_id,
    		app_state,
    		korb
    	from 
    		borg_data
    	where rn = '1'
    		and ((app_state like 'sent'
    			and date_part('week', current_date) = date_part('week', (app_date + interval '5 weeks'))
    			and date_part('year', (app_date + interval '5 weeks')) = date_part('year', current_date))
    		or (app_state like 'call_patient'
    			and date_part('week', current_date) = date_part('week', (app_date + interval '6 weeks'))
    			and date_part('year', (app_date + interval '6 weeks')) = date_part('year', current_date)))
    	order by app_state, voucher_client_name
        ) MSS
        union all
        select *
        from 
        (
        	with foo as (
        	SELECT
        		services.case_id,
        		payers.name, 
        		services.voucher_code,
        		communication_facts.preferred_language,
        		survey_links.response_status,
        		survey_links.created_at,
        		case when surveys.title like '%PMS%' then surveys.title
        				else null
        				end as title,
        		case when services.case_id in (select survey_links.case_id 
        										from "communication-cs-surveys".survey_links 
        										left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
        										where surveys.title like '%Allrounder%') then 'Allrounder' --Patient hat Status Survey inklusive PREM Link bekommen (Termin unbekannt)
        			 when services.case_id in (select survey_links.case_id 
        			 								from "communication-cs-surveys".survey_links
        			 								left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
        			 								where surveys.title like '%Reha%') then 'Reha' --Patient hat direkt PREM bekommen (Termin war bekannt)
        			 else null
        			 end as case_fb,
        		case when surveys.title like '%PMS%' then current_date - survey_links.created_at::date
        				when surveys.title not like '%PMS%' then current_date - fazit::date
        					end as fb_age, --vor wie vielen Tagen wurde der FB verschickt
        		fazit,
        		events.event_type,
        		events.event_body -> 'properties' ->> 'reason' as grund,
        		channel_recommendation
        	FROM
        		"service-cs-selection".services
        		LEFT JOIN "service-cs-selection".contracts ON services.contract_id = contracts.id
        		LEFT JOIN "service-cs-selection".payers ON contracts.payer_id = payers.id
        		LEFT JOIN "service-cs-selection".vouchers ON services.voucher_id = vouchers.id
        		left join "case-cs-workflow-events".events ON services.case_id = events.case_id
        		LEFT JOIN "communication-cs-surveys".survey_links ON survey_links.case_id = services.case_id
        		LEFT JOIN "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id	
        		LEFT JOIN "patient-cs-communication-facts".communication_facts ON services.case_id = communication_facts.case_id
        		LEFT JOIN (select case_id, fazit
        					from (select case_id, event_time as fazit, row_number() over (partition by case_id order by event_time desc) as rn
        							from "case-cs-workflow-events".events 
        							where event_type like 'DID_SEND_FINAL%') no_dublicates
        					where rn = 1
        							) fazit ON services.case_id = fazit.case_id
        	WHERE
        		services.deleted_at is null
        		and voucher_code like '%VRTC%'
        		and events.event_type like '%DID_SEND_FINAL%'
        		and services.service_id like '%:PMS%'
        		and (surveys.title like '%Nachbefragung%' or channel_recommendation not like 'email')
        		and services.case_id not in (select survey_links.case_id 
        								from "communication-cs-surveys".survey_links
        			 					left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id					
        								where surveys.title like '%Nachbefragung%' and response_status is not null) --SS/PREM noch nicht beantwortet
        		and services.case_id not in (select case_id from "case-cs-workflow-events".events where event_type like '%DID_RECEIVE%') --betrachtet alle completion reasons, auch mehrfach nicht erreicht!! Ändern?
        		and services.case_id not in (select events.case_id from "case-cs-workflow-events".events where event_type like '%DID_CLOSE%' and (event_body -> 'properties' ->> 'reason' like '%Keine weitere Befragung%' or event_body -> 'properties' ->> 'reason' like '%Befragung unangebracht%' or event_body -> 'properties' ->> 'reason' like '%Kontaktadresse%'))
        		--im folgenden: keine Fälle in Anrufaktion, bei denen schon PROM rausgegangen ist
        		and services.case_id not in (select survey_links.case_id 
        									from "communication-cs-surveys".survey_links
        			 						left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id								
        									where surveys.title like '%PROM%')
        		and services.case_id not in (select case_id from staging.stg_communication_cs_conversations__conversations where product like 'PMS%' and topics = '["feedback_follow_up"]' and case_id is not null)
        		and services.case_id not in (select case_id
        									from md_campaigns.cam_select cs
        										left join md_campaigns.cam_batch cb on cs.batch_id = cb.batch_id
        									where cam_id not in (1, 2, 3, 8, -1))
        	)
        	select
        		(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
        		(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id, ----------Überprüfen, ob die Lieferung mit MSS verknüpft werden kann
        		case_id, 
        		name as client_name,
        		voucher_code as first_voucher_code,
        		null::int as patient_id,
        		0 as control_flag, 
        		1 as count_cases_patient, 
        		null::numeric as reached_patient_flag,
        		null::date as reached_date, 
        		null::text as refusal_reason,
        		null::numeric as opt_out, 
        		null::text as new_case_id,
        		case when case_fb like 'SS' then 'call_patient' when case_fb like 'PREM' then 'sent' end as app_state,
        		null::text as korb
        	from
        		foo
        	where
        		fb_age > 16-- Versand vor mindestens 2 Wochen
        		and fazit between (current_date - interval '5 month') and (current_date - interval '1 month')
        	order by fb_age desc
        ) PMS
""",
    dataocean_connection,
)


# ==========================================================================================
# ==========================================================================================


# nur wenn all_cases Ergebnisse enthält, dann sollen Trello-Karten erstellt werden
if not all_cases.empty:
    # Erstellt für jede case_id eine neue Karte im Board 'Telefonische Nachbefragung'
    for case_id, payer_name, korb in zip(
        all_cases["case_id"], all_cases["client_name"], all_cases["korb"]
    ):
        if korb == "waiting_q3":
            label_id = [
                "625e9b8626925b41dc0dd419",
                "647d8e9c690a56479841600a",
            ]  # id für Label 'tel. Nachbefragung', 'Q3 Fragebogen'
        else:
            label_id = [
                "625e9b8626925b41dc0dd419",
                "655ef650cbc61e435027751f",
            ]  # id für Label 'tel. Nachbefragung', 'PMS'

        card_data = {
            "key": api_key,
            "token": token,
            "idList": "6593bf668ced54ba6a049346",  # Trello-Liste 'VIP Rot'
            "idBoard": "625825d7e1c26e06248efc0b",  # Trello-Board 'Telefonische Nachbefragung'
            "name": case_id,
            "idLabels": label_id,
            "desc": payer_name,
        }
        # Erstelle eine neue Karte auf dem Trello-Board
        response = requests.post(f"https://api.trello.com/1/cards", params=card_data)

    # Überprüfe den Status der Anfrage
    if response.status_code == 200:
        print("Telefonie Board: Neue Karten erfolgreich erstellt!")
    else:
        print("Telefonie Board: Fehler beim Erstellen der Karten:", response.text)
else:
    print("all_cases ist leer. Es werden keine Trello-Karten erstellt.")


# ==========================================================================================
# ==========================================================================================


# nur wenn all_cases Ergebnisse enthält, soll die cam_select einen Eintrag erhalten
if not all_cases.empty:
    # Erstellt den SQL-INSERT-Befehl
    insert_query = "INSERT INTO md_campaigns.cam_select VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"  # Pro Spalte (16) ist jeweils ein Platzhalter erforderlich %s

    # Führt den INSERT-Befehl für jede Zeile im DataFrame aus
    for index, row in all_cases.iterrows():
        data_tuple = tuple(row)
        dataocean_cursor.execute(insert_query, data_tuple)

    dataocean_connection.commit()

else:
    print("all_cases ist leer. Neue select Einträge finden nicht statt.")


# ==========================================================================================
# ==========================================================================================


# nur wenn all_cases Ergebnisse enthält, soll die cam_batch einen Eintrag erhalten
if not all_cases.empty:
    # dataocean cam_batch entry
    cam_batch = pd.read_sql(
        """
        select
        	14 as cam_id,---VIP-Rot
        	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
        	'bd_nachtelefonie' as batch_acceptor,
        	current_date as batch_selection_date,
        	current_date as batch_transfer_date,
        	current_date as batch_start,
        	current_date + 30 as batch_stop
        """,
        dataocean_connection,
    )

    # Erstellt den SQL-INSERT-Befehl
    insert_query = "INSERT INTO md_campaigns.cam_batch VALUES (%s, %s, %s, %s, %s, %s, %s)"  # Pro Spalte (7) ist jeweils ein Platzhalter erforderlich %s

    # Führt den INSERT-Befehl für jede Zeile im DataFrame aus
    for index, row in cam_batch.iterrows():
        data_tuple = tuple(row)
        dataocean_cursor.execute(insert_query, data_tuple)

    dataocean_connection.commit()

else:
    print("all_cases ist leer. Neuer batch Eintrag findet nicht statt.")


# ==========================================================================================
# ==========================================================================================


# Cursor und Verbindung schließen
dataocean_connection.close()
dataocean_connection.close()
