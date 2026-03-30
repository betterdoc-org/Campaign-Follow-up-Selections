#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 12:50:54 2024

@author: david.wetzels

Achtung!!!!!:
-Das Skript darf nur einmal pro Woche ausgeführt werden
-Es erstellt neue Karten im Board 'Telefonische Nachbefragung'
-Es inserted neue Fälle in die md_campaign Tabellen (dataocean)
!!!!!!!!
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
elif filter_user == 'alexander.maletz':
    filter_user_credentials = (
        "/Users/alexander.maletz/Documents/Credentials/trello.txt"
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
elif filter_user == 'alexander.maletz':
    dataocean_credentials = (
        "/Users/alexander.maletz/Documents/Credentials/dataocean.txt"
    )

# connection to dataocean
dataocean_connection = psycopg2.connect(open(dataocean_credentials).read())
dataocean_cursor = dataocean_connection.cursor()


# ==========================================================================================
# ==========================================================================================


# dataocean cases
all_cases = pd.read_sql(
    """
  with borg_data as (
	SELECT
		distinct on (stg_borg__inquiries.case_id) stg_borg__inquiries.case_id,
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
		left join (select inquiry_id, q3_result from staging.stg_borg__inquiry_physician_contact_entries where q3_result is not null) as stg_borg__inquiry_physician_contact_entries
			on stg_borg__inquiry_physician_contact_entries.inquiry_id = stg_borg__inquiries.id
		left join staging.stg_borg__vouchers on stg_borg__vouchers.id = stg_borg__inquiries.voucher_ids[0]::int
	WHERE
		stg_borg__inquiries.state like 'waiting_q3'
		/*
		!!! New decision to include 'surgery' in the selection of 'Regelprozess'
		!!! 02.02.2026: Jira DA-3454
		*/
		and stg_borg__inquiries.treatment_type in ('second_opinion_before_surgery', 'surgery')
		and stg_borg__inquiry_physician_contact_entries.q3_result is null
		and stg_borg__inquiry_appointments.appointment_type = '3'
		and stg_borg__inquiry_appointments.state in('sent', 'call_patient')
		/*and stg_borg__inquiry_appointments.inquiry_id not in (select inquiry_id 
																from staging.stg_borg__inquiry_appointments
																where (appointment_type = '3' 
																		and state like 'completed'))*/ --Muss ausgeschlossen werden, damit erreichte Fälle der Nachbetreuung nachtelefoniert werden
		and stg_borg__vouchers.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff')
		and (stg_borg__vouchers.value not like '%-MSSNACHRSO' 
			and stg_borg__vouchers.value not like '%-BDTC' 
			and stg_borg__vouchers.value not like 'NUBU-7777' 
			and stg_borg__vouchers.value not like '%AXAP-%' 
			and stg_borg__vouchers.value not like 'ARAG-CM22')
	ORDER BY
		stg_borg__inquiries.case_id, app_state, voucher_client_name
		),
	final_regelprozess as (
	select
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
		korb,
		'de' as preferred_language,
		null as post
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
),
   post as (
	select
		distinct on (i.case_id) i.case_id,
		ia.scheduled_date,
		i.voucher_ids[0]::int as voucher_id,
		i.treatment_type,
		i.state as korb,
		ia.appointment_type, 
		ia.state as app_state,
		v.client_name,
		v.value
	from
		staging.stg_borg__inquiries i
	    	left join staging.stg_borg__inquiry_patient_data ipd on ipd.inquiry_id = i.id
	    	left join staging.stg_borg__inquiry_appointments ia on ipd.inquiry_id = ia.inquiry_id
	    	left join staging.stg_borg__inquiry_insurance_data iid on ipd.inquiry_id = iid.inquiry_id
	    	left join staging.stg_borg__vouchers v on v.id = i.voucher_ids[0]::int
	where
		ipd.preferred_communication_channel like 'post'
		and i.state in ('waiting_q3', 'waiting_q4', 'waiting_q5', 'waiting_q6' )
		and ia.appointment_type not in ('1', '2')
		and ia.state like 'ready_to_send_pdf'
		and iid.options::text = '[]'
		and date_part('week', ia.scheduled_date) = date_part('week', (current_date - interval '1 week')) -----letzte Woche
		and date_part('year', ia.scheduled_date) = date_part('year', (current_date - interval '1 week'))
		and v.value is not null
		and (v.client_name like 'DAK' or treatment_type in ('second_opinion_before_surgery', 'surgery'))
		and v.client_name not like 'AXA Haftpflichtversicherung'
	order by
		i.case_id,
		scheduled_date ASC
	),
final_post as (
	select 
		case_id,
		client_name as client_name,
		value as first_voucher_code,
		null::int as patient_id,
		0 as control_flag, 
		1 as count_cases_patient, 
		null::numeric as reached_patient_flag,
		null::date as reached_date, 
		null::text as refusal_reason,
		null::numeric as opt_out, 
		null::text as new_case_id,
		app_state,
		korb,
		'de' as preferred_language,
		'post' as post
	from post
),
union_regelprozess_post as (
select * from final_regelprozess
union all 
select * from final_post
),
   final as (
   select
       distinct on (case_id)
       (select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
       (select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
       union_regelprozess_post.case_id,
       client_name,
       first_voucher_code,
		patient_id,
		control_flag, 
		count_cases_patient, 
		reached_patient_flag,
		reached_date, 
		refusal_reason,
		opt_out, 
		new_case_id,
		app_state,
		korb,
		union_regelprozess_post.preferred_language,
		post,
		CASE WHEN cs.admission_channel IN ('online_marketing', 'partner_funnel') THEN 1 ELSE 0 END AS om_flag,
		cs.inquiry_type
   from 
   	union_regelprozess_post
   	left join (select distinct on (case_id) * from analytics.cube_services where product = 'MSS') as cs on union_regelprozess_post.case_id = cs.case_id
   )
select * from final
""",
    dataocean_connection,
)


# ==========================================================================================
# ==========================================================================================


# Erstellt für jede case_id eine neue Karte im Board 'Telefonische Nachbefragung'
for case_id, payer_name, korb, om_flag, post, inquiry_type in zip(
    all_cases["case_id"],
    all_cases["client_name"],
    all_cases["korb"],
    all_cases["om_flag"],
    all_cases["post"],
    all_cases["inquiry_type"],
):
    label_id = ["625e9b8626925b41dc0dd419"]  # id Tel. Nachb.

    if korb == "waiting_q3":
        label_id += ["647d8e9c690a56479841600a"]  # id für Label 'Q3 Fragebogen'
    else:
        label_id += ["647d8e9d690a564798416023"]  # id für Label 'Q4-6 Fragebogen'

    if om_flag == 1:
        label_id += ["638e046ca2e9f8034af4ce96"]  # id für Label 'Q3 Fragebogen'
    else:
        pass

    if post == "post":
        label_id += ["625e9be909332e248d271305"]  # id für Label 'Post'
    else:
        pass

    if inquiry_type == "second_opinion_before_surgery":
        label_id += ["639343d95fa70e059e1a6698"]
    elif inquiry_type == "surgery":
        label_id += ["67fc9b645ccf33a14c6ee92c"]
    else:
        pass

    # Kombiniere die IDs zu einer Zeichenkette
    label_id_str = ",".join(label_id)

    card_data = {
        "key": api_key,
        "token": token,
        "idList": "67236695bd1cf349e55eb5d6",  # Trello-Liste 'Pipeline Regelprozess'
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


# ==========================================================================================
# ==========================================================================================


# nur wenn all_cases Ergebnisse enthält, soll die cam_select einen Eintrag erhalten
if not all_cases.empty:
    # Entferne die beiden letzten Spalten
    all_cases_trimmed = all_cases.iloc[
        :, :-3
    ]  # Wähle alle Spalten außer die beiden letzten

    # Überprüfe die Anzahl der Spalten im neuen DataFrame
    num_columns = len(all_cases_trimmed.columns)

    # Erstellt den SQL-INSERT-Befehl dynamisch basierend auf der Anzahl der Spalten
    insert_query = f"INSERT INTO md_campaigns.cam_select VALUES ({', '.join(['%s'] * num_columns)})"

    # Führt den INSERT-Befehl für jede Zeile im DataFrame aus
    for index, row in all_cases_trimmed.iterrows():
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
        	4 as cam_id,-------Regelprozess Deutschland
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


# dataocean post cases to complete in borg
post_cases = pd.read_sql(
    """
 with foo as (
    select
    	distinct on (i.case_id) i.case_id,
    	ia.scheduled_date,
    	i.treatment_type,
    	i.state as korb,
    	ia.appointment_type, 
    	ia.state as app_state,
    	v.client_name
    from
    	staging.stg_borg__inquiries i
    	left join staging.stg_borg__inquiry_patient_data ipd on ipd.inquiry_id = i.id
    	left join staging.stg_borg__inquiry_appointments ia on ipd.inquiry_id = ia.inquiry_id
    	left join staging.stg_borg__inquiry_insurance_data iid on ipd.inquiry_id = iid.inquiry_id
    	left join staging.stg_borg__vouchers v on v.id = i.voucher_ids[0]::int
    where
    	preferred_communication_channel like 'post'
    	and i.state in ('waiting_q3', 'waiting_q4', 'waiting_q5', 'waiting_q6' )
    	and ia.appointment_type not in ('1', '2')
    	and ia.state like 'ready_to_send_pdf'
    	and options::text = '[]'
    	--and ia.scheduled_date between (current_date - 7) and (current_date - 1)-----------------Datum anpassen
    	and date_part('week', ia.scheduled_date) = date_part('week', (current_date - interval '1 week')) -----letzte Woche
    	and date_part('year', ia.scheduled_date) = date_part('year', (current_date - interval '1 week'))
    	and v.client_name is not null
		/*
		!!! New decision to include 'surgery' in the selection of 'Regelprozess'
		!!! 02.02.2026: Jira DA-3454
		*/
    	and (v.client_name not like '%DAK%' 
    		and treatment_type not in ('second_opinion_before_surgery', 'surgery'))
    	and v.client_name not like 'AXA Haftpflichtversicherung'
    order by 
    	i.case_id,
    	i.state ASC
    )
    select 
    	case_id,
    	client_name,
    	app_state,
    	treatment_type
    from 
    	foo
""",
    dataocean_connection,
)

# Cursor und Verbindung schließen
dataocean_connection.close()
dataocean_connection.close()


# Erstellt für jede case_id eine neue Karte im Board 'Analytics: Eingegangene Frabö'
for case_id, payer_name in zip(post_cases["case_id"], post_cases["client_name"]):
    card_data = {
        "key": api_key,
        "token": token,
        "idList": "660fe24f21076f41b3a321dc",  # Trello-Liste 'Fälle in Borg completen (jeden Montag)'
        "idBoard": "5a66fbf7505a5802ed036b4f",  # Trello-Board 'Analytics: Eingegangene Frabö'
        "name": case_id,
        "idLabels": "660fe397371356760e098f48",  # Label 'Fall in Borg completen'
        "desc": payer_name,
    }
    # Erstelle eine neue Karte auf dem Trello-Board
    response = requests.post(f"https://api.trello.com/1/cards", params=card_data)

# Überprüfe den Status der Anfrage
if response.status_code == 200:
    print("Analytics-Board: Neue Karten erfolgreich erstellt!")
else:
    print("Analytics-Board: Fehler beim Erstellen der Karten:", response.text)
