#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 14:48:01 2024

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
with cube_services as (
	select * 
	from 
		analytics.cube_services 
	where 
		payer_group = 'kv' 
		and result_date > current_date - interval '6 month'
		and product = 'MSS'
		--New focus on cases before surgery, because of a big backlog.
		--decission made with Lea S, Saranga T. & Stefanie L. K. (27.11.2025)
		and inquiry_type in ('second_opinion_before_surgery', 'surgery')
),
pii_cube_services as (
	select * from pii_analytics.pii_cube_services
),
report_assura as (
	select * from analytics.report_assura
),
workflow_events as (
	select * from analytics.fct_workflow_events where case_id is not null
),
existing_follow_up_response as (
select 
	survey_links.case_id,
	surveys.title,
	survey_links.response_status,
	survey_links.data_fetched_at 
from 
	staging.stg_communication_cs_surveys__survey_links survey_links
	left join staging.stg_communication_cs_surveys__surveys surveys ON surveys.id = survey_links.survey_id					
where 
	surveys.title ilike '%Nachbefragung%' 
	and survey_links.response_status is not null
	and survey_links.case_id is not null
),
ss_prem_send as (
select
	case_id,
	max(case when upper(event_type) = 'DID_SEND_STATUS_SURVEY' then workflow_event_date end) as ss_send_date,
	max(case when upper(event_type) = 'DID_SEND_PREM_SURVEY' then workflow_event_date end) as prem_send_date
from 
	workflow_events  
where 
	upper(event_type) in ('DID_SEND_STATUS_SURVEY', 'DID_SEND_PREM_SURVEY')
group by 
	case_id
),
case_closed_or_response_received as (
select 
	case_id,
	event_type,
	workflow_event_date
from 
	workflow_events  
where 
	(event_type like '%DID_CLOSE%' 
	or event_type like '%DID_RECEIVE%')
), --betrachtet alle completion reasons, auch mehrfach nicht erreicht!! Ändern?
campaigns as (
select
	row_number () over (partition by case_id),
	cam_select.case_id,
	cam_select.batch_id,
	cam_batch.batch_selection_date,
	cam_batch.cam_id
from 
	md_campaigns.cam_select
	left join md_campaigns.cam_batch on cam_select.batch_id = cam_batch.batch_id
where 
	cam_batch.cam_id = 6
	and cam_select.case_id is not null
),
swiss_cases as (
select
	cube_services.service_id_key_systems,
	cube_services.case_id,
	cube_services.voucher_code,
	cube_services.inquiry_type,
	cube_services.payer_group,
	cube_services.payer_name,
	report_assura.qualimed,
	cube_services.admit_date,
	cube_services.result_date,
	pii_cube_services.channel_reach_email,
	pii_cube_services.channel_reach_phone,
	pii_cube_services.channel_reach_postal,
	pii_cube_services.preferred_channel,
	pii_cube_services.preferred_language,
	greatest(ss_prem_send.ss_send_date, ss_prem_send.prem_send_date) as latest_ss_prem_send_date,
	current_date - greatest(ss_prem_send.ss_send_date, ss_prem_send.prem_send_date) as latest_ss_prem_send_age
from 
	cube_services
	left join report_assura on cube_services.service_id_key_systems = report_assura.service_id_key_systems
	left join pii_cube_services on cube_services.service_id_key_systems = pii_cube_services.service_id_key_systems
	left join ss_prem_send on cube_services.case_id = ss_prem_send.case_id
where
	cube_services.case_id not in (select case_id from campaigns)
	and cube_services.case_id not in (select case_id from existing_follow_up_response)
	and cube_services.case_id not in (select case_id from case_closed_or_response_received)
	and 	
			----case where mails are allowed
		(
			(current_date - greatest(ss_prem_send.ss_send_date, ss_prem_send.prem_send_date) >= 17
				and (report_assura.qualimed is null or report_assura.qualimed = 'nein')
			)
		or 
			----post cases
			(
			pii_cube_services.preferred_channel = 'postal'
				and cube_services.result_date < current_date - interval '2 weeks'
			)
		)
),
final as (
select
	distinct on (case_id)
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	case_id,
	payer_name as client_name,
	voucher_code as first_voucher_code,
	null::int as patient_id,
	0 as control_flag, 
	1 as count_cases_patient, 
	null::numeric as reached_patient_flag,
	null::date as reached_date, 
	null::text as refusal_reason,
	null::numeric as opt_out, 
	null::text as new_case_id,
	null::text as app_state,
	null::text as korb,
	preferred_language,
	inquiry_type
from
	swiss_cases
)
select * from final
""",
    dataocean_connection,
)


# ==========================================================================================
# ==========================================================================================


# Erstellt für jede case_id eine neue Karte im Board 'Telefonische Nachbefragung'
for case_id, payer_name, preferred_language, inquiry_type in zip(
    all_cases["case_id"],
    all_cases["client_name"],
    all_cases["preferred_language"],
    all_cases["inquiry_type"],
):
    if preferred_language == "fr":
        label_id = [
            "625e9b8626925b41dc0dd419",
            "647d8e9c690a56479841600a",
            "62fc9e29b8600b502c39a109",
        ]  # id für Label 'tel. Nachbefragung', 'Q3 Fragebogen', 'FR'
    elif preferred_language == "en":
        label_id = [
            "625e9b8626925b41dc0dd419",
            "647d8e9c690a56479841600a",
            "62f4bdc0cce13800c0f7c45b",
        ]  # id für Label 'tel. Nachbefragung', 'Q3 Fragebogen', 'EN'
    elif preferred_language == "it":
        label_id = [
            "625e9b8626925b41dc0dd419",
            "647d8e9c690a56479841600a",
            "63bffd8970f4f5016ebeaefa",
        ]  # id für Label 'tel. Nachbefragung', 'Q3 Fragebogen', 'It'
    else:
        label_id = [
            "625e9b8626925b41dc0dd419",
            "647d8e9c690a56479841600a",
        ]  # id für Label 'tel. Nachbefragung', 'Q3 Fragebogen'

    # Zusätzliche Bedingungen basierend auf inquiry_type
    if inquiry_type == "second_opinion_before_surgery":
        label_id.append("639343d95fa70e059e1a6698")
    elif inquiry_type == "surgery":
        label_id.append("67fc9b645ccf33a14c6ee92c")

    card_data = {
        "key": api_key,
        "token": token,
        "idList": "62582624897e2669270d1073",  # Trello-Liste 'Tel. Nachbefragung Schweiz'
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

all_cases_insert = all_cases.drop(columns=["inquiry_type"])

# nur wenn all_cases Ergebnisse enthält, soll die cam_select einen Einträge erhalten
if not all_cases_insert.empty:
    # Erstellt den SQL-INSERT-Befehl
    insert_query = "INSERT INTO md_campaigns.cam_select VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"  # Pro Spalte (16) ist jeweils ein Platzhalter erforderlich %s

    # Führt den INSERT-Befehl für jede Zeile im DataFrame aus
    for index, row in all_cases_insert.iterrows():
        data_tuple = tuple(row)
        dataocean_cursor.execute(insert_query, data_tuple)

    dataocean_connection.commit()

else:
    print("all_cases_insert ist leer. Neue select Einträge finden nicht statt.")


# ==========================================================================================
# ==========================================================================================


# nur wenn all_cases_insert Ergebnisse enthält, soll die cam_batch einen EIntrag erhalten
if not all_cases_insert.empty:
    # dataocean cam_batch entry
    cam_batch = pd.read_sql(
        """
        select
        	6 as cam_id,-------Regelprozess Schweiz
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
    print("all_cases_insert ist leer. Neuer batch Eintrag findet nicht statt.")


# ==========================================================================================
# ==========================================================================================


# Cursor und Verbindung schließen
dataocean_connection.close()
dataocean_connection.close()
