#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 11:55:30 2024

@author: david.wetzels
"""


## IMPORT PACKAGES
import requests
import os  # create folder for person
import psycopg2 #connection to dataocean package
import pandas as pd #dataframe package
from datetime import datetime #um das aktuelle Datum zu kennen


# ==========================================================================================
# ==========================================================================================


## USER FILTER
filter_user = os.environ.get("USER")

#trello_credentials
if filter_user == "david.wetzels":
    filter_user_credentials = "/Users/david.wetzels/Documents/Credentials/trello_credentials.txt"
elif filter_user == "oumaima.el-bellam":
    filter_user_credentials = "/Users/oumaima.el-bellam/Documents/Credentials/trello_credentials.txt"

# Datei trello_credentials einlesen
with open(filter_user_credentials, 'r') as file:
    lines = file.readlines()
    api_key = lines[0].strip()
    token = lines[1].strip()
    
    
#dataocean_credentials
if filter_user == "david.wetzels":
    dataocean_credentials = "/Users/david.wetzels/Documents/Credentials/dataozean.txt"
elif filter_user == "oumaima.el-bellam":
    dataocean_credentials = "/Users/oumaima.el-bellam/Documents/Credentials/dataocean.txt"

#connection to dataocean
dataocean_connection = psycopg2.connect(open(dataocean_credentials).read())
dataocean_cursor = dataocean_connection.cursor()


# ==========================================================================================
# ==========================================================================================


# dataocean cases
all_cases = pd.read_sql("""
                    With borg_fälle as 
    (
    	SELECT
    		distinct on (i.case_id) i.case_id,
    		dcbr.least_result_date as results_sent_at,
    		ia.appointment_type,
    		ia.state as app_state,
    		i.state as case_state,
    		ice.q3_result,
    		status_survey_sent_at,
    		status_survey_reminder_sent_at,
    		v.client_name,
    		v.value as first_voucher_code
    	from 
    		staging.stg_borg__inquiries i
    		LEFT JOIN staging.stg_borg__inquiry_appointments ia on ia.inquiry_id = i.id
    		LEFT JOIN staging.stg_borg__inquiry_physician_contact_entries ice ON ice.inquiry_id = i.id
    		left join staging.stg_borg__inquiry_patient_data ipd on ipd.inquiry_id = i.id
    		left join staging.stg_borg__vouchers v on v.id = i.voucher_ids[0]::int
    		left join analytics.dim_clean_borg_results dcbr on i.service_id_key_systems = dcbr.service_id_key_systems
    	WHERE
    		i.state like 'waiting_q3'
    		and i.id not IN (SELECT inquiry_id FROM borg.inquiry_physician_contact_entries WHERE q3_result is not NULL)
    		and ia.appointment_type = '3'
    		and ia.inquiry_id not in (select inquiry_id from borg.inquiry_appointments where appointment_type = 3 and state in ('ready_to_send_pdf', 'send_manually', 'sent', 'deleted'))
    		and ia.state not in ('ready_to_send_pdf', 'send_manually', 'completed', 'waiting', 'sent', 'deleted')
    		and ipd.preferred_communication_channel like 'email'
    		and i.case_id not in (select case_id 
    								from staging.stg_borg__inquiries
    								LEFT JOIN staging.stg_borg__inquiry_appointments on stg_borg__inquiries.id = stg_borg__inquiry_appointments.inquiry_id
    							where appointment_type in ('4', '5', '6'))
    		and i.case_id not in (select case_id as masked_id
    								from md_campaigns.cam_select cs
    									left join md_campaigns.cam_batch cb on cs.batch_id = cb.batch_id
    								where cam_id not in (1, 2, 3, -1))
    		and status_survey_reminder_sent_at is null
    		and v.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff')
    		and (v.value not like '%-MSSNACHRSO' 
    			and v.value not like '%-BDTC' 
    			and v.value not like 'NUBU-7777' 
    			and v.value not like '%AXAP-%' 
    			and v.value not like 'ARAG-CM22')
    	order by 
    		i.case_id,
    		dcbr.least_result_date
    )
    select	
    	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
    	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
    	case_id,
    	client_name,
    	first_voucher_code,
    	null::int as patient_id,
    	0 as control_flag, 
    	1 as count_cases_patient, 
    	null::numeric as reached_patient_flag,
    	null::date as reached_date, 
    	null::text as refusal_reason,
    	null::numeric as opt_out, 
    	null::text as new_case_id,
    	app_state
    from 
    	borg_fälle
    where 
    	date_part('week', current_date) = date_part('week', (results_sent_at + interval '5 weeks'))
    	and date_part('year', (results_sent_at + interval '5 weeks')) = date_part('year', current_date)
""",  dataocean_connection)


# ==========================================================================================
# ==========================================================================================


# Erstelle den Dateinamen mit dem aktuellen Datum
date_today = datetime.now().strftime("%Y-%m-%d")

# Definiere den Dateipfad für das Dokumente-Verzeichnis
documents_path = os.path.expanduser("~/Documents")
file_path = os.path.join(documents_path, f"Send_second_status_survey_{date_today}.csv")

# Speichere den DataFrame als CSV-Datei im Dokumente-Verzeichnis
all_cases.to_csv(file_path, index=False)


# ==========================================================================================
# ==========================================================================================


#nur wenn all_cases Ergebnisse enthält, soll die cam_select einen Eintrag erhalten 
if not all_cases.empty:
    
    # Erstellt den SQL-INSERT-Befehl
    insert_query = "INSERT INTO md_campaigns.cam_select VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" # Pro Spalte (14) ist jeweils ein Platzhalter erforderlich %s
    
    # Führt den INSERT-Befehl für jede Zeile im DataFrame aus
    for index, row in all_cases.iterrows():
        data_tuple = tuple(row)
        dataocean_cursor.execute(insert_query, data_tuple)
    
    dataocean_connection.commit()
    
else:
    print("all_cases ist leer. Neue select Einträge finden nicht statt.")


# ==========================================================================================
# ==========================================================================================


#nur wenn all_cases Ergebnisse enthält, soll die cam_batch einen Eintrag erhalten 
if not all_cases.empty:
        
    # dataocean cam_batch entry
    cam_batch = pd.read_sql("""
        select
        	8 as cam_id, ---second status survey
        	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
        	'bd_second_status_survey' as batch_acceptor,
        	current_date as batch_selection_date,
        	current_date as batch_transfer_date,
        	current_date as batch_start,
        	current_date + 30 as batch_stop
    """,  dataocean_connection)
    
    
    # Erstellt den SQL-INSERT-Befehl
    insert_query = "INSERT INTO md_campaigns.cam_batch VALUES (%s, %s, %s, %s, %s, %s, %s)" # Pro Spalte (7) ist jeweils ein Platzhalter erforderlich %s
    
    # Führt den INSERT-Befehl für jede Zeile im DataFrame aus
    for index, row in cam_batch.iterrows():
        data_tuple = tuple(row)
        dataocean_cursor.execute(insert_query, data_tuple)
    
    dataocean_connection.commit()
    
else:
    print("all_cases ist leer. Neuer batch Eintrag findet nicht statt.")

# Cursor und Verbindung schließen
dataocean_connection.close()
dataocean_connection.close()


# ==========================================================================================
# ==========================================================================================


print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!CSV-Datei in Jira in einem neuen Ticket bei Patient Service im Backlog erstellen und Katie bzw. PD Bescheid sagen.')