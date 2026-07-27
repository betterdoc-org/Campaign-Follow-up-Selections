#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 07:11:27 2024

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
with cube_services as (
	select * from analytics.cube_services 
),
patient_id as (
	select * from md_patient_id.patient_id
),
m_client_hist as (
	select * from staging.stg_gsheets__m_client_hist where valid_to = '2999-12-31'
),
cam_select as (
	select * from md_campaigns.cam_select
),
cam_batch as (
	select * from md_campaigns.cam_batch
),
patient_facts as (
	select * 
	from 
		"patient-cs-facts"._patients
		left join "patient-cs-facts"._cases on _patients.id = _cases.patient_id
		left join "patient-cs-communication-facts"._communication_facts on _cases.external_id = _communication_facts.case_id
),
borg_beihilfe as (
	select
		_inquiries.masked_id as case_id,
		beihilfe.beihilfe
	from 
		borg._inquiries
		left join (select 
						max(inquiry_id) as inquiry_id, 
						supplementary as beihilfe 
					from 
						borg."_inquiry_insurance_data" 
					where 
						main is true 
					group by 
						inquiry_id, 
						supplementary
				) as beihilfe on beihilfe.inquiry_id = _inquiries.id
	where 
		beihilfe.beihilfe is true
),
all_patients as (
	select
		distinct on (cube_services.dna_patient_id) cube_services.dna_patient_id,
		cube_services.service_id_key_systems,
		cube_services.case_id,
		cube_services.product,
		cube_services.result_date,
		cube_services.admission_channel,
		cube_services.payer_name,
		m_client_hist.coop_status,
		cube_services.payer_group,
		cube_services.insurance_main_name,
		cube_services.voucher_code,
		cube_services.voucher_code_second,
		cube_services.completion_reason,
		cube_services.completion_details,
		cube_services.gender,
		case when cube_services.gender = 'female' or cube_services.gender = 'F' then 'Frau'
		 when cube_services.gender = 'male'or cube_services.gender = 'M' then 'Herr'
		 else cube_services.gender end as anrede,
		patient_facts.first_name,
		patient_facts.last_name,
		patient_facts.email,
		cube_services.country_code,
		cube_services.bd_nps_value,
		cube_services.medic_nps_value,
		cube_services.visited_recommended_medic
	from
		cube_services
		--left join patient_id on cube_services.case_id = patient_id.case_id
		left join m_client_hist on cube_services.payer_name = m_client_hist.client_name
		left join patient_facts on cube_services.case_id = patient_facts.case_id
		left join borg_beihilfe on cube_services.case_id = borg_beihilfe.case_id
	where 
		--cube_services.result_date between (current_date - interval '48 month') 
			--and (current_date - interval '24 month')
		cube_services.result_date between '2018-01-01' and '2020-12-31'
		and cube_services.medic_nps_value not in (0,2,3,4,5,6)
		and cube_services.bd_nps_value not in (0,2,3,4,5,6)
		and cube_services.bd_nps_value is not null
		and cube_services.voucher_code not in ('KULANZ-MSSNICHTKOOPP', 'GADS-LF22') ---kein Kulanz
		and cube_services.voucher_code not like '%VRTC%' ---kein VIP
		and cube_services.payer_group in ('gkv', 'pkv')
		and cube_services.product != 'RSO'
		and cube_services.admission_channel not in ('proactive_steering', 'betterdoc_staff')
		and borg_beihilfe.beihilfe is not true ---Die Angabe Beihilfe gibt es vermutlich nur für Borg
		and cube_services.country_code = 'de'
		and (cube_services.completion_reason is null or cube_services.completion_reason in ('Befragung vollständig durchgeführt', 'Patient mehrfach nicht erreicht', 'Spezialist nicht aufgesucht - Patient meldet sich bei Bedarf'))
		and cube_services.payer_name not like '%Allianz%'
		and cube_services.payer_name not like '%BIG%'
		and cube_services.payer_name not like '%Generali%'
		and cube_services.payer_name not like 'AOK Plus'
		and m_client_hist.coop_status is true
		and LENGTH(patient_facts.email) >= 4
		and patient_facts.last_name is not null
		--no vorstände/ansprechpartner etc.
		and cube_services.case_id not in (
							'12-1212-111',
							'13-2230-551',
							'85-1144-354',
							'91-2140-04',
							'APJ7-JKDD-3B0A')
		--widerspruch mailings
		and cube_services.case_id not in (
						'92-1142-005',
						'93-1420-255',
						'15-2105-015',
						'11-2043-111',
						'11-1321-455',
						'15-4234-22',
						'AS34-VH1C-480T',
						'ADQC-4QEW-JJ0D',
						'A137-QYNM-GZ0J',
						'ABYJ-XPYX-0E0G',
						'AV04-4PGQ-JM09',
						'11-2043-111',
						'15-1520-044',
						'15-2105-015',
						'14-1115-545',
						'14-3124-58',
						'16-2023-112',
						'11-5440-02',
						'A29A-1JV7-NZ0N',
						'15-4033-44',
						'11-5425-25',
						'11-5154-23',
						'15-2240-511',
						'14-1553-251',
						'A5T7-35QG-5105',
						'16-1344-051',
						'19-1221-102',
						'13-1435-235',
						'16-5423-12',
						'15-1254-541',
						'16-5423-12',
						'12-5311-52',
						'21-5435-14',
						'12-4324-53',
						'32-5505-51',
						'92-2053-058',
						'14-2300-341',
						'15-2332-101',
						'A21Q-HSK6-N914',
						'ASZZ-4X3N-Q603',
						'AQXT-1P35-5P11',
						'AJ7N-8WX7-K90D',
						'14-1511-114')
		/*and cube_services.case_id not in (
							select 
								cam_select.case_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
							where 
								cam_batch.cam_id in (2, 3)
							)*/
	order by 
		cube_services.dna_patient_id,
		cube_services.result_date DESC
),
final as (
	select
		case_id,
		dna_patient_id as patient_id,
		product,
		result_date,
		payer_name,
		payer_group,
		voucher_code,
		gender,
		anrede,
		first_name,
		last_name,
		email,
		country_code
	from
		all_patients
)
select * from final
""",  dataocean_connection)


# ==========================================================================================
# ==========================================================================================


# Erstelle den Dateinamen mit dem aktuellen Datum
date_today = datetime.now().strftime("%Y-%m-%d")

# Definiere den Dateipfad für das Dokumente-Verzeichnis
documents_path = os.path.expanduser("~/Documents")
file_path = os.path.join(documents_path, f"Bestandskundenmail_{date_today}.csv")

# Speichere den DataFrame als CSV-Datei im Dokumente-Verzeichnis
all_cases.to_csv(file_path, index=False)