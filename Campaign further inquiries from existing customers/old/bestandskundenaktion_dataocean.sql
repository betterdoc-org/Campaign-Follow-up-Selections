with unification as (
with cases as (
--BORG Fälle
(select
	'borg' as system,
	masked_id as case_id,
	concat(masked_id,':MSS') as service_id,
	'MSS' as product,
	results_sent_at as result_date,
	--to_char(results_sent_at,'YYYY-MM-DD')::text as result_date,
	v1.value as first_voucher_code,
	v2.value as second_voucher_code,
	v1.client_name as client_1,
	v2.client_name as client_2,
	insurance,
	beihilfe,
	i.state,
	i.description,
	case when lower(i.description) like '%offline%' then true else offline end as offline,
	preferred_communication_channel,
	gender,
	encrypted_title as title,
	encrypted_first_name as first_name,
	encrypted_last_name as last_name,
	encrypted_birth_date as birth_date,
	encrypted_street as street,
	encrypted_house_no as street_number,
	encrypted_zip as zip,
	encrypted_city as city,
	null as county,
	lower(ipd.country_code) as country,
	encrypted_email as email,
	encrypted_phone_number as primary_phone,
	encrypted_second_phone_number as secondary_phone,
	betterdoc_nps::text,
	betterdoc_nps_reason,
	--treatment_type,
	completion_reason_main,
	completion_reason_additional,
	completion_details,
	exclude_from_reporting::text as exclude,
	deleted_at 
from 
	borg."_inquiries" i 
	left join borg."_inquiry_patient_data" ipd on i.id = ipd.inquiry_id 
	left join borg."_vouchers" v1 on i.voucher_ids[1] = v1.id 
	left join borg."_vouchers" v2 on i.voucher_ids[2] = v2.id
	left join (select 
					inquiry_id , 
					insurance_id, name as insurance 
			   from 
			   		borg."_inquiry_insurance_data" id
					left join borg."_insurances" i on i.id = id.insurance_id 
			   where 
			   		main is true
			   group by 
			   		inquiry_id, insurance_id, name) as borg_insurance on borg_insurance.inquiry_id = i.id
	left join (select 
					max(inquiry_id) as inquiry_id, 
					supplementary as beihilfe  
					from borg."_inquiry_insurance_data" 
					where 
						main is true 
					group by 
						inquiry_id, 
						supplementary
				) as beihilfe on beihilfe.inquiry_id = i.id
where 
	i.id not in (select 
					distinct inquiry_id
				 from 
				 	borg."_inquiry_physician_contact_entries"
				 where
					 quality in (0,2,3,4,5,6)
					 and contact_type != 'Q1'
					 --and bd_responsible is true
					 --sand contact_type = 'Q3'
					 )
) 
union all 
(--PARC Fälle
select 
	'parc' as system,
	s.case_id,
	s.service_id,
	pr.name as product,
	result_date,
	voucher_code as first_voucher_code,
	null as second_voucher_code,
	pa.name as client_1,
	null as client_2,
	insurance,
	false as beihilfe,
	null as state,
	null as description,
	null as offline,
	null as preferred_communication_channel,
	gender,
	title,
	first_name,
	last_name,
	date_of_birth::text as birth_date,
	pp.street,
	pp.street_number,
	pp.zip,
	pp.city,
	pp.state as county,
	country,
	email,
	primary_phone,
	secondary_phone,
	betterdoc_nps::text,
	betterdoc_nps_reason,
	completion_reason_main,
	null as completion_reason_additional,
	null as completion_details,
	exclude::text,
	s.deleted_at 
from 
	(select * from "service-cs-selection"."_services" where deleted_at is null) as s 
	left join "service-cs-selection"."_contracts" sc on sc.id = s.contract_id 
	left join "service-cs-selection"."_payers" pa on pa.id = sc.payer_id 
	left join "service-cs-selection"."_products" pr on pr.id = sc.product_id 
	left join "patient-cs-facts"."_cases" pc on pc.external_id = s.case_id 
	left join "patient-cs-facts"."_patients" pp on pp.id = pc.patient_id
	left join "patient-cs-communication-facts"."_communication_facts" pcf on pcf.case_id = s.case_id 
	left join (with nps as (select row_number() over (partition by service_id order by case nps when '0' then 0 when '1' then 1 when '2' then 2 when '3' then 3 when '4' then 4 when '5' then 5 when '6' then 6 when '7' then 7 when '8' then 8 when '9' then 9 when '10' then 10 when 'not_specified' then 11 end) as n_row,
								   service_id, nps, free_text
							from "service-cs-selection"."_feedbacks" 
							where deleted_at is null) 
			   select service_id, nps as betterdoc_nps, free_text as betterdoc_nps_reason from nps where n_row = 1) as parc_nps on parc_nps.service_id = s.id 
	join (select case_id,
					  to_date(event_body -> 'properties' ->> 'date','DD-MM-YYYY') as result_date,
					  concat(case_id,':',service) as service_id
			   from "case-cs-workflow-events"."_events"
			   where deleted_at is null
					 and event_type like '%DID_SEND_FINAL%') as parc_result_date on parc_result_date.service_id = s.service_id 
	left join (select case_id,
					  event_body -> 'properties' ->> 'reason' as completion_reason_main,
					  concat(case_id,':',service) as service_id
			   from "case-cs-workflow-events"."_events"
			   where deleted_at is null
					 and event_type like '%DID_CLOSE%') as parc_close on parc_close.service_id = s.service_id 
	left join (select case_id,
	   				  case when markers ->> 'test_case' = 'true' or markers ->> 'deleted' is not null then 'true' else 'false' end as exclude
			   from "case-cs-marker"."_case_markers") as parc_exclude on parc_exclude.case_id = s.case_id 
	left join (select case_id, title as insurance
			   from "case-cs-healthinsurance-coverage"."_primary_insurance_selections" p
					 left join "case-cs-healthinsurance-coverage"."_insurance_companies" i on i.id = p.insurance_company_id) as parc_insurance on parc_insurance.case_id = s.case_id
where s.case_id not in (select distinct case_id
						from "medic-cs-visits"."_visits"
						where
							deleted_at is null
							and visit_timing = 1
							and visit_nps in ('0','1','2','3','4','5','6')
							--and visit_referrer = 0
							--and survey_type = 'prem'
							)
)
)
select
	system,
	case_id,
	service_id,
	product,
	result_date::date,
	now() - result_date,
	first_voucher_code,
	second_voucher_code,
	client_1,
	client_2,
	insurance,
	beihilfe,
	state,
	offline,
	preferred_communication_channel,
	gender,
	case when gender = 'female' or gender = 'F' then 'Frau'
		 when gender = 'male'or gender = 'M' then 'Herr'
		 else gender end as anrede,
	title,
	first_name,
	last_name,
	birth_date,
	street,
	street_number,
	zip,
	city,
	county,
	country,
	email,
	primary_phone,
	secondary_phone,
	betterdoc_nps,
	betterdoc_nps_reason,
	completion_reason_main,
	completion_reason_additional,
	completion_details,
	client_infos_1.client_group as client_1_group,
	client_infos_1.coop_status as client_1_coop_status,
	client_infos_2.client_group as client_2_group,
	client_infos_2.coop_status as client_2_coop_status,
	client_infos_3.client_group as client_3_group,
	client_infos_3.coop_status as client_3_coop_status
from
	cases
	left join (select client_id, t.client_name, client_group, coop_status
		       from  raw.gsheets__m_client_translation t
				     left join md_meta.m_client_infos i using(client_id)
			   group by client_id, t.client_name, client_group, coop_status) as client_infos_1 on client_infos_1.client_name = client_1
	left join (select client_id, t.client_name, client_group, coop_status
		       from  raw.gsheets__m_client_translation t
				     left join md_meta.m_client_infos i using(client_id)
			   group by client_id, t.client_name, client_group, coop_status) as client_infos_2 on client_infos_2.client_name = client_2
	left join (select client_id, t.client_name, client_group, coop_status
		       from  raw.gsheets__m_client_translation t
				     left join md_meta.m_client_infos i using(client_id)
			   group by client_id, t.client_name, client_group, coop_status) as client_infos_3 on client_infos_3.client_name = insurance
where 
	--only patients that received a result
	result_date between (current_date - interval '48 month') and (current_date - interval '24 month')
	--only patients living in Germany
	and country = 'de' --UNSICHER OB GLEICHER COUNTRY CODE in BEIDEN SYSTEMEN
	--no patients with anonymised address
	and (first_name != 'jf0PKPLg+05YG+B8/wKJQQ==
'		 or first_name is null) --UNSICHER WIE in PARC ANONYMISIERT WIRD
	--only promotors and passives (NPS)
	and (betterdoc_nps in ('10','9','8','7'))--,'8','7','-1') or betterdoc_nps is null) 
	and (exclude = 'false' or exclude is null) 
	--only happy, real patients
	and (completion_reason_main not in ('survey_inappropriate',
									   'survey_aborted_by_patient',
									   'patient_not_contactable',
									   'employee_request',
									   'internal_reason',
									   'other',
									   'Befragung unangebracht - Sonstige',
									   'Interner Grund - Kunde, keine Befragung',
									   'Interner Grund - Sonstige',
									   'Keine weitere Befragung gewünscht',
									   'Patient mehrfach nicht erreicht',
									   'Sonstige',
									   'Spezialist nicht aufgesucht - Entfernung nicht passend')
										or completion_reason_main is null) 
	and (completion_reason_additional not in ('patient_difficult',
											  'patient_critically_ill_or_dead',
											  'cost_for_physician',
											  'not_satisfied_with_recommendations_had_other_expectations_regarding_the_service',
											  'distance_does_not_match',
											  'problems_making_an_appointment',
											  'b2b_no_survey',
											  'test_inquiry',
											  'rerun',
											  'doc_inquiry',
											  'other')
											   or completion_reason_main is null) 
	and (completion_details is null or completion_details = '') 
	--only patients with voucher code
	and not (cases.system = 'borg' and first_voucher_code is null)
	--no insurance employees
	and ((first_voucher_code not like '%MA4L%' 
		--no kulanz/pilot
		and lower(first_voucher_code) not like '%pilot%'
		and lower(first_voucher_code) not like '%kulanz%'
		and first_voucher_code not in ('AX91-FLA3','ruecken2015') 
		--no case management/Gutachten/proaktive Steuerung
		and first_voucher_code not in ('AXAU-CM11','AXAU-22GS','AXAP-22GS','ARAG-CM22','NUBU-7777'))
		or first_voucher_code is null) 
	--only real, german cases
	and client_infos_1.client_group not in ('vip','other','swiss_kv')
	and (lower(description) not like '%vip%' or description is null)
	--no clients with restricted access or billing issues
	and client_1 not in ('Allianz Private Krankenversicherungs-AG',
						 'Generali Versicherung AG',
						 'BIG direkt gesund')
						 --'IKK - Die Innovationskasse',
						 --'BIG direkt gesund',
						 --'BKK24') 
	and (insurance not in ('Allianz Private Krankenversicherung',
							'BIG direkt gesund')
						  --'IKK - Die Innovationskasse',
						  --'BKK24',
						  --'BIG direkt gesund') 
						  or insurance is null)
	--if client is no health insurance, only coop-partners as insurance					  
	and (client_infos_3.client_group in ('gkv','pkv', 'corporate') or client_infos_3.client_group is null)
	and (client_infos_3.coop_status = 'TRUE' or client_infos_3.coop_status is null)
	--no beihilfe
	and not ((beihilfe is true and (lower(client_1) not like '%axa pkv%' and client_infos_1.client_group != 'corporate')) or beihilfe is null)
	--no vorstände/ansprechpartner etc.
	and case_id not in ('12-1212-111',
						'13-2230-551',
						'85-1144-354',
						'91-2140-04',
						'APJ7-JKDD-3B0A')
	--widerspruch mailings
	and case_id not in ('92-1142-005',
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
	and case_id not in (select 
							case_id
						from 
							md_campaigns.cam_select
							left join md_campaigns.cam_batch on cam_select.batch_id = cam_batch.batch_id
						where 
							cam_id in (2, 3)
						)
	and case_id not in (select 
							case_id
						from 
							md_campaigns.cam_select
							left join md_campaigns.cam_batch on cam_select.batch_id = cam_batch.batch_id
						where 
							cam_id in (1)
							and cam_batch.batch_selection_date < (current_date - interval '4 weeks')
						)
)
select
	distinct on (p.patient_id) p.patient_id,
	u."system",
	u.case_id,
	gender,
	anrede,
	title,
	first_name,
	last_name,
	result_date,
	county,
	country,
	email,
	client_1,
	client_2
from
	unification u
	left join md_patient_id.patient_id p using (case_id)
	left join patient-cs-facts._patients
order by 
	p.patient_id,
	result_date DESC


	
	
	
	
	
	
	
	
--insert into md_campaigns.cam_batch
select
	3 as cam_id,--Bestandskunden Mail
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'mail_chimp' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop