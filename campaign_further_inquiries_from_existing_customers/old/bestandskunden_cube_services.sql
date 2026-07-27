--insert into md_campaigns.cam_select
with cube_services as (
	select * from analytics.cube_services 
	--where result_date > '2024-09-01'
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
parc_patient_facts as (
	select * 
	from 
		"patient-cs-facts"._patients
		left join "patient-cs-facts"._cases on _patients.id = _cases.patient_id
		left join "patient-cs-communication-facts"._communication_facts on _cases.external_id = _communication_facts.case_id
),
/*borg_patient_facts as (
	select * 
	from 
		staging.stg_borg__inquiry_patient_data
		left join staging.stg_borg__inquiries on stg_borg__inquiry_patient_data.inquiry_id = stg_borg__inquiries.id
),*/
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
		case when cube_services.admission_channel IN ('online_marketing', 'partner_funnel') THEN 1 ELSE 0
	    	END AS om_flag,
	    case when cube_services.admission_channel IN ('online_marketing', 'partner_funnel') THEN 1 ELSE 0
	    	END AS online_admission,
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
		parc_patient_facts.first_name as parc_first_name,
		parc_patient_facts.last_name as parc_last_name,
		parc_patient_facts.email as parc_email,
		--borg_patient_facts.encrypted_first_name as borg_first_name,
		--borg_patient_facts.encrypted_last_name as borg_last_name,
		--borg_patient_facts.encrypted_email as borg_email,
		cube_services.country_code,
		cube_services.bd_nps_value,
		cube_services.medic_nps_value,
		cube_services.visited_recommended_medic
	from
		cube_services
		left join m_client_hist on cube_services.payer_name = m_client_hist.client_name
		left join parc_patient_facts on cube_services.case_id = parc_patient_facts.case_id
		--left join borg_patient_facts on cube_services.service_id_key_systems = borg_patient_facts.service_id_key_systems
		left join borg_beihilfe on cube_services.case_id = borg_beihilfe.case_id
	where 
		--cube_services.result_date between (current_date - interval '48 month') 
			--and (current_date - interval '24 month')
		cube_services.result_date between '2020-01-01' and '2024-09-30'
		and (cube_services.medic_nps_value not in (0,2,3,4,5,6) or cube_services.medic_nps_value is null)
		and (cube_services.bd_nps_value not in (0,2,3,4,5,6) or cube_services.bd_nps_value is null)
		--and cube_services.bd_nps_value is not null
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
		and cube_services.payer_name not like '%Audi BKK%'
		and m_client_hist.coop_status is true
		and LENGTH(parc_patient_facts.email) >= 4--LENGTH(coalesce(parc_patient_facts.email, borg_patient_facts.encrypted_email)) >= 4
		and parc_patient_facts.last_name is not null--coalesce(parc_patient_facts.last_name, borg_patient_facts.encrypted_last_name) is not null
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
						'14-1511-114',
						'AHTV-ENYY-WV11',
						'A8Y6-10FF-PB14')
		and cube_services.case_id not in (
							select 
								cam_select.case_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
							where 
								(cam_batch.cam_id in (3)
									and cam_batch.batch_selection_date > current_date - interval '1 year'
								)
								and cam_select.case_id is not null
							)
		and cube_services.dna_patient_id not in (
							select 
								cube_services.dna_patient_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
								left join cube_services on cam_select.case_id = cube_services.case_id
							where 
								(cam_batch.cam_id in (3)
									and cam_batch.batch_selection_date > current_date - interval '1 year'
								)
								and cube_services.dna_patient_id is not null
							)
		/*and cube_services.case_id not in (
							select 
								cam_select.case_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
							where 
								(cam_batch.cam_id in (3)
									or cam_batch.batch_selection_date > current_date - interval '1 month'
								)
								and cam_select.case_id is not null
							)
		and cube_services.dna_patient_id not in (
							select 
								cube_services.dna_patient_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
								left join cube_services on cam_select.case_id = cube_services.case_id
							where 
								(cam_batch.cam_id in (3)
									or cam_batch.batch_selection_date > current_date - interval '1 month'
								)
								and cube_services.dna_patient_id is not null
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
		om_flag,
		payer_group,
		voucher_code,
		gender,
		anrede,
		parc_first_name,
		parc_last_name,
		parc_email,
		--borg_first_name,
		--borg_last_name,
		--borg_email,
		country_code
	from
		all_patients
)/*,
final_cam_select as (
select 
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	case_id,
	payer_name as client_name,
	voucher_code as first_voucher_code,
	patient_id,
	0 as control_flag, 
	1 as count_cases_patient, 
	null::numeric as reached_patient_flag,
	null::date as reached_date, 
	null::text as refusal_reason,
	null::numeric as opt_out, 
	null::text as new_case_id,
	null::text app_state
from final
)*/
select * from final




--insert into md_campaigns.cam_batch
select
	3 as cam_id,--Bestandskundenaktion (email)
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'mail_chimp' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop