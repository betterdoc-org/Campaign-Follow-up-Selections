--insert into md_campaigns.cam_select
with cube_services as (
	select * from analytics.cube_services
),
campaign as (
	select * from pii_analytics.pii_base_campaign_existing_customer
),
cam_select as (
	select * from md_campaigns.cam_select
),
cam_batch as (
	select * from md_campaigns.cam_batch
),
all_patients as (
	select
		distinct on (campaign.dna_patient_id) campaign.dna_patient_id,
		campaign.service_id_key_systems,
		campaign.case_id,
		campaign.product,
		campaign.result_date,
		campaign.admission_channel,
		campaign.payer_name,
		campaign.coop_status,
		campaign.payer_group,
		campaign.insurance_main_name,
		campaign.voucher_code,
		campaign.voucher_code_second,
		campaign.gender,
		campaign.anrede,
		campaign.first_name,
		campaign.last_name,
		campaign.email,
		campaign.country_code,
		campaign.bd_nps_value,
		campaign.medic_nps_value,
		campaign.visited_recommended_medic
	from
		campaign
	where 
		payer_name NOT IN (
		      'Allianz Unfall/Haftpflicht',
		      'Allianz Private Krankenversicherungs-AG',
		      'Frau Hauck, Allianz Global',
		      'BIG direkt gesund',
		      'Generali Versicherung AG',
		      'Generali Health Solutions GmbH (GHS) (Unternehmenskunden)',
		      'Generali Mobile Health App',
		      'AOK Plus',
		      'Audi BKK',
		      'Versicherungskammer Bayern',
		      'Versicherungskammer Bayern bKV',
		      'mhplus Krankenkasse',
		      'DAK-Gesundheit',
		      'AOK Hessen',
		      'IKK - Die Innovationskasse',
		      'BIG direkt gesund',
		      'Mobil Krankenkasse',
		      'bkk melitta hmr',
		      'energie-BKK',
		      'IKK - Die Innovationskasse',
		      'Salus BKK',
		      'Inter Versicherungsgruppe'
		    				)
		and campaign.result_date between (current_date - interval '6 year') and (current_date - interval '3 month')
		--and campaign.result_date between '2024-07-01' and '2024-12-31'
		--and (campaign.medic_nps_value not in (0,2,3,4,5,6) or campaign.medic_nps_value is null)
		--and (campaign.bd_nps_value not in (0,2,3,4,5,6) or campaign.bd_nps_value is null)
		/*and campaign.case_id not in (
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
		and campaign.dna_patient_id not in (
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
							)*/
		------------------------------
		--exclude cases who was conntacted for google campaigns 2025
		------------------------------
		and campaign.case_id not in (
							select 
								cam_select.case_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
							where 
								(cam_batch.cam_id in (1)
									and cam_batch.batch_selection_date > current_date - interval '1 year'
								)
								and cam_select.case_id is not null
							)
		and campaign.dna_patient_id not in (
							select 
								cube_services.dna_patient_id
							from 
								cam_select
								left join cam_batch on cam_select.batch_id = cam_batch.batch_id
								left join cube_services on cam_select.case_id = cube_services.case_id
							where 
								(cam_batch.cam_id in (1)
									and cam_batch.batch_selection_date > current_date - interval '1 year'
								)
								and cube_services.dna_patient_id is not null
							)
	order by 
		campaign.dna_patient_id,
		campaign.result_date DESC
),
final_cam_select as (
select 
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	case_id,
	payer_name as client_name,
	voucher_code as first_voucher_code,
	dna_patient_id as patient_id,
	0 as control_flag, 
	1 as count_cases_patient, 
	null::numeric as reached_patient_flag,
	null::date as reached_date, 
	null::text as refusal_reason,
	null::numeric as opt_out, 
	null::text as new_case_id,
	null::text app_state
from all_patients
)
select * from all_patients order by result_date ASC



--insert into md_campaigns.cam_batch
select
	3 as cam_id,--Bestandskundenaktion (email)
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'mail_chimp' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop