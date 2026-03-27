--insert into md_campaigns.cam_select
with case_cube as (
	select * from analytics.cube_services
	where 
		case_id is not null
		--and result_date between '2025-09-01' and '2025-11-30'
		and result_date >= current_date - interval '6 month'
		and payer_group in ('gkv', 'pkv')
		AND product = 'MSS'
		and kulanz_bool is false
		and payer_name not in ('Allianz Private Krankenversicherungs-AG',
								'DAK-Gesundheit',
								'Generali Health Solutions GmbH (GHS) (Unternehmenskunden)',
								'Generali Mobile Health App',
								'Generali Versicherung AG'
								)
		and voucher_code is not null 
		and voucher_code != ''
		and voucher_code not like 'VRTC%' ---Keine VIP-Fälle
		and bd_nps_value >= 7
		and (medic_nps_value >= 7 or medic_nps_value is null)
		and case_id not in (
										select case_id 
										from 
											md_campaigns.cam_select 
											left join md_campaigns.cam_batch using (batch_id)
										where 
											cam_id in (1)
											and case_id is not null
							)
		and dna_patient_id not in (
							select 
								cube_services.dna_patient_id
							from 
								md_campaigns.cam_select
								left join md_campaigns.cam_batch on cam_select.batch_id = cam_batch.batch_id
								left join analytics.cube_services on cam_select.case_id = cube_services.case_id
							where 
								cam_batch.cam_id in (1)
								and cube_services.dna_patient_id is not null
							)
		and (
    			completion_reason IS NULL
    		OR NOT (
		        completion_reason ILIKE '%Keine weitere Befragung%'
		        OR completion_reason ILIKE '%Befragung unangebracht%'
		        OR completion_reason ILIKE '%Interner Grund%'
		        OR completion_reason ILIKE '%Sonstig%'
    			)
			)
),
client_infos as (
	select * from analytics.dim_payers_infos
),
pii_case_cube as (
select 
	*,
	right(lower(email),length(email)-position('@' in email)+1) as domain
from 
	pii_analytics.pii_cube_services
),
final as (
	select
		distinct on (case_cube.case_id) case_cube.case_id,
		case_cube.dna_patient_id,
		case_cube.product,
		case_cube.result_date,
		case_cube.voucher_code,
		case_cube.payer_name,
		case_cube.payer_group,
		case_cube.insurance_main_name,
		case_cube.gender,
		case when case_cube.gender = 'F' then 'Frau'
			 when case_cube.gender = 'M' then 'Herr'
			 else null end as salutation,
		pii_case_cube.title,
		pii_case_cube.first_name,
		pii_case_cube.last_name,
		pii_case_cube.city,
		pii_case_cube.state,
		pii_case_cube.email,
		pii_case_cube.domain,
		case_cube.bd_nps_value,
		case_cube.medic_nps_value
	from case_cube
	left join client_infos on case_cube.payer_id = client_infos.client_id
	left join pii_case_cube on case_cube.service_id_key_systems = pii_case_cube.service_id_key_systems
	where
		client_infos.coop_status is TRUE --bei Koop-Partner versichert
		and pii_case_cube.country_code = 'de'
		--and coalesce(patients_parc.email, patients_borg.email) != '' 
		and pii_case_cube.domain in ('@gmail.com','@googlemail.com')
	order by 
		case_cube.case_id, 
		case_cube.dna_patient_id, 
		case_cube.result_date DESC
),
---------!!!!!Ergebnis für die Google Rezensionen
distinct_patients as (
	select distinct on (dna_patient_id) * 
	from final
	order by 
		dna_patient_id, 
		result_date desc
),
----------!!!!!nur für cam_select
cam_select as (
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
from distinct_patients
)
select * from cam_select



--insert into md_campaigns.cam_batch
select
	1 as cam_id,------Google recommendation campaign
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'mail_chimp' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop