--Karten hochladen in: Liste Nürnberger Nachbetreuung auf dem Board Tel. Nachbetreuung


--insert into md_campaigns.cam_select 
-- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with cube_services as (
	select * from analytics.cube_services
	---onco cases should be excluded for this selection
	where inquiry_type != 'second_opinion_oncology'
),
communication_facts as (
	select * from pii_staging.pii_stg_patient_cs_communication_facts__communication_facts
),
dim_clean_borg_results as (
	select * from analytics.dim_clean_borg_results
),
final as (
	select 
		distinct on (cube_services.case_id) cube_services.case_id,
		dim_clean_borg_results.least_result_date as result_date,
		cube_services.payer_name,
		cube_services.voucher_code
	from 
		cube_services
		LEFT JOIN communication_facts ON cube_services.case_id = communication_facts.case_id
		left join dim_clean_borg_results on cube_services.service_id_key_systems = dim_clean_borg_results.service_id_key_systems
	where 
		cube_services.product = 'MSS'
		and cube_services.case_id not in (
							select 
								case_id
							from 
								md_campaigns.cam_select
								left join md_campaigns.cam_batch on cam_select.batch_id = cam_batch.batch_id
							where 
								case_id is not null
								and cam_batch.cam_id = 57
							)
		and NOT communication_facts.primary_phone ~* 'Ma'
		and cube_services.case_id not in (---------Todesfälle
						'AP4X-83BZ-DS11',
						'A1Y5-46MP-0308',
						'APT7-MCWW-DF0G',
						'AWFT-AX4S-760F',
						'AYE2-HAV8-4N03',
						'AMQB-D5E0-ZH07',
						'AYSW-9XBE-JB01',
						'A5DF-VJGE-0H0Y',
						'ASFK-TZDY-D70R',
						'A80C-P6PQ-6R0C',
						'AVDA-WPHH-SM0B',
						'A4MS-K9KK-3X0W'
						)
		 and cube_services.voucher_code = 'NUBU-7777'
	order by 
		cube_services.case_id,
		dim_clean_borg_results.least_result_date DESC
),
final_cam_select as (
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id, ----------Überprüfen, ob die Lieferung mit MSS verknüpft werden kann
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
	'call_patient' as app_state,
	null::text as korb,
	null::text as preferred_language
from
	final
where 
	result_date between '2025-10-27' and '2025-11-12'
)
select * from final_cam_select




--insert into md_campaigns.cam_batch
select
	57 as cam_id,--------Anrufaktion Nürnberger
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop