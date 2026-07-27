-----Auf Trello Board 'Proaktive Steuerung' hochladen. Spalte 'Nachtelefonie'

--insert into md_campaigns.cam_select 
-- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with nürnberger_daten as (
select
	cs.case_id,
	cs.payer_name,
	cs.payer_id,
	cs.voucher_code,
	cs.result_date,
	pcs.email,
	pcs.primary_phone,
	pcs.secondary_phone,
	pcs.exclude_surveys,
	pcs.exclude_sms,
	pcs.preferred_language
 FROM 
		analytics.cube_services cs
		LEFT JOIN pii_analytics.pii_cube_services pcs ON cs.service_id_key_systems = pcs.service_id_key_systems
where
	cs.payer_id = 330
	and cs.voucher_code ilike 'NUBU-7777'
	and cs.reject_date is null
	and cs.result_date between (current_date - interval '12 months') and (current_date - interval '3 months')
	--and results_sent_at::date between '2024-01-01' and '2024-11-17'
	and cs.case_id not in (select case_id
										from md_campaigns.cam_select
											left join md_campaigns.cam_batch using (batch_id)
										where 
											case_id is not null
											and cam_id = 51)
	and NOT lower(pcs.primary_phone) ~* 'ma'
	--and ( (results_sent_at::date between '2024-03-23' and '2024-04-30') or (results_sent_at::date between '2023-11-01' and '2023-11-14') )
	and cs.case_id not in (---------Todesfälle
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
)
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
	preferred_language
from
	nürnberger_daten
	
	

	

--insert into md_campaigns.cam_batch
select
	51 as cam_id,--------Anrufaktion Nürnberger
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop
	

