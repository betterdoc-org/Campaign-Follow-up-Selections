
with foofoo as (
with foo as
(SELECT
	masked_id,
	unnest(inquiries.voucher_ids) as voucher_id,
	appointment_type,
	borg.inquiries.results_sent_at,
	inquiry_appointments.state as app_state,
	inquiries.state as case_state
FROM 
	borg.inquiries
	LEFT JOIN borg.inquiry_appointments on inquiries.id = inquiry_appointments.inquiry_id
	left join borg.inquiry_physician_contact_entries on inquiry_physician_contact_entries.inquiry_id = inquiries.id
WHERE
	inquiries.state like 'waiting_q3'
	--and inquiries.treatment_type like 'second_opinion_before_surgery'
	and inquiry_physician_contact_entries.q3_result is null
	and inquiry_appointments.appointment_type = '3'
	and inquiry_appointments.state like 'completed'
	and masked_id not in (select masked_id 
							from borg.inquiries
								LEFT JOIN borg.inquiry_appointments on inquiries.id = inquiry_appointments.inquiry_id
							where appointment_type in ('4', '5', '6'))
	and masked_id not in (select masked_id 
							from borg.inquiries
								LEFT JOIN borg.inquiry_appointments on inquiries.id = inquiry_appointments.inquiry_id
							where borg.inquiry_appointments.state not like 'completed')
	)
SELECT
	distinct masked_id,
	row_number () over (partition by masked_id order by voucher_id) as rn,
	v.client_name as voucher_client_name,
	v.value as first_voucher_code,
	app_state,
	results_sent_at::date,
	case_state as korb
FROM
	foo
left join borg.vouchers v on v.id = foo.voucher_id
where
	v.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff')
	and (v.value not like '%-MSSNACHRSO' and v.value not like '%-BDTC' and v.value not like 'NUBU-7777' and v.value not like '%AXAP-%' and v.value not like 'ARAG-CM22')
ORDER BY
	app_state, voucher_client_name)
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	masked_id as case_id,
	results_sent_at,
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
	app_state
from foofoo
where rn = '1'
	and results_sent_at > '2023-10-23'
	--and results_sent_at < '2023-10-25'
	and masked_id not in ( select case_id as masked_id
							from md_analytics.events_cube ec 
							where Board like 'Analytics: Eingegangene Frabö' 
								and list like 'Neu eingegangene Frabö' 
								and transaction_name like 'action_create_card'
						)
order by results_sent_at desc




----------------entsprechenden Namen ergänzen, sodass das Cleaning zugewiesen werden kann





