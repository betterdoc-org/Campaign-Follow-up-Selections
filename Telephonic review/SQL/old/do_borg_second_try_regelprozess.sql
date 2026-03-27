--- DE Fälle tel. Nachbefragung
--insert into md_campaigns.cam_select -- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with borg as (
SELECT
	masked_id,
	unnest(inquiries.voucher_ids) as voucher_id,
	appointment_type,
	case when inquiry_appointments.state like 'call_patient' then results_sent_at
		when inquiry_appointments.state like 'sent' then scheduled_date else results_sent_at end as app_date,
	inquiry_appointments.state as app_state,
	inquiries.state as case_state
FROM 
	borg.inquiries
	LEFT JOIN borg.inquiry_appointments on inquiries.id = inquiry_appointments.inquiry_id
	left join borg.inquiry_physician_contact_entries on inquiry_physician_contact_entries.inquiry_id = inquiries.id
WHERE
	inquiries.state in ('waiting_q3', 'completed')
	and inquiries.treatment_type like 'second_opinion_before_surgery'
	and inquiry_physician_contact_entries.q3_result is null
	and inquiry_appointments.appointment_type = '3'
	and inquiry_appointments.state in ('completed', 'sent', 'call_patient')
	and masked_id not in (select case_id as masked_id from md_campaigns.cam_select left join md_campaigns.cam_batch using (batch_id) where cam_id = 56)
	and (completion_reason_main not in ('survey_inappropriate','survey_aborted_by_patient', 'patient_did_not_visit_recommended_physician', 'employee_request', 'patient_not_contactable', 'survey_completed') or completion_reason_additional not in ('patient_critically_ill_or_dead','patient_difficult','doc_inquiry','not_satisfied_with_recommendations_had_other_expectations_regarding_the_service', 'got_recommendations_as_precaution', 'free_of_pain', 'treated_by_a_non_bd_physician', 'b2b_no_survey', 'test_inquiry'))
	),
voucher_exclusion as (
SELECT
	distinct masked_id,
	row_number () over (partition by masked_id order by voucher_id) as rn,
	v.client_name as voucher_client_name,
	v.value as first_voucher_code,
	app_state,
	app_date::date,
	case_state as korb
FROM
	borg
left join borg.vouchers v on v.id = borg.voucher_id
where
	v.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff')
	and (v.value not like '%-MSSNACHRSO' and v.value not like '%-BDTC' and v.value not like 'NUBU-7777' and v.value not like '%AXAP-%' and v.value not like 'ARAG-CM22')
ORDER BY
	app_state, voucher_client_name
),
final as (
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	masked_id as case_id,
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
from 
	voucher_exclusion
where 
	rn = '1'
	and app_date < current_date - interval '9 weeks'
	and app_date > current_date - interval '6 month'
	and first_voucher_code not in ('GADS-LF22', 'KULANZ-MSSNICHTKOOPP')
	--and voucher_client_name like '%Salus BKK%'--('mhplus', 'IKK Brandenburg und Berlin', 'vivida bkk')
order by app_state, voucher_client_name
)
select * from final

--insert into md_campaigns.cam_batch
select
	56 as cam_id,--second try Regelprozess
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop
	