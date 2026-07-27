with foofoo as
(with foo as
(SELECT
	i.masked_id,
	unnest(i.voucher_ids) as voucher_id,
	i.results_sent_at::timestamp::date,
	ia.appointment_type,
	case when ia.state like 'call_patient' then i.results_sent_at::timestamp::date
		when ia.state like 'sent' then ia.scheduled_date else i.results_sent_at::timestamp::date end as app_date,
	ia.state as app_state,
	i.state as case_state,
	i.completion_reason_main,
	i.completion_reason_additional,
	i.completion_details,
	ice.q3_result,
	ipd.preferred_communication_channel as channel
	--case when inquiry_appointments.state like 'call_patient' then current_date - send_out_date
		--when inquiry_appointments.state like 'sent' then current_date - scheduled_date end as age
from borg.inquiries i
	LEFT JOIN borg.inquiry_appointments ia on ia.inquiry_id = i.id
	LEFT JOIN borg.inquiry_physician_contact_entries ice ON ice.inquiry_id = i.id
	left join borg.inquiry_patient_data ipd on ipd.inquiry_id = i.id
WHERE
	--inquiries.source_entry_deleted_at is NULL
	(i.state like 'waiting_q3' or i.state like 'completed')
	and treatment_type not like 'second_opinion_before_surgery'
	and i.id not IN (SELECT inquiry_id FROM borg.inquiry_physician_contact_entries WHERE q3_result is not NULL)
	and ia.appointment_type = '3'
	and ia.state not in ('ready_to_send_pdf', 'send_manually', 'completed', 'waiting')
	and (completion_reason_main is null or completion_reason_main like 'internal_reason')
	--and completion_reason_main not in ('survey_aborted_by_patient', 'patient_not_contactable', 'other', 'survey_inappropriate', 'patient_did_not_visit_recommended_physician')
	)
SELECT
	distinct masked_id,
	v.client_name as voucher_client_name,
	row_number () over (partition by masked_id order by voucher_id) as rn,
	v.value as first_voucher_code,
	channel,
	app_state,
	app_date,
	case_state as korb,
	completion_reason_main,
	completion_reason_additional,
	completion_details
FROM
	foo
left join borg.vouchers v on v.id = foo.voucher_id
WHERE
	v.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff') -- ARAG Casemanagement (CM22), AXA Haftpflicht, DAK -LA3F, keine Testanfragen, VIPs, BetterDoc Staff, Promo, Endung -BDTC
	and (v.value not like '%-MSSNACHRSO' and v.value not like '%-BDTC' and v.value not like 'NUBU-7777')
ORDER by
	app_state, voucher_client_name)
select
	distinct masked_id,
	voucher_client_name,
	first_voucher_code,
	channel,
	app_state,
	app_date,
	'Anrufaktion' as parameter,
	current_date as card_created,
	korb,
	completion_reason_main,
	completion_reason_additional,
	completion_details,
	'de' as preferred_language
from foofoo
where rn = '1'
	--and first_voucher_code like 'GADS-LF22'
	and voucher_client_name like '%Nürnberger Kranken%'-- hier Payer einsetzen!
	and masked_id not in ('AVJ5-WEXN-R606')
	and ((app_state like 'deleted' and korb like 'completed' and app_date between '2020-01-01' and '2022-10-30')
	or (app_state like 'call_patient' and app_date BETWEEN '2020-01-01' and '2022-09-25') --Sendout heutige KW - 6 Wochen
	or (app_state like 'sent' and app_date between '2020-01-01' and '2022-10-02')) --FB Versand vor mindestens 5 Wochen
order by channel, masked_id
	