with foo as
(select
	distinct i.masked_id,
	ia.scheduled_date,
	unnest(i.voucher_ids) as voucher_id,
	i.treatment_type,
	i.state as korb,
	ia.appointment_type, 
	ia.state as app_state
from borg.inquiry_patient_data ipd
left join borg.inquiries i on ipd.inquiry_id = i.id
left join borg.inquiry_appointments ia on ipd.inquiry_id = ia.inquiry_id
left join borg.inquiry_insurance_data iid on ipd.inquiry_id = iid.inquiry_id
where
	preferred_communication_channel like 'post'
	and i.state in ('waiting_q3', 'waiting_q4', 'waiting_q5', 'waiting_q6' )
	and ia.appointment_type not in ('1', '2')
	and ia.state like 'ready_to_send_pdf'
	and options = '{}'
	and ia.scheduled_date between (current_date - 8) and (current_date - 1)
order by scheduled_date, treatment_type, i.state)
select 
	masked_id,
	scheduled_date as app_date,
 	v.client_name as voucher_client_name,
	v.value as first_voucher_code,
	treatment_type,
	korb,
	appointment_type,
	app_state,
	'Tel. Nachbefragung' as parameter,
	current_date as card_created,
	'de' as preferred_language
from foo 
left join borg.vouchers v on v.id = foo.voucher_id
where voucher_id is not null
and v.client_name not like 'AXA Haftpflichtversicherung'
order by scheduled_date
--select masked_id, i.state, ia.appointment_type, ia.state  from borg.inquiries i left join borg.inquiry_appointments ia on i.id = ia.inquiry_id where masked_id like '11-2424-011'