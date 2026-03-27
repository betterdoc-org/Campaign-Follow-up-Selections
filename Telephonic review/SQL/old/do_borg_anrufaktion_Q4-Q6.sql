with foo as
(select
	distinct i.case_id,
	ia.scheduled_date,
	(i.voucher_ids)[0]::int as voucher_id,
	i.treatment_type,
	i.state as korb,
	ia.appointment_type, 
	ia.state as app_state
from 
	staging.stg_borg__inquiry_patient_data ipd
	left join staging.stg_borg__inquiries i on ipd.inquiry_id = i.id
	left join staging.stg_borg__inquiry_appointments ia on ipd.inquiry_id = ia.inquiry_id
	left join staging.stg_borg__inquiry_insurance_data iid on ipd.inquiry_id = iid.inquiry_id
where
	i.state in ('waiting_q4', 'waiting_q5', 'waiting_q6' )
	and ia.appointment_type not in ('1', '2')
	and ia.state like 'sent'
	and iid.options::text = '[]'
order by scheduled_date, treatment_type, i.state)
select 
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	case_id,
	v.client_name as client_name,
	v.value as first_voucher_code,
	null::int as patient_id,
	0 as control_flag, 
	1 as count_cases_patient, 
	null::numeric as reached_patient_flag,
	null::date as reached_date, 
	null::text as refusal_reason,
	null::numeric as opt_out, 
	null::text as new_case_id,
	app_state,
	korb,
	'de' as preferred_language
from 
	foo 
	left join staging.stg_borg__vouchers v on v.id = foo.voucher_id
where 
	voucher_id is not null
	and v.client_name like '%DB Regi%'
order by scheduled_date