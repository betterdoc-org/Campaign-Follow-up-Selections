--- DE Fälle tel. Nachbefragung
--insert into md_campaigns.cam_select -- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with foofoo as (
with foo as
(SELECT
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
	inquiries.state like 'waiting_q3'
	and inquiries.treatment_type like 'second_opinion_before_surgery'
	and inquiry_physician_contact_entries.q3_result is null
	and inquiry_appointments.appointment_type = '3'
	and inquiry_appointments.state in('sent', 'call_patient')
	and inquiry_appointments.inquiry_id not in (select inquiry_id from borg.inquiry_appointments where (appointment_type = '3' and state like 'completed'))
	)
SELECT
	distinct masked_id,
	row_number () over (partition by masked_id order by voucher_id) as rn,
	v.client_name as voucher_client_name,
	v.value as first_voucher_code,
	app_state,
	app_date,
	case_state as korb,
	date_part('week', current_date) as current_week,
	date_part('week', app_date) as week_appdate
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
	/*and ((current_week - week_appdate > 0 and (date_part('year', app_date) = date_part('year', current_date) 
		and ((app_state like 'call_patient' and current_week - week_appdate = 6)
		or (app_state like 'sent' and current_week - week_appdate = 5))))
		or (current_week - week_appdate < 0 and (date_part('year', app_date) = (date_part('year', current_date)-1)
		and((app_state like 'call_patient' and week_appdate - current_week = 46) --achtung, in 2026 hat das Jahr 53 KWs!
		or (app_state like 'sent' and week_appdate - current_week = 45)))))*/
	and ((app_state like 'sent'
		and date_part('week', current_date) = date_part('week', (app_date + interval '5 weeks'))
		and date_part('year', (app_date + interval '5 weeks')) = date_part('year', current_date))
	or (app_state like 'call_patient'
		and date_part('week', current_date) = date_part('week', (app_date + interval '6 weeks'))
		and date_part('year', (app_date + interval '6 weeks')) = date_part('year', current_date)))
order by app_state, voucher_client_name


--- Post Fälle tel Nachbefragung
--insert into md_campaigns.cam_select -- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
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
	--and ia.scheduled_date between (current_date - 7) and (current_date - 1)-----------------------Datum anpassen
	and date_part('week', ia.scheduled_date) = date_part('week', (current_date - interval '1 week')) -----letzte Woche
	and date_part('year', ia.scheduled_date) = date_part('year', (current_date - interval '1 week'))
order by scheduled_date, treatment_type, i.state)
select 
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	masked_id as case_id,
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
from foo 
left join borg.vouchers v on v.id = foo.voucher_id
where voucher_id is not null
	and (v.client_name like 'DAK' or treatment_type like 'second_opinion_before_surgery')
and v.client_name not like 'AXA Haftpflichtversicherung'
order by scheduled_date
--select masked_id, i.state, ia.appointment_type, ia.state  from borg.inquiries i left join borg.inquiry_appointments ia on i.id = ia.inquiry_id where masked_id like '11-2424-011'


--insert into md_campaigns.cam_batch
select
	4 as cam_id,--4 = de, 6 = ch
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop
	
	

---Post Fälle, die completed werden müssen!!!
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
	--and ia.scheduled_date between (current_date - 7) and (current_date - 1)-----------------Datum anpassen
	and date_part('week', ia.scheduled_date) = date_part('week', (current_date - interval '1 week')) -----letzte Woche
	and date_part('year', ia.scheduled_date) = date_part('year', (current_date - interval '1 week'))
order by scheduled_date, treatment_type, i.state)
select 
	masked_id as case_id,
	v.client_name as client_name,
	app_state,
	treatment_type
from foo 
left join borg.vouchers v on v.id = foo.voucher_id
where voucher_id is not null
	and (v.client_name not like 'DAK' and treatment_type not like 'second_opinion_before_surgery')
and v.client_name not like 'AXA Haftpflichtversicherung'
order by scheduled_date
--select masked_id, i.state, ia.appointment_type, ia.state  from borg.inquiries i left join borg.inquiry_appointments ia on i.id = ia.inquiry_id where masked_id like '11-2424-011'