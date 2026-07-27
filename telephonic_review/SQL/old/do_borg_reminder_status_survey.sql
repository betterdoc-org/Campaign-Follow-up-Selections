
--- DE Fälle Reminder Status Survey
--insert into md_campaigns.cam_select -- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
With borg_fälle as 
(
	SELECT
		distinct i.masked_id as case_id,
		i.results_sent_at::timestamp::date,
		ia.appointment_type,
		ia.state as app_state,
		i.state as case_state,
		ice.q3_result,
		unnest(i.voucher_ids) as voucher_id,
		status_survey_sent_at,
		status_survey_reminder_sent_at
	from 
		borg.inquiries i
		LEFT JOIN borg.inquiry_appointments ia on ia.inquiry_id = i.id
		LEFT JOIN borg.inquiry_physician_contact_entries ice ON ice.inquiry_id = i.id
		left join borg.inquiry_patient_data ipd on ipd.inquiry_id = i.id
	WHERE
		i.state like 'waiting_q3'
		and i.id not IN (SELECT inquiry_id FROM borg.inquiry_physician_contact_entries WHERE q3_result is not NULL)
		and ia.appointment_type = '3'
		and ia.inquiry_id not in (select inquiry_id from borg.inquiry_appointments where appointment_type = 3 and state in ('ready_to_send_pdf', 'send_manually', 'sent', 'deleted'))
		and ia.state not in ('ready_to_send_pdf', 'send_manually', 'completed', 'waiting', 'sent', 'deleted')
		and ipd.preferred_communication_channel like 'email'
		and masked_id not in (select masked_id 
							from borg.inquiries
								LEFT JOIN borg.inquiry_appointments on inquiries.id = inquiry_appointments.inquiry_id
							where appointment_type in ('4', '5', '6'))
		and masked_id not in (select case_id as masked_id
								from md_campaigns.cam_select cs
									left join md_campaigns.cam_batch cb on cs.batch_id = cb.batch_id
								where cam_id not in (1, 2, 3, -1))
		and status_survey_reminder_sent_at is null
		--and status_survey_sent_at is not null
),
vouchers as 
(
	select 
		case_id,
		row_number () over (partition by case_id order by voucher_id) as rn,
		results_sent_at,
		appointment_type,
		app_state,
		case_state,
		q3_result,
		voucher_id,
		client_name,
		v.value as first_voucher_code
	from 
		borg_fälle
		left join borg.vouchers v on v.id = borg_fälle.voucher_id
where
	v.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff')
	and (v.value not like '%-MSSNACHRSO' and v.value not like '%-BDTC' and v.value not like 'NUBU-7777' and v.value not like '%AXAP-%' and v.value not like 'ARAG-CM22')
)
select	
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	case_id,
	client_name,
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
	vouchers
where 
	rn = 1
	--and client_name in ()
	--and results_sent_at >= '2023-12-04'
	--and results_sent_at <= '2024-01-07'
	--and results_sent_at < (current_date - interval '5 weeks')
	/*and ((date_part('week', current_date) - date_part('week', results_sent_at) > 0 
		and (date_part('year', results_sent_at) = date_part('year', current_date) 
		and date_part('week', current_date) - date_part('week', results_sent_at) = 5))
	or (date_part('week', current_date) - date_part('week', results_sent_at) < 0 
		and (date_part('year', results_sent_at) = (date_part('year', current_date)-1) --achtung, in 2026 hat das Jahr 53 KWs!
		and date_part('week', results_sent_at) - date_part('week', current_date) = 45)))*/
	and date_part('week', current_date) = date_part('week', (results_sent_at + interval '5 weeks'))
	and date_part('year', (results_sent_at + interval '5 weeks')) = date_part('year', current_date)
order by case_id



--insert into md_campaigns.cam_batch
select
	8 as cam_id,
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_second_status_survey' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop