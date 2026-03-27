---HOC single question experiment to get mor prem_results
--insert into md_campaigns.cam_select
with rep as (
	select * from analytics.rep_campaing_single_question_prem_result
),
pii_cube_services as (
	select
		service_id_key_systems,
		email
	from 
		pii_analytics.pii_cube_services
),
selected_campaigns as (
select
	cam_select.case_id,
	cam_batch.batch_selection_date,
	cam_cam.cam_category
from
	md_campaigns.cam_select 
	left join md_campaigns.cam_batch using (batch_id)
	left join md_campaigns.cam_cam using (cam_id)
where 
	cam_batch.batch_selection_date > current_date - interval '1 month'
),
final as (
select 
	ROW_NUMBER() OVER (ORDER BY random()) AS random_order,
	rep.case_id, 
	rep.product,
	rep.dna_patient_id,
	rep.result_date, 
	rep.payer_name, 
	rep.payer_group,
	rep.voucher_code, 
	rep.admission_channel, 
	rep.preferred_channel, 
	rep.channel_reach_email,
	pii_cube_services.email,
	rep.inquiry_type, 
	rep.therapy_group_bc, 
	rep.therapy_bc, 
	rep.result as prem_result
from 
	rep
	left join pii_cube_services on rep.service_id_key_systems = pii_cube_services.service_id_key_systems
where 
	payer_group in ('gkv', 'pkv')
	and product = 'MSS'
	and result_date between (current_date - interval '7 month')
											and (current_date - interval '3 month')
	and reporting_bool is true
	and result is null
	and therapy_group_bc in ('Knie', 'Hüfte', 'Schulter')
	and therapy_bc not in ('Sonstige Operation')
	and automated_q0_op_cat = 1
	and inquiry_type in ('surgery', 'second_opinion_before_surgery')
	and case_id not in (select case_id from selected_campaigns)
	and cnt_cases_of_patient_last_two_years = 1
	and inquiries_state like 'waiting_q3'
	and appointment_type = '3'
	and inquiry_appointments_state in ('sent', 'call_patient')
	and channel_reach_email is true
	and preferred_channel = 'email'
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
from FINAL
)
select * from cam_select




--insert into md_campaigns.cam_batch
select
	76 as cam_id,------HOC single question prem result
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'mail_chimp' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop
	
	