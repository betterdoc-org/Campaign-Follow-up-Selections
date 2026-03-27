--insert into md_campaigns.cam_select 
-- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with foofoo as
(with foo as
(SELECT
	i.case_id,
	i.voucher_ids[0]::int as voucher_id,
	i.results_sent_at::timestamp::date,
	ia.appointment_type,
	case when ia.state like 'call_patient' then i.results_sent_at::timestamp::date
		when ia.state like 'sent' then ia.scheduled_date else i.results_sent_at::timestamp::date end as app_date,
	ia.state as app_state,
	i.state as case_state,
	i.completion_reason_main,
	i.completion_reason_additional,
	i.completion_details,
	ice.q3_result
from staging.stg_borg__inquiries i
	LEFT JOIN staging.stg_borg__inquiry_appointments ia on ia.inquiry_id = i.id
	LEFT JOIN staging.stg_borg__inquiry_physician_contact_entries ice ON ice.inquiry_id = i.id
WHERE
	--inquiries.source_entry_deleted_at is NULL
	(i.state like 'waiting_q3' or i.state like 'completed')
	and treatment_type not like 'second_opinion_before_surgery'
	and i.id not IN (SELECT inquiry_id FROM staging.stg_borg__inquiry_physician_contact_entries WHERE q3_result is not NULL)
	and ia.appointment_type = '3'
	and ia.state not in ('ready_to_send_pdf', 'send_manually', 'completed', 'waiting') -----------------completed darf eig drin sein. Problem: ausgefüllter FB, der noch nicht eingetragen ist
	and (completion_reason_main is null or completion_reason_main like 'internal_reason')
	)
SELECT
	distinct case_id,
	v.client_name as voucher_client_name,
	row_number () over (partition by case_id order by voucher_id) as rn,
	v.value as first_voucher_code,
	app_state,
	app_date,
	case_state as korb,
	completion_reason_main,
	completion_reason_additional,
	completion_details
FROM
	foo
left join staging.stg_borg__vouchers v on v.id = foo.voucher_id
WHERE
	v.client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff') -- ARAG Casemanagement (CM22), AXA Haftpflicht, DAK -LA3F, keine Testanfragen, VIPs, BetterDoc Staff, Promo, Endung -BDTC
	and (v.value not like '%-MSSNACHRSO' and v.value not like '%-BDTC' and v.value not like 'NUBU-7777')
ORDER by
	app_state, voucher_client_name)
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
	case_id,
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
	--and first_voucher_code like 'NUKV-KT11'
	and voucher_client_name ilike '%Osterhus%'---------------------------------------- hier Payer einsetzen!
	and case_id not in ('A6PY-TX0G-PC11', 'AM8X-SDM1-ZQ01', 'ADRF-4RK3-VC0J')
	-------------------manuelle Zeitspanne!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	/*and ((app_state like 'deleted' and korb like 'completed' and app_date between '2021-01-01' and '2023-08-07') --letzter Tag der letzten Woche
	or (app_state like 'call_patient' and app_date BETWEEN '2021-01-01' and '2023-06-26') --Sendout heutige KW - 6 Wochen
	or (app_state like 'sent' and app_date between '2021-01-01' and '2023-07-03')) --FB Versand vor mindestens 5 Wochen*/
	-------------------automatisierte Zeitspanne!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		and ((app_state like 'deleted' and korb like 'completed' and app_date between (current_date - interval '6 month') and current_date) --letzter Tag der letzten Woche
	or (app_state like 'call_patient' and app_date BETWEEN (current_date - interval '6 month') and (current_date - interval '6 weeks')) --Sendout heutige KW - 6 Wochen
	or (app_state like 'sent' and app_date between (current_date - interval '6 month') and (current_date - interval '5 weeks'))) --FB Versand vor mindestens 5 Wochen 
	and case_id not in (select case_id
						from md_campaigns.cam_select cs
						left join md_campaigns.cam_batch using (batch_id)
						where cam_id not in (1, 2, 3, -1, 8))
	and case_id not in (select case_id
						from md_campaigns.cam_select cs
							left join md_campaigns.cam_batch using (batch_id)
						where 
							cam_id in (8)
							and batch_selection_date >= date_trunc('week', current_date)::date)
order by case_id


--insert into md_campaigns.cam_batch
select
	/*nachfolgend Zahl einfügen und später wieder löschen */ as cam_id, -------------cam_id ist in cam_cam: Achtung es darf immer nur eine Kampagne existieren
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop

	
	
-------ACHTUNG!! Nur bei einer neuen Kampagne, eine neue cam_id erstellen!!!
	
--insert into md_campaigns.cam_cam
select
	(select max(cam_id) from md_campaigns.cam_cam) + 1 as cam_id,
	'phone' as cam_channel,
	'tesa Werk Hamburg GmbH' as cam_name, -----------------------------------------ACHTUNG hier Namen anpassen!!!
	'extra call campaign for our follow ups - tesa Werk Hamburg GmbH' as cam_description, ------------ACHTUNG hier Namen anpassen!!!
	'bd_patients' as cam_audience,
	'information' as cam_type,
	'2022-01-01' as valid_from,    ------Datum?
	'2999-12-31' as valid_to,
	'Anrufaktion' as cam_category
	
	

	
----------------Unterstützung zur Suche der passenden Kamapgne
select *
from md_campaigns.cam_cam
order by cam_name ASC
	