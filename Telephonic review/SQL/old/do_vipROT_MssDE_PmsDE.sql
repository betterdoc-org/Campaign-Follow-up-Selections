
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Achtung batch_id wird nur beim ersten Skript neu erstellt
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!



--MSS Deutschland
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
	v.value like '%VRTC%'
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




--insert into md_campaigns.cam_select 
-- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with foo as (
SELECT
	services.case_id,
	payers.name, 
	services.voucher_code,
	communication_facts.preferred_language,
	survey_links.response_status,
	survey_links.created_at,
	case when surveys.title like '%PMS%' then surveys.title
			else null
			end as title,
	case when services.case_id in (select survey_links.case_id 
									from "communication-cs-surveys".survey_links 
									left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
									where surveys.title like '%Allrounder%') then 'Allrounder' --Patient hat Status Survey inklusive PREM Link bekommen (Termin unbekannt)
		 when services.case_id in (select survey_links.case_id 
		 								from "communication-cs-surveys".survey_links
		 								left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
		 								where surveys.title like '%Reha%') then 'Reha' --Patient hat direkt PREM bekommen (Termin war bekannt)
		 else null
		 end as case_fb,
	case when surveys.title like '%PMS%' then current_date - survey_links.created_at::date
			when surveys.title not like '%PMS%' then current_date - fazit::date
				end as fb_age, --vor wie vielen Tagen wurde der FB verschickt
	fazit,
	events.event_type,
	events.event_body -> 'properties' ->> 'reason' as grund,
	channel_recommendation
FROM
	"service-cs-selection".services
	LEFT JOIN "service-cs-selection".contracts ON services.contract_id = contracts.id
	LEFT JOIN "service-cs-selection".payers ON contracts.payer_id = payers.id
	LEFT JOIN "service-cs-selection".vouchers ON services.voucher_id = vouchers.id
	left join "case-cs-workflow-events".events ON services.case_id = events.case_id
	LEFT JOIN "communication-cs-surveys".survey_links ON survey_links.case_id = services.case_id
	LEFT JOIN "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id	
	LEFT JOIN "patient-cs-communication-facts".communication_facts ON services.case_id = communication_facts.case_id
	LEFT JOIN (select case_id, fazit
				from (select case_id, event_time as fazit, row_number() over (partition by case_id order by event_time desc) as rn
						from "case-cs-workflow-events".events 
						where event_type like 'DID_SEND_FINAL%') no_dublicates
				where rn = 1
						) fazit ON services.case_id = fazit.case_id
WHERE
	services.deleted_at is null
	and voucher_code like '%VRTC%'
	--and (name like '%TÜV%') --------------------------------------------------------------------HIER PAYER ANPASSEN
	and events.event_type like '%DID_SEND_FINAL%'
	and services.service_id like '%:PMS%'
	and (surveys.title like '%Nachbefragung%' or channel_recommendation not like 'email')
	and services.case_id not in (select survey_links.case_id 
							from "communication-cs-surveys".survey_links
		 					left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id					
							where surveys.title like '%Nachbefragung%' and response_status is not null) --SS/PREM noch nicht beantwortet
	and services.case_id not in (select case_id from "case-cs-workflow-events".events where event_type like '%DID_RECEIVE%') --betrachtet alle completion reasons, auch mehrfach nicht erreicht!! Ändern?
	and services.case_id not in (select events.case_id from "case-cs-workflow-events".events where event_type like '%DID_CLOSE%' and (event_body -> 'properties' ->> 'reason' like '%Keine weitere Befragung%' or event_body -> 'properties' ->> 'reason' like '%Befragung unangebracht%' or event_body -> 'properties' ->> 'reason' like '%Kontaktadresse%'))
	--im folgenden: keine Fälle in Anrufaktion, bei denen schon PROM rausgegangen ist
	and services.case_id not in (select survey_links.case_id 
								from "communication-cs-surveys".survey_links
		 						left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id								
								where surveys.title like '%PROM%')
	and services.case_id not in (select conversations.case_id from "communication-cs-conversations".conversations where product like 'PMS%'and topics = '{feedback_follow_up}')
	and services.case_id not in (select case_id
								from md_campaigns.cam_select cs
									left join md_campaigns.cam_batch cb on cs.batch_id = cb.batch_id
								where cam_id not in (1, 2, 3, 8, -1))
)
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) as batch_id, ----------Überprüfen, ob die Lieferung mit MSS verknüpft werden kann
	case_id, 
	name as client_name,
	voucher_code as first_voucher_code,
	null::int as patient_id,
	0 as control_flag, 
	1 as count_cases_patient, 
	null::numeric as reached_patient_flag,
	null::date as reached_date, 
	null::text as refusal_reason,
	null::numeric as opt_out, 
	null::text as new_case_id,
	case when case_fb like 'SS' then 'call_patient' when case_fb like 'PREM' then 'sent' end as app_state,
	null::text as korb,
	preferred_language
from
	foo
where
	fb_age > 16-- Versand vor mindestens 2 Wochen
	and fazit between (current_date - interval '5 month') and (current_date - interval '1 month')
order by fb_age desc







--Achtung richtige batch-id?
--insert into md_campaigns.cam_batch
select
	14 as cam_id,---VIP-Rot
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop