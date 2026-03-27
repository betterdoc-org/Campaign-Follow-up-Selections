--insert into md_campaigns.cam_select 
--achtung, nur ausführen, wenn Karten wirklich erstellt werden!
with foo as (
SELECT
	services.case_id,
	payers.name, 
	services.voucher_code,
	communication_facts.preferred_language,
	survey_links.response_status,
	survey_links.created_at,
	surveys.title,
	case when services.case_id in (select survey_links.case_id 
									from "communication-cs-surveys".survey_links 
									left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
									where surveys.title like '%Status%') then 'SS' --Patient hat Status Survey inklusive PREM Link bekommen (Termin unbekannt)
		 when services.case_id in (select survey_links.case_id 
		 							from "communication-cs-surveys".survey_links
		 							left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
		 							where surveys.title like '%PREM%')
		 and services.case_id not in (select survey_links.case_id 
		 								from "communication-cs-surveys".survey_links
		 								left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id
		 								where surveys.title like '%Status%') then 'PREM' --Patient hat direkt PREM bekommen (Termin war bekannt)
		 end as case_fb,
	current_date - survey_links.created_at::date as fb_age --vor wie vielen Tagen wurde der FB verschickt
FROM
	"service-cs-selection".services
	LEFT JOIN "service-cs-selection".contracts ON services.contract_id = contracts.id
	LEFT JOIN "service-cs-selection".payers ON contracts.payer_id = payers.id
	LEFT JOIN "service-cs-selection".vouchers ON services.voucher_id = vouchers.id
	left join "case-cs-workflow-events".events ON services.case_id = events.case_id
	LEFT JOIN "communication-cs-surveys".survey_links ON survey_links.case_id = services.case_id
	LEFT JOIN "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id	
	LEFT JOIN "patient-cs-communication-facts".communication_facts ON services.case_id = communication_facts.case_id
WHERE
	(name like '%ÖKK%' or name like '%EGK%' or name like '%Helsana%' or name like '%Atupri%' or name like 'Aquilana%' or name like 'CSS%' or name like 'Agrisano%' or name like 'KPT%') --HIER PAYER ANPASSEN
	and services.deleted_at is null
	and events.event_type like '%DID_SEND_FINAL%'
	and services.service_id like '%:MSS%'
	and surveys.title like '%Nachbefragung%'
	and services.case_id not in (select survey_links.case_id id 
							from "communication-cs-surveys".survey_links
		 					left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id					
							where surveys.title like '%Nachbefragung%' and response_status is not null) --SS/PREM noch nicht beantwortet
	and services.case_id not in (select events.case_id from "case-cs-workflow-events".events where event_type like '%DID_CLOSE%' or event_type like '%DID_RECEIVE%') --betrachtet alle completion reasons, auch mehrfach nicht erreicht!! Ändern?
	--im folgenden: keine Fälle in Anrufaktion, bei denen schon PROM rausgegangen ist
	and services.case_id not in (select survey_links.case_id 
								from "communication-cs-surveys".survey_links
		 						left join "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id								
								where surveys.title like '%PROM%')
	and services.case_id not in (select conversations.case_id from "communication-cs-conversations".conversations where topics = '{feedback_follow_up_mss}')
)
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id,
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
	((title like '%PREM%' and case_fb = 'PREM')
		OR (title like '%Status%' and case_fb = 'SS'))
	and not (case_fb = 'PREM' and fb_age < 16) --PREM Versand vor mindestens 2 Wochen
	and not (case_fb = 'SS' and fb_age < 16) --Status Survey Versand vor mindestens 2 Wochen
	and not fb_age > 21
	and case_id not in ('AQ5T-YGS7-0J0J')
order by fb_age desc


--insert into md_campaigns.cam_select
----nur ausführen, wenn Karten erstellt werden
with foo as (
SELECT
	services.case_id,
	payers.name as client_name,
	voucher_code as first_voucher_code,
	_visits.visit_date,
	events.event_time,
	case when _visits.visit_timing = 1 and _visits.visit_date is not null then _visits.visit_date
		else events.event_time + interval '2 week'
		end as app_date,
	preferred_language
from
	"service-cs-selection".services
	LEFT JOIN "service-cs-selection".contracts ON services.contract_id = contracts.id
	LEFT JOIN "service-cs-selection".payers ON contracts.payer_id = payers.id
	LEFT JOIN "service-cs-selection".vouchers ON services.voucher_id = vouchers.id
	left join "case-cs-workflow-events".events ON services.case_id = events.case_id
	LEFT JOIN "communication-cs-surveys".survey_links ON survey_links.case_id = services.case_id
	LEFT JOIN "communication-cs-surveys".surveys ON surveys.id = survey_links.survey_id	
	LEFT JOIN "patient-cs-communication-facts"._communication_facts ON services.case_id = _communication_facts.case_id
	left join "medic-cs-visits"._visits on services.case_id = _visits.case_id
where 
	(name like '%Assura%' 
	or name like '%ÖKK%' 
	or name like '%EGK%' 
	or name like '%Helsana%' 
	or name like '%Atupri%' 
	or name like 'Aquilana%' 
	or name like 'CSS%' 
	or name like 'Agrisano%' 
	or name like 'KPT%'
	or name like '%Visana%') --HIER PAYER ANPASSEN
	and events.event_type like '%DID_SEND_FINAL%'
	and events.service like 'MSS'
	and (channel_recommendation like 'postal' or email is null or email like '')
Group by 
	services.case_id,
	payers.name,
	voucher_code,
	_visits.visit_date,
	events.event_time,
	case when _visits.visit_timing = 1 and _visits.visit_date is not null then _visits.visit_date
		else events.event_time + interval '2 week'
		end,
	preferred_language
)
SELECT
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
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
	app_date::date,
	null::text as korb,
	preferred_language
from
	foo 
where 
	date_part('week', app_date) = date_part('week', current_date)
	and date_part('year', app_date) = date_part('year', current_date)



--insert into md_campaigns.cam_batch
select
	6 as cam_id,---4 = de, 6 = ch --------------------------------------CH regulärer Prozess und tel. Karte wegen Aquilana etc unterscheiden?
	(select max(batch_id) from md_campaigns.cam_select) as batch_id,
	'bd_nachtelefonie' as batch_acceptor,
	current_date as batch_selection_date,
	current_date as batch_transfer_date,
	current_date as batch_start,
	current_date + 30 as batch_stop
	