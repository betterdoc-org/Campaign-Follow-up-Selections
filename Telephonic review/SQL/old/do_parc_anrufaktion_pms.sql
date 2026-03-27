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
	and (name like '%tesa%') --------------------------------------------------------------------HIER PAYER ANPASSEN
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
	and services.case_id not in (select case_id from staging.stg_communication_cs_conversations__conversations where product like 'PMS%'and topics = '["feedback_follow_up"]')
	and services.case_id not in (select case_id
								from md_campaigns.cam_select cs
									left join md_campaigns.cam_batch cb on cs.batch_id = cb.batch_id
								where cam_id not in (1, 2, 3, 8))
)
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id, ----------Überprüfen, ob die Lieferung mit MSS verknüpft werden kann
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
	and case_id not in ('AQ5T-YGS7-0J0J') ------------------------------------------------------------------------ACHTUNG: nachfolgend zukünftig rauslöschen!!!
	and case_id not in (select distinct case_id from md_analytics.events_cube where board = 'Nachbefragung CH PARC' and list = 'INBOX CH Original LZ' and action_type = 'moveCardToBoard' and card_create_timestamp between '2023-05-05 07:50:00' and '2023-05-05 08:15:00' and case_id like 'A%') 
	and fazit between (current_date - interval '1 year') and (current_date - interval '1 month')
order by fb_age desc





--insert into md_campaigns.cam_batch
select
	/*nachfolgend Zahl einfügen und später wieder löschen */73 as cam_id, -------------cam_id ist in cam_cam: Achtung es darf immer nur eine Kampagne existieren
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
	'LVM Krankenversicherungs-AG' as cam_name, -----------------------------------------ACHTUNG hier Namen anpassen!!!
	'extra call campaign for our follow ups - LVM Krankenversicherungs-AG' as cam_description, ------------ACHTUNG hier Namen anpassen!!!
	'bd_patients' as cam_audience,
	'information' as cam_type,
	'2022-01-01' as valid_from,    ------Datum?
	'2999-12-31' as valid_to
	
	

	
----------------Unterstützung zur Suche der passenden Kamapgne
select *
from md_campaigns.cam_cam

