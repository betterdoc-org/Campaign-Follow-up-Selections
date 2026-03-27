--insert into datalake_analytics.cam_telefonie
with foo as (
SELECT
	case_id, 
	workflow_events.product, 
	payer_brand, 
	voucher_code,
	preferred_language,
	survey_title,
	link_created_at,
	response_status,
	case when case_id in (select case_id from surveys where survey_title like '%Status%') then 'SS' --Patient hat Status Survey inklusive PREM Link bekommen (Termin unbekannt)
		 when case_id in (select case_id from surveys where survey_title like '%PREM%')
		 				   and case_id not in (select case_id from surveys where survey_title like '%Status%') then 'PREM' --Patient hat direkt PREM bekommen (Termin war bekannt)
		 end as case_fb,
	current_date - link_created_at::date as fb_age --vor wie vielen Tagen wurde der FB verschickt
FROM
	service_selections
	left join workflow_events using (case_id)
	left JOIN surveys using (case_id)
	left JOIN patient_communication_facts using (case_id)
WHERE
	service_selections.source_entry_deleted_at is null
	and (payer_brand like '%ÖKK%' or payer_brand like '%EGK%' or payer_brand like 'Sanitas%' or payer_brand like '%Helsana%' or payer_brand like '%Atupri%') --HIER PAYER ANPASSEN
	and kind like '%DID_SEND_FINAL%'
	and workflow_events.product like '%MSS%'
	and survey_title like '%Nachbefragung%'
	and case_id not in (select case_id from surveys where survey_title like '%Nachbefragung%' and response_status is not null) --SS/PREM noch nicht beantwortet
	and case_id not in (select case_id from workflow_events where kind like '%DID_CLOSE%' or kind like '%DID_RECEIVE%') --betrachtet alle completion reasons, auch mehrfach nicht erreicht!! Ändern?
	--im folgenden: keine Fälle in Anrufaktion, bei denen schon PROM rausgegangen ist
	and case_id not in (select case_id from surveys where survey_title like '%PROM%')
	and case_id not in (select case_id from communication_conversations where topics = '{feedback_follow_up_mss}')
)
select
	case_id, 
	payer_brand as voucher_client_name,
	voucher_code as first_voucher_code,
	case when case_fb like 'SS' then 'call_patient' when case_fb like 'PREM' then 'sent' end as app_state,
	link_created_at::date as app_date,
	'Tel. Nachbefragung' as parameter,
	current_date as card_created,
	'waiting_q3' as korb,
	preferred_language
	--fb_age
	--topics
from
	foo
where
	((survey_title like '%PREM%' and case_fb = 'PREM')
		OR (survey_title like '%Status%' and case_fb = 'SS'))
	and not (case_fb = 'PREM' and fb_age < 16) --PREM Versand vor mindestens 2 Wochen
	and not (case_fb = 'SS' and fb_age < 16) --Status Survey Versand vor mindestens 2 Wochen
	and not fb_age > 21
	and case_id not in ('AQ5T-YGS7-0J0J', 'ARJQ-HEPE-AB08')
order by fb_age desc