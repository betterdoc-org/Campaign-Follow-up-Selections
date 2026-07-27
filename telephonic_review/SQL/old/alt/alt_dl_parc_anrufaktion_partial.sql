with services as (
	SELECT
		distinct case_id, product, payer_brand, to_date(body -> 'properties' ->> 'date','DD-MM-YYYY') as results_date
	FROM
		service_selections
		left join workflow_events using (case_id)
	WHERE
		service_selections.source_entry_deleted_at is NULL
		and kind like '%DID_SEND_FINAL%'
		and((service_selections.product LIKE 'MSS'
				AND workflow_events.body -> 'properties' ->> 'product' LIKE 'Spezi%')
			OR(service_selections.product LIKE 'MSS_%'
				AND workflow_events.body -> 'properties' ->> 'product' LIKE 'Weit%')
			OR(service_selections.product LIKE 'RSO'
				AND workflow_events.body -> 'properties' ->> 'product' LIKE '%Remote%')
			OR(service_selections.product LIKE 'SPC'
				AND workflow_events.body -> 'properties' ->> 'product' LIKE 'Frei%')
			OR(service_selections.product LIKE '%PMS%'
				AND workflow_events.body -> 'properties' ->> 'product' LIKE 'Erst%')))
		
, bd_feedbacks as (
	SELECT
		distinct case_id, case when nps is null then null else 'yes' end as bd_nps_flag
	FROM
		service_feedbacks)
		
, medic_feedbacks as (
	SELECT
		distinct case_id, case when nps is null then null else 'yes' end as medic_nps_flag
	FROM
		medic_visits
	WHERE
		nps is not null)
		
, rso_sustainability as (
	SELECT 
		distinct case_id, case when therapy_decision_patient is null then null else 'yes' end as rso_sustainability_flag
	FROM
		medic_opinions)
		
SELECT
	*
FROM
	services
	left join bd_feedbacks using (case_id)
	left join medic_feedbacks using (case_id)
	left join rso_sustainability using (case_id)



--HIER FLEXIBEL MIT DEN EINSCHLUSSKRITERIEN SPIELEN	

WHERE
	product like '%MSS%'
	and (bd_nps_flag is null AND medic_nps_flag is null)
	--and (bd_nps_flag is null OR medic_nps_flag is null)
	--and (bd_nps_flag is null OR medic_nps_flag is null) AND rso_sustainability_flag is NULL

--HIER FLEXIBEL MIT DEN EINSCHLUSSKRITERIEN SPIELEN	
	



ORDER BY
	results_date



