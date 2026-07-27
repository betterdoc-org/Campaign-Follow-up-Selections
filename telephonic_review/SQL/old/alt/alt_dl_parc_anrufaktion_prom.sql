with foo as (
SELECT
	row_number() over (partition by case_id order by case_id, link_created_at desc) as n_row, --kürzlichster PROM als 1 (oben)
	case_id, 
	product, 
	payer_brand,
	link_created_at::date - to_date(body -> 'properties' ->> 'date','DD-MM-YYYY') as time_diff,
	case when link_created_at::date - to_date(body -> 'properties' ->> 'date','DD-MM-YYYY') < 364 then 'prom_6' --stimmen diese Gruppierungen
		 when link_created_at::date - to_date(body -> 'properties' ->> 'date','DD-MM-YYYY') > 720 then 'prom_24'
		 else 'prom_12' end as prom_type,
	survey_title, 
	response_status
FROM
	service_selections
	left join workflow_events using (case_id)
	left join surveys using (case_id)
WHERE
	service_selections.source_entry_deleted_at is null
	and payer_brand LIKE '%Assura%'
	and product = 'MSS'
	and kind like '%DID_SEND_FINAL%'
	and survey_title like '%PROM%'
)
select * from foo 
where 
	n_row = 1 --nur den kürzlichsten PROM betrachten
	and not response_status is not null --der kürzlichste PROM ist unbeantwortet
order BY
	case_id, prom_type desc
	
	
	
--MSS_Further bedenken!