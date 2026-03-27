-- Alle WF events
SELECT
	case_id, payer_brand, insurance_name, voucher_code, voucher_description, betterdoc_nps, betterdoc_nps_reason
FROM
	service_selections
	LEFT JOIN workflow_events USING (case_id)
	LEFT JOIN healthinsurance_coverage USING (case_id)
	left join betterdoc_nps using (case_id)
WHERE
	kind LIKE '%DID_SEND_FINAL%'
	AND(product LIKE 'RSO'
		OR product LIKE '%PMS%')
	AND insurance_type = 'primary'
	AND payer_brand NOT LIKE '%Allianz%'
	AND payer_brand NOT LIKE '%BetterDoc%'
	AND payer_brand NOT LIKE '%BKK24%'
	AND payer_brand NOT LIKE '%IKK - Die Inno%'
	AND payer_brand NOT LIKE '%Patientenclub%'
	AND payer_brand NOT LIKE '%VIP%'
	and payer_brand not like '%HDI%'
	and voucher_code not like '%MA4L%'
	and voucher_code not like 'DAK1-LA3F'
	and voucher_code not like 'DAK-RZM-LEAD'
	and voucher_code not like 'DAKG-MSSNACHRSO'