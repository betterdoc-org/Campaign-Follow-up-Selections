with foo as (
SELECT
	case_id,
	voucher_client_name,
	client_group,
	first_voucher_code,
	country_code,
	betterdoc_nps
FROM
	case_selections
	left join datalake_analytics.m_client_infos on voucher_client_name = client
	left join vouchers using (case_id)
	left join patient_attributes using (case_id)
	left join betterdoc_nps using (case_id)
WHERE
	send_out_date IS NOT NULL --keine Abbrüche, kein Fall mehr im Prozess
	and voucher_client_name not like '%Allianz%' --Abrechnung
	and voucher_client_name not like '%Assura%' --Schweiz
	and voucher_client_name not like '%VIP%'
	and voucher_client_name not like '%BetterDoc%' --Staff
	and voucher_client_name not like '%Promo%'
	and voucher_client_name not like '%Test%'
	and voucher_client_name not like '%Patient%' --Patientenclub
	and client_group != 'Corporate'
	and first_voucher_code not in ('B1-L7 KULANZ','AX91-FLA3') --Kulanz
	and first_voucher_code not in ('ARAG-CM22','AXAU-22GS','AXAP-22GS') --Ansprechpartner ist Versicherungsmensch
	and first_voucher_code not like '%MA4L%' --Mitarbeitende der Versicherung
	and (country_code = 'DE' or country_code is null) --keine im Ausland lebenden Patienten
	and (betterdoc_nps >= 7 or betterdoc_nps is null) --keine Detraktoren BD NPS
	)
select * from foo 




