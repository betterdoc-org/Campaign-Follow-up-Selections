insert into datalake_analytics.cam_telefonie -- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with foo as
(SELECT
	masked_id,
	first_voucher_code,
	voucher_client_name,
	appointment_type,
	case when inquiry_appointments.state like 'call_patient' then send_out_date
		when inquiry_appointments.state like 'sent' then scheduled_date else send_out_date end as app_date,
	inquiry_appointments.state as app_state,
	inquiries.state as case_state,
	case when inquiry_appointments.state like 'call_patient' then current_date - send_out_date
		when inquiry_appointments.state like 'sent' then current_date - scheduled_date end as age
FROM 
	inquiries
	LEFT JOIN inquiry_appointments on inquiries.borg_id = inquiry_appointments.inquiry_borg_id
	LEFT JOIN vouchers using (case_id)
	LEFT JOIN case_selections using (case_id)
	LEFT JOIN raw_inquiry_physician_contact_entries on inquiries.borg_id = raw_inquiry_physician_contact_entries.inquiry_borg_id
WHERE
	inquiries.source_entry_deleted_at is NULL
	and inquiries.state like 'waiting_q3'
	and treatment_type like 'second_opinion_before_surgery'
	and q3_result is null
	and appointment_type = '3'
	and inquiry_appointments.state in('sent', 'call_patient')
	and inquiry_appointments.inquiry_borg_id not in (select inquiry_borg_id from inquiry_appointments where (appointment_type = '3' and state like 'completed'))
	and voucher_client_name not in ('VIP GELB', 'VIP ROT', 'BetterDoc Staff') -- ARAG Casemanagement (CM22), AXA Haftpflicht, DAK -LA3F, keine Testanfragen, VIPs, BetterDoc Staff, Promo, Endung -BDTC
	and (first_voucher_code not like '%-MSSNACHRSO' and first_voucher_code not like '%-BDTC' and first_voucher_code not like 'NUBU-7777')
	)
SELECT
	distinct masked_id,
	voucher_client_name,
	first_voucher_code,
	app_state,
	app_date,
	'Tel. Nachbefragung' as parameter,
	current_date as card_created,
	case_state as korb,
	'de' as preferred_language
FROM
	foo 
WHERE
	(app_state like 'call_patient' and age > 35 and app_date BETWEEN '2022-12-05' and '2022-12-11') --Sendout heutige KW - 6 Wochen
	or (app_state like 'sent' and age > 28 and app_date between '2022-12-12' and '2022-12-18') --FB Versand vor mindestens 5 Wochen 
ORDER BY
	app_state, voucher_client_name