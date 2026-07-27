--insert into md_campaigns.cam_select 
-- Achtung, nur aussführen, wenn wirklich Karten erstellt werden!
with cube_selection_outcome_campaigns as (
	select * from pii_analytics.cube_selection_outcome_campaigns
),
cube_services as (
	select * from analytics.cube_services
),
md_campaigns as (
--This CTE is a temporary solution, as this information is currently not available in DBT
select
	cam_select.case_id,
	cam_batch.batch_selection_date,
	cam_cam.cam_name,
	cube_services.dna_patient_id
from 
	md_campaigns.cam_select
	left join md_campaigns.cam_batch on cam_select.batch_id = cam_batch.batch_id
	left join md_campaigns.cam_cam on cam_batch.cam_id = cam_cam.cam_id
	left join cube_services on cam_select.case_id = cube_services.case_id
where 
	cam_cam.cam_category in ('Anrufaktion', 'Regelprozess')
	and cam_select.case_id is not null
),
final_selection as (
select
	--Each patient should only be selected once.
	--Because it could be possible that one patient has 2 cases
	distinct on (dna_patient_id)
	* 
from 
	cube_selection_outcome_campaigns
where
	---------------------------------
	(1 = 1)
	--hard filters
	and product_v2 ilike '%pmn%'
	and result_date between (current_date - interval '6 month') and (current_date - interval '6 weeks')
	--exclusion of specific voucher/ payers
	and hard_exclusion is false
	--specific completion reason need to be excluded
	and questioning_inappropriate is false
	and (status_survey_latest_planned_at is null or status_survey_latest_planned_at < (current_date - interval '1 weeks'))
	and (prem_latest_planned_at is null or prem_latest_planned_at < (current_date - interval '2 weeks'))
	and (prem_latest_sent_at is null or prem_latest_sent_at < (current_date - interval '2 weeks'))
	and prem_return_bool is false
	--case_id was not already selected for a telephonic review (meaning 'Anrtufaktion' or 'Regelprozess')
	and case_id not in (select case_id from md_campaigns)
	--patient_id was not already selected for a telephonic review a month ago (meaning 'Anrtufaktion' or 'Regelprozess')
	and (dna_patient_id is null or dna_patient_id not in (select dna_patient_id from md_campaigns where dna_patient_id is not null and batch_selection_date >= current_date - interval '1 month'))
	---------------------------------
	--variable filters
	---------------------------------
	and payer_name ilike '%AXA Krankenversicherung AG%'
	and bd_nps_value is null
	and medic_nps_value is null
),
final_dialer as ( 
select
	case_id,
	product_v2,
	result_date,
	payer_name,
	payer_group,
	pii_primary_phone as primary_phone_number,
	'parc.betterdoc.org/stack/case?case_id=' || case_id
		as parc_link
from 
	final_selection
),
final as (
select
	(select max(select_id) from md_campaigns.cam_select) + row_number() over() as select_id,
	(select max(batch_id) from md_campaigns.cam_select) + 1 as batch_id, ----------Überprüfen, ob die Lieferung mit MSS verknüpft werden kann
	case_id, 
	payer_name as client_name,
	voucher_code as first_voucher_code,
	dna_patient_id as patient_id,
	0 as control_flag, 
	1 as count_cases_patient, 
	null::numeric as reached_patient_flag,
	null::date as reached_date, 
	null::text as refusal_reason,
	null::numeric as opt_out, 
	null::text as new_case_id,
	null::text as app_state,
	null::text as korb,
	preferred_language
from 
	final_selection 
)
select * from final







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
	'Richard Bergner Holding GmbH & Co. KG' as cam_name, -----------------------------------------ACHTUNG hier Namen anpassen!!!
	'extra call campaign for our follow ups - Richard Bergner Holding GmbH & Co. KG' as cam_description, ------------ACHTUNG hier Namen anpassen!!!
	'bd_patients' as cam_audience,
	'information' as cam_type,
	'2022-01-01' as valid_from,    ------Datum?
	'2999-12-31' as valid_to,
	'Anrufaktion' as cam_category
	
	

	
----------------Unterstützung zur Suche der passenden Kamapgne
select *
from md_campaigns.cam_cam
order by cam_name