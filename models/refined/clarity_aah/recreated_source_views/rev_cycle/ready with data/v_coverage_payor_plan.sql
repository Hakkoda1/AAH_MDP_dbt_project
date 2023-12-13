/*copyright (c) 2011 epic systems corporation
********************************************************************************
title:   v_coverage_payor_plan
purpose: returns the payor and plan associated with coverages for particular
         effective dates
author:  stuart isaac
properties: 
revision history: 
*soi 11/2010 dlg#191453 - created
*soi 04/2011 189063 - remove efn function calls
********************************************************************************
*/


with 

coverage as (
	select * from {{ ref('coverage_base') }}
),

cvg as (
	select * from {{ ref('coverage_base') }}
),

plan_grp_ben_plan as (
	select * from {{ ref('plan_grp_ben_plan_base') }}
),

plangrpmc as (
	select * from {{ ref('plan_grp_ben_plan_base') }}
),

plan_grp_ot as (
	select * from {{ ref('plan_grp_ot_base') }}
),

ppgot as (
	select * from {{ ref('plan_grp_ot_base') }}
),

ppgotnexteff as (
	select * from {{ ref('plan_grp_ot_base') }}
),

ppgotintervene as (
	select * from {{ ref('plan_grp_ot_base') }}
),

clarity_epm as (
	select * from {{ ref('clarity_epm_base') }}
),

payor as (
	select * from {{ ref('clarity_epm_base') }}
),

clarity_epp as (
	select * from {{ ref('clarity_epp_base') }}
),

benplan as (
	select * from {{ ref('clarity_epp_base') }}
),

zc_fin_class as (
	select * from {{ ref('lkp_clr_fin_class_base') }}
),

zcfin as (
	select * from {{ ref('lkp_clr_fin_class_base') }}
),

clarity_carrier as (
	select * from {{ ref('clarity_carrier_base') }}
),

carrier as (
	select * from {{ ref('clarity_carrier_base') }}
),

clarity_lob as (
	select * from {{ ref('clarity_lob_base') }}
),

lob as (
	select * from {{ ref('clarity_lob_base') }}
)
select cvg.coverage_id
, coalesce(plangrpmc.ben_plan_eff_date, ppgot.contact_date, to_date('1900-01-01', 'YYYY-MM-DD')) as eff_date

, coalesce(plangrpmc.ben_plan_term_dt
           , dateadd(day,-1,ppgotnexteff.contact_date)
           , to_date('2173-09-27', 'YYYY-MM-DD')) as term_date
, coalesce(plangrpmc.ben_plan_payor_id, ppgot.payor_id, cvg.payor_id) as payor_id
, case 
    when coalesce(plangrpmc.ben_plan_payor_id, ppgot.payor_id, cvg.payor_id) is null 
    then '*unspecified payor'
    when payor.payor_name is null 
    then '*unnamed payor ' || payor.payor_id::varchar
    else payor.payor_name
end as payor_name
, payor.financial_class as fin_class_c
, case
    when payor.financial_class is null then '*unspecified financial class'
    when zcfin.fin_class_c is null then '*unknown financial class ' || payor.financial_class::varchar
    when zcfin.name is null then '*unnamed financial class ' || payor.financial_class::varchar
    else zcfin.name
end as fin_class_name
, coalesce(plangrpmc.ben_plan_id, ppgot.benefit_plan_id, cvg.plan_id) as benefit_plan_id
, case 
    when coalesce(plangrpmc.ben_plan_id, ppgot.benefit_plan_id, cvg.plan_id) is null 
    then '*unspecified plan'
    when benplan.benefit_plan_id is null 
    then '*unknown plan ' || coalesce(plangrpmc.ben_plan_id, ppgot.benefit_plan_id, cvg.plan_id)::varchar
    when benplan.benefit_plan_name is null 
    then '*unnamed plan ' || benplan.benefit_plan_id::varchar
    else benplan.benefit_plan_name
end as benefit_plan_name

, plangrpmc.ben_plan_car_id as carrier_id

, case
    when cvg.coverage_type_c = 2 
    then case 
        when plangrpmc.ben_plan_car_id is null 
        then '*unspecified carrier'
        when carrier.carrier_id is null 
        then '*unknown carrier ' || plangrpmc.ben_plan_car_id::varchar
        when carrier.carrier_name is null 
        then '*unnamed carrier ' || plangrpmc.ben_plan_car_id::varchar
        else carrier.carrier_name
    end
    else null
end as carrier_name

, plangrpmc.plan_lob_id as lob_id

, case
    when cvg.coverage_type_c = 2 
    then case
        when plangrpmc.plan_lob_id is null 
        then '*unspecified line of business'
        when lob.lob_id is null 
        then 'unknown line of business ' || plangrpmc.plan_lob_id::varchar
        when lob.lob_name is null 
        then '*unnamed line of business ' || plangrpmc.plan_lob_id::varchar
        else lob.lob_name
    end
    else null
end as lob_name

, cvg.cm_phy_owner_id
, cvg.cm_log_owner_id
from coverage cvg
left join plan_grp_ben_plan plangrpmc on --just mc coverages linked to ppg
    cvg.plan_grp_id = plangrpmc.plan_grp_id and cvg.coverage_type_c = 2 
left join plan_grp_ot ppgot on        --just indemnity coverages linked to ppg
    cvg.plan_grp_id = ppgot.plan_grp_id and cvg.coverage_type_c = 1
left join plan_grp_ot ppgotnexteff on 
    ppgot.plan_grp_id = ppgotnexteff.plan_grp_id and ppgotnexteff.contact_date > ppgot.contact_date
left join plan_grp_ot ppgotintervene on
    ppgot.plan_grp_id = ppgotintervene.plan_grp_id and ppgotintervene.contact_date > ppgot.contact_date
    and ppgotintervene.contact_date < ppgotnexteff.contact_date
left join clarity_epm payor on 
    coalesce(plangrpmc.ben_plan_payor_id, ppgot.payor_id, cvg.payor_id) = payor.payor_id
left join clarity_epp benplan on 
    coalesce(plangrpmc.ben_plan_id, ppgot.benefit_plan_id, cvg.plan_id) = benplan.benefit_plan_id
left join zc_fin_class zcfin on payor.financial_class = zcfin.fin_class_c
left join clarity_carrier carrier on plangrpmc.ben_plan_car_id = carrier.carrier_id
left join clarity_lob lob on plangrpmc.plan_lob_id = lob.lob_id
where ppgotintervene.plan_grp_id is null --restrict to earliest next effective date for ppgotnexteff