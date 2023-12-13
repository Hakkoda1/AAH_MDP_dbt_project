/*copyright (c) 2012 epic systems corporation
********************************************************************************
title:   v_arpb_rvu_data
purpose: summarize rvu data from tdl in an end-user friendly format
author:  tim keogh
properties: 
revision history: 
*tpk 07/12 dlg#244095 - created
********************************************************************************
*/


with 

clarity_tdl_tran as (
	select * from {{ ref('clarity_tdl_tran_base') }}
),

tdl as (
	select * from {{ ref('clarity_tdl_tran_base') }}
),

arpb_transactions as (
	select * from {{ ref('arpb_transactions_base') }}
),

arpb as (
	select * from {{ ref('arpb_transactions_base') }}
),

clarity_eap as (
	select * from {{ ref('clarity_eap_base') }}
),

clarity_eap as (
	select * from {{ ref('clarity_eap_base') }}
),

edp_proc_cat_info as (
	select * from {{ ref('edp_proc_cat_info_base') }}
),

edp_proc_cat_info as (
	select * from {{ ref('edp_proc_cat_info_base') }}
),

clarity_ser as (
	select * from {{ ref('clarity_ser_base') }}
),

billingprovider as (
	select * from {{ ref('clarity_ser_base') }}
),

serviceprovider as (
	select * from {{ ref('clarity_ser_base') }}
),

clarity_sa as (
	select * from {{ ref('clarity_sa_base') }}
),

clarity_sa as (
	select * from {{ ref('clarity_sa_base') }}
),

clarity_dep as (
	select * from {{ ref('clarity_dep_base') }}
),

clarity_dep as (
	select * from {{ ref('clarity_dep_base') }}
),

clarity_loc as (
	select * from {{ ref('clarity_loc_base') }}
),

clarity_loc as (
	select * from {{ ref('clarity_loc_base') }}
),

bill_area as (
	select * from {{ ref('bill_area_base') }}
),

bill_area as (
	select * from {{ ref('bill_area_base') }}
),

fin_div as (
	select * from {{ ref('fin_div_base') }}
),

fin_div as (
	select * from {{ ref('fin_div_base') }}
),

fin_subdiv as (
	select * from {{ ref('fin_subdiv_base') }}
),

fin_subdiv as (
	select * from {{ ref('fin_subdiv_base') }}
),

date_dimension as (
	select * from {{ ref('date_dimension_base') }}
),

sd as (
	select * from {{ ref('date_dimension_base') }}
),

pd as (
	select * from {{ ref('date_dimension_base') }}
),

opd as (
	select * from {{ ref('date_dimension_base') }}
),

vd as (
	select * from {{ ref('date_dimension_base') }}
)
select
  tdl.tdl_id, 
  tdl.tx_id as transaction_id,
  tdl.account_id as account_id,
  tdl.tx_num as transaction_in_account,
  case when tdl.detail_type=1 then 'charge post'
    when tdl.detail_type=10 then 'charge void'
    end as activity,
  tdl.post_date as activity_date, 
  pd.year as activity_year,
  pd.quarter_str as activity_quarter,
  pd.month_name as activity_month, 
  pd.day_of_week as activity_day_of_week,
  tdl.orig_service_date as service_date,
  sd.year as service_year,
  sd.quarter_str as service_quarter,
  sd.month_name as service_month,
  sd.day_of_week as service_day_of_week,
  tdl.orig_post_date as charge_post_date,
  opd.year as charge_post_year,
  opd.quarter_str as charge_post_quarter,
  opd.month_name as charge_post_month,
  opd.day_of_week as charge_post_day_of_week,
  arpb.void_date as void_date,
  vd.year as void_year,
  vd.quarter_str as void_quarter,
  vd.month_name as void_month,  
  vd.day_of_week as void_day_of_week,  
  tdl.serv_area_id,
  case
    when tdl.serv_area_id is null
    then '*unspecified sa'
    when clarity_sa.serv_area_id is null
    then '*unknown sa'
    else serv_area_name
  end as serv_area_name,
  tdl.loc_id,
  case
    when tdl.loc_id is null
    then '*unspecified location'
    when clarity_loc.loc_id is null
    then '*unknown location'
    else loc_name
  end as loc_name,
  tdl.dept_id department_id,
  case
    when tdl.dept_id is null
    then '*unspecified department'
    when clarity_dep.department_id is null
    then '*unknown department'
    else department_name
  end as department_name,
   tdl.fin_div_id,
    case
    when tdl.fin_div_id is null
    then '*unspecified financial division'
    when fin_div.fin_div_id is null
    then '*unknown financial division'
    else fin_div.fin_div_nm
  end as fin_div_name,
    tdl.fin_subdiv_id,
    case
    when tdl.fin_subdiv_id is null
    then '*unspecified financial subdivision'
    when fin_subdiv.fin_subdiv_id is null
    then '*unknown financial subdivision'
    else fin_subdiv.fin_subdiv_nm
  end as fin_subdiv_name,
  tdl.bill_area_id,
    case
    when tdl.bill_area_id is null
    then '*unspecified bill area'
    when bill_area.bill_area_id is null
    then '*unknown bill area'
    else bill_area.record_name
  end as bill_area_name,
  tdl.performing_prov_id service_prov_id,
case
    when tdl.performing_prov_id is null
    then '*unspecified provider'
    when serviceprovider.prov_id is null
    then '*unknown provider ' || tdl.performing_prov_id::varchar
    else serviceprovider.prov_name || ' ' || serviceprovider.prov_id::varchar
end as service_prov_nm_wid,

tdl.billing_provider_id as billing_prov_id,

case
    when tdl.billing_provider_id is null
    then '*unspecified provider'
    when billingprovider.prov_id is null
    then '*unknown provider ' || tdl.billing_provider_id::varchar
    else billingprovider.prov_name || ' ' || billingprovider.prov_id::varchar
  end as billing_prov_nm_wid,
  tdl.proc_id,
  edp_proc_cat_info.proc_cat_name proc_cat_name,
  tdl.cpt_code,
  clarity_eap.proc_code,
case
    when clarity_eap.proc_name is null 
    then '*unknown procedure'
    when clarity_eap.proc_name is not null 
    then clarity_eap.proc_name
    else null
  end as proc_name,
  tdl.procedure_quantity proc_quantity,
  tdl.amount,
  tdl.rvu_work*tdl.procedure_quantity rvu_work,
  tdl.rvu_malpractice*tdl.procedure_quantity rvu_malpractice,
  tdl.rvu_overhead*tdl.procedure_quantity rvu_overhead,
  case
    when detail_type=10 then -1*tdl.rvu_proc_units
    when detail_type=1 then tdl.rvu_proc_units
    else null 
  end as rvu_total, 
  tdl.relative_value_unit as rvu_fsc_total
from clarity_tdl_tran tdl
left outer join arpb_transactions arpb on tdl.tx_id=arpb.tx_id
left outer join clarity_eap on tdl.proc_id=clarity_eap.proc_id
left outer join edp_proc_cat_info on clarity_eap.proc_cat_id=edp_proc_cat_info.proc_cat_id
left outer join clarity_ser billingprovider on tdl.billing_provider_id=billingprovider.prov_id
left outer join clarity_ser serviceprovider on tdl.performing_prov_id=serviceprovider.prov_id
left outer join clarity_sa on tdl.serv_area_id=clarity_sa.serv_area_id
left outer join clarity_dep on clarity_dep.department_id=tdl.dept_id
left outer join clarity_loc on tdl.loc_id=clarity_loc.loc_id
left outer join bill_area on bill_area.bill_area_id=tdl.bill_area_id
left outer join fin_div on fin_div.fin_div_id=tdl.fin_div_id
left outer join fin_subdiv on fin_subdiv.fin_subdiv_id=tdl.fin_subdiv_id
left outer join date_dimension sd on tdl.orig_service_date=sd.calendar_dt
left outer join date_dimension pd on tdl.post_date=pd.calendar_dt
left outer join date_dimension opd on tdl.orig_post_date=opd.calendar_dt
left outer join date_dimension vd on arpb.void_date=vd.calendar_dt
where tdl.detail_type in (1,10)