/*copyright (c) 2016 epic systems corporation
********************************************************************************
title:   v_arpb_chg_review_user
purpose: this view shows the activities performed by users on tars that are in a charge review workqueue.
author:  sid palaniswami
properties: none
revision history: 
*sp 12/22/09 dlg#169700 - created
*dcb 11/16 dlg#452691 - use v_coverage_payor_plan logic for deprecated pre_ar_chg columns
********************************************************************************
*/


with 

pre_ar_chg_hx as (
	select * from {{ ref('pre_ar_chg_hx_base') }}
),

pre_ar_chg_hx as (
	select * from {{ ref('pre_ar_chg_hx_base') }}
),

pre_ar_chg as (
	select * from {{ ref('pre_ar_chg_base') }}
),

pre_ar_chg as (
	select * from {{ ref('pre_ar_chg_base') }}
),

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

clarity_epm as (
	select * from {{ ref('clarity_epm_base') }}
),

payor as (
	select * from {{ ref('clarity_epm_base') }}
),

clarity_epm as (
	select * from {{ ref('clarity_epm_base') }}
),

clarity_dep as (
	select * from {{ ref('clarity_dep_base') }}
),

clarity_dep as (
	select * from {{ ref('clarity_dep_base') }}
),

clarity_sa as (
	select * from {{ ref('clarity_sa_base') }}
),

clarity_sa as (
	select * from {{ ref('clarity_sa_base') }}
),

clarity_loc as (
	select * from {{ ref('clarity_loc_base') }}
),

clarity_loc as (
	select * from {{ ref('clarity_loc_base') }}
),

clarity_pos as (
	select * from {{ ref('clarity_pos_base') }}
),

clarity_pos as (
	select * from {{ ref('clarity_pos_base') }}
),

bill_area as (
	select * from {{ ref('bill_area_base') }}
),

bill_area as (
	select * from {{ ref('bill_area_base') }}
),

clarity_ser as (
	select * from {{ ref('clarity_ser_base') }}
),

serviceprovider as (
	select * from {{ ref('clarity_ser_base') }}
),

billingprovider as (
	select * from {{ ref('clarity_ser_base') }}
),

clarity_emp as (
	select * from {{ ref('clarity_emp_base') }}
),

clarity_emp as (
	select * from {{ ref('clarity_emp_base') }}
),

zc_chrg_source_tar as (
	select * from {{ ref('lkp_clr_chrg_source_tar_base') }}
),

zc_chrg_source_tar as (
	select * from {{ ref('lkp_clr_chrg_source_tar_base') }}
),

clarity_epp as (
	select * from {{ ref('clarity_epp_base') }}
),

clarity_epp as (
	select * from {{ ref('clarity_epp_base') }}
),

clarity_fc as (
	select * from {{ ref('clarity_fc_base') }}
),

clarity_fc as (
	select * from {{ ref('clarity_fc_base') }}
),

wqf_cr_wq as (
	select * from {{ ref('wqf_cr_wq_base') }}
),

wqf_cr_wq as (
	select * from {{ ref('wqf_cr_wq_base') }}
),

zc_chg_review_act as (
	select * from {{ ref('lkp_clr_chg_review_act_base') }}
),

zc_chg_review_act as (
	select * from {{ ref('lkp_clr_chg_review_act_base') }}
),

zc_delete_reason as (
	select * from {{ ref('lkp_clr_delete_reason_base') }}
),

zc_delete_reason as (
	select * from {{ ref('lkp_clr_delete_reason_base') }}
)
select pre_ar_chg_hx.tar_id,
  activity_c,
  activity_date ,
  pre_ar_chg_hx.workqueue_id,
  pre_ar_chg_hx.user_id,
  deletion_reason_c,
  user_comment,
  amount,
  charge_line_count,
  case
    when pre_ar_chg_hx.workqueue_id is null
    then '*unspecified wq '
    when wqf_cr_wq.workqueue_id is null
    then '*unknown wq'
    else wqf_cr_wq.workqueue_name
  end as wq_name,

  case
    when pre_ar_chg_hx.workqueue_id is null then '*unspecified wq'
    when wqf_cr_wq.workqueue_id is null then concat('*unknown wq ', cast(pre_ar_chg_hx.workqueue_id as varchar))
    when wqf_cr_wq.workqueue_name is null then concat('*unnamed wq ', cast(wqf_cr_wq.workqueue_id as varchar))
    else concat(wqf_cr_wq.workqueue_name, ' ', cast(wqf_cr_wq.workqueue_id as varchar))
  end as wq_nm_wid,

  {# case
    when pre_ar_chg_hx.workqueue_id is null
    then '*unspecified wq '
    when wqf_cr_wq.workqueue_id is null
    then '*unknown wq '
      + cast(pre_ar_chg_hx.workqueue_id as varchar)
      + ''
    when wqf_cr_wq.workqueue_name is null
    then '*unnamed wq '
      + cast(wqf_cr_wq.workqueue_id as varchar)
      + ''
    else wqf_cr_wq.workqueue_name
      + ' '
      + cast(wqf_cr_wq.workqueue_id as varchar)
      + ''
  end as wq_nm_wid, #}


  case
    when pre_ar_chg_hx.user_id is null
    then '*unspecified user'
    when clarity_emp.user_id is null
    then '*unknown user'
    else clarity_emp.name
  end as user_name,

case
  when pre_ar_chg_hx.user_id is null
  then '*unspecified user'
  when clarity_emp.user_id is null
  then concat('*unknown user ', cast(pre_ar_chg_hx.user_id as varchar))
  when clarity_emp.name is null
  then concat('*unnamed user ', cast(pre_ar_chg_hx.user_id as varchar))
  else concat(clarity_emp.name, ' ', cast(pre_ar_chg_hx.user_id as varchar))
end as user_nm_wid,

  sess_bill_area_id as bill_area_id,
  case
    when sess_bill_area_id is null
    then '*unspecified bill area'
    when bill_area.bill_area_id is null
    then '*unknown bill area'
    else bill_area.record_name
  end as bill_area_name,

case
  when sess_bill_area_id is null
  then '*unspecified bill area'
  when bill_area.bill_area_id is null
  then concat('*unknown bill area ', cast(sess_bill_area_id as varchar))
  when bill_area.record_name is null
  then concat('*unnamed bill area ', cast(sess_bill_area_id as varchar))
  else concat(bill_area.record_name, ' ', cast(sess_bill_area_id as varchar))
end as bill_area_nm_wid,

  sess_dept_id as dept_id,
  case
    when sess_dept_id is null
    then '*unspecified department'
    when clarity_dep.department_id is null
    then '*unknown department'
    else department_name
  end as dept_name,

case
  when sess_dept_id is null
  then '*unspecified department'
  when clarity_dep.department_id is null
  then concat('*unknown department ', cast(sess_dept_id as varchar))
  when department_name is null
  then concat('*unnamed department ', cast(sess_dept_id as varchar))
  else concat(department_name, ' ', cast(sess_dept_id as varchar))
end as dept_nm_wid,

  sess_pos_id as pos_id,
  case
    when sess_pos_id is null
    then '*unspecified pos'
    when clarity_pos.pos_id is null
    then '*unknown pos'
    else clarity_pos.pos_name
  end as pos_name,

case
  when sess_pos_id is null
  then '*unspecified pos'
  when clarity_pos.pos_id is null
  then concat('*unknown pos ', cast(sess_pos_id as varchar))
  when clarity_pos.pos_name is null
  then concat('*unnamed pos ', cast(sess_pos_id as varchar))
  else concat(clarity_pos.pos_name, ' ', cast(sess_pos_id as varchar))
end as pos_nm_wid,

  tar.serv_area_id,
  case
    when tar.serv_area_id is null
    then '*unspecified sa'
    when clarity_sa.serv_area_id is null
    then '*unknown sa'
    else serv_area_name
  end as serv_area_name,

case
  when tar.serv_area_id is null
  then '*unspecified sa'
  when clarity_sa.serv_area_id is null
  then concat('*unknown sa ', cast(tar.serv_area_id as varchar))
  when clarity_sa.serv_area_name is null
  then concat('*unnamed sa ', cast(tar.serv_area_id as varchar))
  else concat(clarity_sa.serv_area_name, ' ', cast(tar.serv_area_id as varchar))
end as serv_area_nm_wid,

  tar.loc_id,
  case
    when tar.loc_id is null
    then '*unspecified location'
    when clarity_loc.loc_id is null
    then '*unknown location'
    else loc_name
  end as loc_name,

case
  when tar.loc_id is null
  then '*unspecified location'
  when clarity_loc.loc_id is null
  then concat('*unknown location ', cast(tar.loc_id as varchar))
  when loc_name is null
  then concat('*unnamed location ', cast(tar.loc_id as varchar))
  else concat(loc_name, ' ', cast(tar.loc_id as varchar))
end as loc_name_wid,

  perf_prov_id as serv_prov_id,
  case
    when perf_prov_id is null
    then '*unspecified provider'
    when serviceprovider.prov_id is null
    then '*unknown provider'
    else serviceprovider.prov_name
  end as service_prov_name,

case
  when perf_prov_id is null
  then '*unspecified provider'
  when serviceprovider.prov_id is null
  then concat('*unknown provider ', cast(perf_prov_id as varchar))
  when serviceprovider.prov_name is null
  then concat('*unnamed provider ', cast(perf_prov_id as varchar))
  else concat(serviceprovider.prov_name, ' ', cast(perf_prov_id as varchar))
end as service_prov_nm_wid,

  bill_prov_id,
  case
    when bill_prov_id is null
    then '*unspecified provider'
    when billingprovider.prov_id is null
    then '*unknown provider'
    else billingprovider.prov_name
  end as billing_prov_name,

case
  when bill_prov_id is null
  then '*unspecified provider'
  when billingprovider.prov_id is null
  then concat('*unknown provider ', cast(bill_prov_id as varchar))
  when billingprovider.prov_name is null
  then concat('*unnamed provider ', cast(bill_prov_id as varchar))
  else concat(billingprovider.prov_name, ' ', cast(bill_prov_id as varchar))
end as billing_prov_nm_wid,

  tar.charge_source_c as source_c,
  case
    when tar.charge_source_c is null
    then '*unspecified source'
    when zc_chrg_source_tar.charge_source_c is null
    then '*unknown source'
    else zc_chrg_source_tar.name
  end as source_name,

case
  when tar.charge_source_c is null
  then '*unspecified source'
  when zc_chrg_source_tar.charge_source_c is null
  then concat('*unknown source ', cast(tar.charge_source_c as varchar))
  when zc_chrg_source_tar.name is null
  then concat('*unnamed source ', cast(tar.charge_source_c as varchar))
  else concat(zc_chrg_source_tar.name, ' ', cast(tar.charge_source_c as varchar))
end as source_nm_wid,


  tar.payor_id,
  case
    when tar.payor_id is null
    then '*unspecified payor'
    when clarity_epm.payor_id is null
    then '*unknown payor'
    else clarity_epm.payor_name
  end as payor_name,

case
  when tar.payor_id is null
  then '*unspecified payor'
  when clarity_epm.payor_id is null
  then concat('*unknown payor ', cast(tar.payor_id as varchar))
  when clarity_epm.payor_name is null
  then concat('*unnamed payor ', cast(tar.payor_id as varchar))
  else concat(clarity_epm.payor_name, ' ', cast(tar.payor_id as varchar))
end as payor_nm_wid,

  tar.benefit_plan_id,
  case
    when tar.benefit_plan_id is null
    then '*unspecified plan'
    when clarity_epp.benefit_plan_id is null
    then '*unknown plan'
    else clarity_epp.benefit_plan_name
  end as plan_name,

case
  when tar.payor_id is null
  then '*unspecified payor'
  when clarity_epm.payor_id is null
  then concat('*unknown payor ', cast(tar.payor_id as varchar))
  when clarity_epm.payor_name is null
  then concat('*unnamed payor ', cast(tar.payor_id as varchar))
  else concat(clarity_epm.payor_name, ' ', cast(tar.payor_id as varchar))
end as payor_nm_wid,

  tar.fin_class_c financial_class_c,
  case
    when tar.fin_class_c is null
    then '*unspecified financial class'
    when financial_class_name is null
    then '*unknown financial class'
    else financial_class_name
  end as financial_class,
  case
    when activity_c is null
    then '*unspecified activity '
    when chg_review_act_c is null
    then '*unknown activity'
    else zc_chg_review_act.name
  end as activity_name,
  case
    when deletion_reason_c is null
    then '*unspecified reason '
    when delete_reason_c is null
    then '*unknown reason'
    else zc_delete_reason.name
  end as delete_reason_nm
from pre_ar_chg_hx pre_ar_chg_hx
join
  (select sum(amount) amount,
    count(charge_line) charge_line_count,
    tar_id
  from pre_ar_chg
  group by tar_id
  ) tarsum
on tarsum.tar_id = pre_ar_chg_hx.tar_id
join
  (select chg.sess_bill_area_id,
    chg.sess_dept_id,
    chg.sess_pos_id,
    chg.serv_area_id,
    chg.sess_perf_prov_id perf_prov_id,
    chg.sess_bill_prov_id bill_prov_id,
    chg.charge_source_c,
    chg.loc_id,
    cvg.payor_id,
    cvg.benefit_plan_id,
    cvg.fin_class_c,
    chg.tar_id
  from pre_ar_chg chg
  left outer join 
  (select
  cvg.coverage_id,
  coalesce(plangrpmc.ben_plan_eff_date, ppgot.contact_date, '1900-01-01'::date) as eff_date,
  coalesce(plangrpmc.ben_plan_term_dt, dateadd('day', -1, ppgotnexteff.contact_date), '2173-09-27'::date) as term_date,
  coalesce(plangrpmc.ben_plan_payor_id, ppgot.payor_id, cvg.payor_id) as payor_id,
  coalesce(plangrpmc.ben_plan_id, ppgot.benefit_plan_id, cvg.plan_id) as benefit_plan_id,
  payor.financial_class as fin_class_c

{# select cvg.coverage_id,
    coalesce(plangrpmc.ben_plan_eff_date, ppgot.contact_date, convert(datetime, '1900-01-01', 120)) as eff_date,
	coalesce(plangrpmc.ben_plan_term_dt, dateadd(day,-1,ppgotnexteff.contact_date), convert(datetime, '2173-09-27', 120)) as term_date,
	coalesce(plangrpmc.ben_plan_payor_id, ppgot.payor_id, cvg.payor_id) as payor_id,
    coalesce(plangrpmc.ben_plan_id, ppgot.benefit_plan_id, cvg.plan_id) as benefit_plan_id,
    payor.financial_class as fin_class_c #}

	from coverage cvg
  left outer join plan_grp_ben_plan plangrpmc on --just mc coverages linked to ppg
    cvg.plan_grp_id = plangrpmc.plan_grp_id and cvg.coverage_type_c = 2 
  left outer join plan_grp_ot ppgot on        --just indemnity coverages linked to ppg
    cvg.plan_grp_id = ppgot.plan_grp_id and cvg.coverage_type_c = 1
  left outer join plan_grp_ot ppgotnexteff on 
    ppgot.plan_grp_id = ppgotnexteff.plan_grp_id and ppgotnexteff.contact_date > ppgot.contact_date
  left outer join clarity_epm payor on 
    coalesce(plangrpmc.ben_plan_payor_id, ppgot.payor_id, cvg.payor_id) = payor.payor_id) cvg on
	chg.coverage_id = cvg.coverage_id and chg.service_date between cvg.eff_date and cvg.term_date
  where chg.charge_line=1
  ) tar
on tar.tar_id =pre_ar_chg_hx.tar_id
left outer join clarity_dep clarity_dep
on sess_dept_id=clarity_dep.department_id
left outer join clarity_sa clarity_sa
on tar.serv_area_id =clarity_sa.serv_area_id
left outer join clarity_loc clarity_loc
on tar.loc_id =clarity_loc.loc_id
left outer join clarity_pos clarity_pos
on sess_pos_id=clarity_pos.pos_id
left outer join bill_area bill_area
on sess_bill_area_id=bill_area.bill_area_id
left outer join clarity_ser serviceprovider
on serviceprovider.prov_id=perf_prov_id
left outer join clarity_ser billingprovider
on billingprovider.prov_id=bill_prov_id
left outer join clarity_emp clarity_emp
on pre_ar_chg_hx.user_id=clarity_emp.user_id
left outer join zc_chrg_source_tar zc_chrg_source_tar
on tar.charge_source_c =zc_chrg_source_tar.charge_source_c
left outer join clarity_epm clarity_epm
on tar.payor_id=clarity_epm.payor_id
left outer join clarity_epp clarity_epp
on tar.benefit_plan_id=clarity_epp.benefit_plan_id
left outer join clarity_fc clarity_fc
on tar.fin_class_c=clarity_fc.financial_class
left outer join wqf_cr_wq wqf_cr_wq
on wqf_cr_wq.workqueue_id=pre_ar_chg_hx.workqueue_id
left outer join zc_chg_review_act zc_chg_review_act
on activity_c=chg_review_act_c
left outer join zc_delete_reason zc_delete_reason
on delete_reason_c           =deletion_reason_c
where pre_ar_chg_hx.user_id is not null