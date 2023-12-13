/*copyright (c) 2011-2016 epic systems corporation
********************************************************************************
title: v_arhb_bkt_aging_detail
purpose: creates a view that consolidates data elements used in aging based reports, includes names of dimensions,
         and calculates age for a number of different dates relevant to the hospital account and bucket. 
author:  jon mcgee
properties: p_residual_selfpay_name - this property is used to determine the name of residual self-pay 
            balances for the calculated financial class, payor, and plan columns. any balances in a 
            self-pay bucket that also have a primary coverage on the account will be labeled with this 
            value.
            p_uninsured_selfpay_name - this property is used to determine the name of uninsured 
            self-pay balances for the calculated financial class, payor, and plan columns. any balances
            that have no primary coverage on the account will be labeled with this value.
revision history: 
*jpm 03/31/09 dlg#152225 - created
*jpm 06/30/11 dlg#208437 - add har_amount column, support acct based bad debt in calc columns
*jpm 10/25/11 dlg#217909 - update calc_* column logic to check for ins bkts before self-pay
*jrm 12/27/11 dlg#220431 - add bucket and har product type, calculated discharge date, and account base class columns
*pnr 01/23/13 dlg#258625 - add handling for home health buckets
*jaa 04/01/14 dlg#290953 - add contractual columns
*jaa 12/05/14 dlg#341916 - bkt/clm billed cols
*rsd 08/20/15 dlg#382904 - account for self-pay coverages when computing self-pay bucket payor/plan names
*etc 05/31/16 dlg#423084 - handle alphanumeric financial classes
********************************************************************************
*/


with 

age as (
	select * from {{ ref('hsp_bkt_aging_hx_base') }}
),

bkt as (
	select * from {{ ref('hsp_bucket_base') }}
),

bkttype as (
	select * from {{ ref('lkp_clr_bkt_type_ha_base') }}
),

bktsts as (
	select * from {{ ref('lkp_clr_bkt_sts_ha_base') }}
),

balsts as (
	select * from {{ ref('lkp_clr_bkt_balance_status_base') }}
),

sa as (
	select * from {{ ref('clarity_sa_base') }}
),

acctclass as (
	select * from {{ ref('lkp_clr_acct_class_ha_base') }}
),

sts as (
	select * from {{ ref('lkp_clr_acct_billsts_ha_base') }}
),

pfinclass as (
	select * from {{ ref('lkp_clr_financial_class_base') }}
),

bfinclass as (
	select * from {{ ref('lkp_clr_financial_class_base') }}
),

bpayor as (
	select * from {{ ref('clarity_epm_base') }}
),

ppayor as (
	select * from {{ ref('clarity_epm_base') }}
),

pplan as (
	select * from {{ ref('clarity_epp_base') }}
),

bplan as (
	select * from {{ ref('clarity_epp_base') }}
),

pplan2 as (
	select * from {{ ref('clarity_epp_2_base') }}
),

bplan2 as (
	select * from {{ ref('clarity_epp_2_base') }}
),

pplanpt as (
	select * from {{ ref('lkp_clr_prod_type_base') }}
),

bplanpt as (
	select * from {{ ref('lkp_clr_prod_type_base') }}
),

har as (
	select * from {{ ref('hsp_account_base') }}
),

agency as (
	select * from {{ ref('cl_col_agncy_base') }}
),

clform as (
	select * from {{ ref('lkp_clr_claim_form_type_base') }}
),

har2 as (
	select * from {{ ref('hsp_account_2_base') }}
),

loc as (
	select * from {{ ref('clarity_loc_base') }}
),

sp as (
	select * from {{ ref('lkp_clr_sp_level_base') }}
),

hbc as (
	select * from {{ ref('hsd_base_class_map_base') }}
),

baseclass as (
	select * from {{ ref('lkp_clr_acct_basecls_ha_base') }}
)
select 
   age.bucket_id,
   age.aging_date,
   age.amount,
   age.hsp_account_id,
   coalesce(har.hsp_account_name,'*unknown account name') || ' ' || cast(age.hsp_account_id as varchar(18)) || '' hsp_acct_nm_wid,
   bkt.bkt_type_ha_c,
   coalesce(bkttype.name,'*unknown bucket type') || ' ' || cast(bkt.bkt_type_ha_c as varchar(18)) || '' bucket_type_nm_wid,
   age.serv_area_id,
   coalesce(sa.serv_area_name,'*unknown service area') || ' ' || cast(age.serv_area_id as varchar(18)) || '' serv_area_nm_wid,
   har.loc_id,
   (
   case
      when har.loc_id is null then '*no location'
      else coalesce(loc.loc_name,'*unknown location') || ' ' || cast(har.loc_id as varchar(18)) || '' 
   end
   )loc_nm_wid,
   age.har_account_class_c,
   coalesce(acctclass.name,'*unknown account class') || ' ' || age.har_account_class_c || '' har_account_class_nm_wid,
   age.har_acct_status_c,
   coalesce(sts.name,'*unknown account status') || ' ' || cast(age.har_acct_status_c as varchar(18)) || '' har_acct_status_nm_wid,
   age.har_fin_class_c har_fin_class_c,
   (
   case
      when age.har_fin_class_c is null then '*no primary financial class'
      else coalesce(pfinclass.name,'*unknown financial class') || ' ' || age.har_fin_class_c || ''
   end
   ) har_fin_class_nm_wid,
   age.har_prim_payor_id har_payor_id,
   (
   case
      when age.har_prim_payor_id is null then '*no primary payor'
      else coalesce(ppayor.payor_name,'*unknown payor') || ' ' || cast(age.har_prim_payor_id as varchar(18)) || ''
   end
   ) har_payor_nm_wid,
   age.har_prim_plan_id har_plan_id,
   (
   case
      when age.har_prim_plan_id is null then '*no primary plan'
      else coalesce(pplan.benefit_plan_name,'*unknown plan') || ' ' || cast(age.har_prim_plan_id as varchar(18)) || ''
   end
   ) har_plan_nm_wid,
   bpayor.financial_class bucket_fin_class_c,
   (
   case
      when age.payor_id is null or bpayor.financial_class is null then '*no bucket financial class'
      else coalesce(bfinclass.name,'*unknown financial class') || ' ' || bpayor.financial_class || ''
   end
   ) bucket_fin_class_nm_wid,
   age.payor_id bucket_payor_id,
   (
   case
      when age.payor_id is null then '*no bucket payor'
      else coalesce(bpayor.payor_name,'*unknown payor') || ' ' || cast(age.payor_id as varchar(18)) || ''
   end
   ) bucket_payor_nm_wid,
   bkt.benefit_plan_id bucket_plan_id,
   (
   case
      when bkt.benefit_plan_id is null then '*no bucket plan'
      else coalesce(bplan.benefit_plan_name,'*unknown plan') || ' ' || cast(bkt.benefit_plan_id as varchar(18)) || ''
   end
   ) bucket_plan_nm_wid,
   (
   case
      when bkt.bkt_type_ha_c = 5 or (age.bad_debt_flag_yn='y' and bkt.bkt_type_ha_c <> 1) then 'bad debt' 
      when bkt.bkt_type_ha_c in (2,3,6,7,20,21,25,26) then 
      (
      case
         when bpayor.financial_class is null then '*no bucket financial class'
         else coalesce(bfinclass.name,'*unknown financial class') || ' ' || bpayor.financial_class || ''
      end
      ) 
      when bkt.bkt_type_ha_c = 8 then 'undistributed'
      when age.har_fin_class_c is null or age.har_fin_class_c = '4' then 'self-pay - uninsured'  --self-pay balance with no existing primary coverage
      when bkt.bkt_type_ha_c = 4 then 'self-pay - residual'  --self-pay balance with an existing primary coverage
      when bkt.bkt_type_ha_c = 1 then coalesce(pfinclass.name,'*unknown financial class') || ' ' || age.har_fin_class_c || ''
   end
   ) calc_fin_class_nm_wid,
   (
   case
      when bkt.bkt_type_ha_c = 5 or (age.bad_debt_flag_yn='y' and bkt.bkt_type_ha_c <> 1) then 'bad debt'
      when bkt.bkt_type_ha_c in (2,3,6,7,20,21,25,26) then 
      (
      case
         when age.payor_id is null then '*no bucket payor'
         else coalesce(bpayor.payor_name,'*unknown payor') || ' ' || cast(age.payor_id as varchar(18)) || ''
      end
      )
      when bkt.bkt_type_ha_c = 8 then 'undistributed'
      when age.har_prim_payor_id is null or age.har_fin_class_c = '4' then 'self-pay - uninsured'  -- rsd 08/15 382904 check har financial class to account for self-pay cvgs
      when bkt.bkt_type_ha_c = 4 then 'self-pay - residual'
      when bkt.bkt_type_ha_c = 1 then coalesce(ppayor.payor_name,'*unknown payor') || ' ' || cast(age.har_prim_payor_id as varchar(18)) || ''
   end
   ) calc_payor_nm_wid,  
   (
   case
      when bkt.bkt_type_ha_c = 5 or (age.bad_debt_flag_yn='y' and bkt.bkt_type_ha_c <> 1) then 'bad debt'
      when bkt.bkt_type_ha_c in (2,3,6,7,20,21,25,26) then 
      (
      case
         when bkt.benefit_plan_id is null then '*no bucket plan'
         else coalesce(bplan.benefit_plan_name,'*unknown plan') || ' ' || cast(bkt.benefit_plan_id as varchar(18)) || ''
      end
      )
      when bkt.bkt_type_ha_c = 8 then 'undistributed'
      when age.har_prim_plan_id is null or age.har_fin_class_c = '4' then 'self-pay - uninsured'  -- rsd 08/15 382904 check har financial class to account for self-pay cvgs
      when bkt.bkt_type_ha_c = 4 then 'self-pay - residual'
      when bkt.bkt_type_ha_c = 1 then coalesce(pplan.benefit_plan_name,'*unknown plan') || ' ' || cast(age.har_prim_plan_id as varchar(18)) || ''
   end
   ) calc_plan_nm_wid,
   coalesce(age.outsource_flag_yn,'n') outsource_flag_yn,
   coalesce(age.extern_ar_flag_yn,'n') extern_ar_flag_yn,
   coalesce(age.bad_debt_flag_yn,'n') bad_debt_flag_yn,
   age.agency_id,
   (
   case
      when age.agency_id is null then '*no assigned agency'
      else concat(coalesce(agency.coll_agency_name,'*unknown agency'), ' ', cast(age.agency_id as varchar(18)))
   end
   ) agency_nm_wid,
   age.claim_form_type_c,
   (
   case
      when age.claim_form_type_c is null then '*no claim form type'
      else concat(coalesce(clform.name,'*unknown claim form type'), ' ', cast(age.claim_form_type_c as varchar(18)))
   end
   ) claim_form_type_nm_wid,
   age.sp_level_c,

   (
      case
         when age.sp_level_c is null then '*no self-pay level'
         else concat(coalesce(sp.name, '*unknown self-pay level'), ' ', cast(age.sp_level_c as varchar(18)))
      end
   ) sp_level_nm_wid,

   age.har_discharge_date har_discharge_date,
   (
   case
      when datediff(d,age.har_discharge_date,age.aging_date) > 0 then
      datediff(d,age.har_discharge_date,age.aging_date)
      else 0
   end
   ) har_discharge_age,
   age.agency_assign_date,
   (
   case
      when datediff(d,age.agency_assign_date,age.aging_date) > 0 then
      datediff(d,age.agency_assign_date,age.aging_date) 
      else 0
   end
   ) agency_assign_age,
   bkt.first_claim_date,
   (
   case
      when datediff(d,bkt.first_claim_date,age.aging_date) > 0 then
      datediff(d,bkt.first_claim_date,age.aging_date)
      else 0
   end
   ) first_claim_age,  
   age.last_claim_date,
   (
   case
      when datediff(d,age.last_claim_date,age.aging_date) > 0 then
      datediff(d,age.last_claim_date,age.aging_date)
      else 0
   end
   ) last_claim_age,
   age.first_ext_clm_date first_ext_claim_date,
   (
   case
      when datediff(d,age.first_ext_clm_date,age.aging_date) > 0 then
      datediff(d,age.first_ext_clm_date,age.aging_date)
      else 0
   end
   ) first_ext_claim_age,
   age.last_ext_clm_date last_ext_claim_date,
   (
   case
      when datediff(d,age.last_ext_clm_date,age.aging_date) > 0 then
      datediff(d,age.last_ext_clm_date,age.aging_date)
      else 0
   end
   ) last_ext_claim_age,
   age.first_payor_date,
   (
   case
      when datediff(d,age.first_payor_date,age.aging_date) > 0 then
      datediff(d,age.first_payor_date,age.aging_date) 
      else 0
   end
   ) first_payor_age,
   age.last_payor_date,
   (
   case
      when datediff(d,age.last_payor_date,age.aging_date) > 0 then
      datediff(d,age.last_payor_date,age.aging_date) 
      else 0
   end
   ) last_payor_age,
   har.frst_stmt_date first_stmt_date,
   (
   case
      when datediff(d,har.frst_stmt_date,age.aging_date) > 0 then
      datediff(d,har.frst_stmt_date,age.aging_date)
      else 0
   end
   ) first_stmt_age,
   har2.frst_full_stmt_dt first_full_stmt_date,
   (
   case
      when datediff(d,har2.frst_full_stmt_dt,age.aging_date) > 0 then
      datediff(d,har2.frst_full_stmt_dt,age.aging_date)
      else 0
   end
   ) first_full_stmt_age,
   age.sp_cycle_start_dt sp_cycle_start_date,
   (
   case
      when datediff(d,age.sp_cycle_start_dt,age.aging_date) > 0 then
      datediff(d,age.sp_cycle_start_dt,age.aging_date)
      else 0 
   end
   ) sp_cycle_start_age,
   age.har_amount,
   pplan2.prod_type_c pplan2,

  (
    case
      when pplan2.prod_type_c is null then '*no har primary product type'
      else concat(coalesce(pplanpt.name,'*unknown har primary product type'), ' ', pplan2.prod_type_c)
    end
  ) har_product_type_nm_wid,

   bplan2.prod_type_c,

  (
    case
      when bplan2.prod_type_c is null then '*no bucket product type'
      else concat(coalesce(bplanpt.name,'*unknown bucket product type'), ' ', bplan2.prod_type_c)
    end
  ) bucket_product_type_nm_wid,

   (
   case
      when bkt.bkt_type_ha_c in (6,7) and bkt.interim_end_date is not null then bkt.interim_end_date
      else age.har_discharge_date
   end
   ) calc_discharge_date,
   (
   case
      when bkt.bkt_type_ha_c in (6,7) and bkt.interim_end_date is not null then
      (
      case
         when datediff(d,bkt.interim_end_date,age.aging_date) > 0 then
          datediff(d,bkt.interim_end_date,age.aging_date)
         else 0
      end
      )
      else
      (
      case
         when datediff(d,age.har_discharge_date,age.aging_date) > 0 then
          datediff(d,age.har_discharge_date,age.aging_date)
         else 0
      end
      )
   end
   ) calc_discharge_age,
   hbc.base_class_map_c har_base_class_c,

  (
    case
      when hbc.base_class_map_c is null then '*no account base class'
      else concat(coalesce(baseclass.name, '*unknown account base class'), ' ', cast(hbc.base_class_map_c as varchar(18)))
    end
  ) har_base_class_nm_wid,

   bktsts.name,
   age.previous_credits,   
   age.tot_chgs,  
   age.tot_pmts, 
   age.tot_adjs, 
   age.bkt_balance_status_c,
   balsts.name,
   age.expected_not_allowed_amt, 
   age.expected_allowed_amt,
   age.posted_not_allowed_amt, 
   age.payor_allowed_amt, 
   age.payor_billed_amt,
   age.clm_billed_amt,
   age.bkt_billed_amt,
   age.max_collectible_ar
from
    age
left outer join
    bkt
on
   age.bucket_id = bkt.bucket_id 
left outer join
    bkttype
on
   bkt.bkt_type_ha_c = bkttype.bkt_type_ha_c
left outer join
 bktsts
on age.bkt_sts_ha_c=bktsts.bkt_sts_ha_c
left outer join
    balsts
on
   age.bkt_balance_status_c = balsts.bkt_balance_status_c
left outer join
    sa
on
   age.serv_area_id = sa.serv_area_id
left outer join
    acctclass
on
   age.har_account_class_c = acctclass.acct_class_ha_c
left outer join
    sts
on
   age.har_acct_status_c =  sts.acct_billsts_ha_c 
left outer join
    pfinclass
on
   age.har_fin_class_c = pfinclass.financial_class
left outer join
    bpayor
on
   age.payor_id = bpayor.payor_id   
left outer join
    bfinclass
on
   bpayor.financial_class = bfinclass.financial_class
left outer join
    ppayor
on
   age.har_prim_payor_id = ppayor.payor_id
left outer join
    pplan
on
   age.har_prim_plan_id = pplan.benefit_plan_id
left outer join
    pplan2
on
   age.har_prim_plan_id = pplan2.benefit_plan_id
left outer join
    pplanpt
on
   pplan2.prod_type_c = pplanpt.prod_type_c
left outer join
    bplan
on
   bkt.benefit_plan_id = bplan.benefit_plan_id
left outer join
    bplan2
on
   bkt.benefit_plan_id = bplan2.benefit_plan_id
left outer join
    bplanpt
on
   bplan2.prod_type_c =bplanpt.prod_type_c
left outer join
    har
on
   age.hsp_account_id = har.hsp_account_id  
left outer join
    agency
on
   age.agency_id = agency.col_agncy_id
left outer join
    clform
on
   age.claim_form_type_c = clform.claim_form_type_c
left outer join
    har2
on
   age.hsp_account_id = har2.hsp_account_id
left outer join
    loc
on
   har.loc_id = loc.loc_id
left outer join
    sp
on
   age.sp_level_c = sp.sp_level_c
left outer join
    hbc
on age.har_account_class_c = hbc.acct_class_map_c
left outer join
    baseclass
on hbc.base_class_map_c = baseclass.acct_basecls_ha_c