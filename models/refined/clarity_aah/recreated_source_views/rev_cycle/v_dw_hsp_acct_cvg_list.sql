with

coverage_mem_list as (
  select * from {{ref('coverage_member_list_base')}}
),

coverage as (
  select * from {{ref('coverage_base')}}
),

hsp_acct_cvg_list as (
  select * from {{ref('hsp_acct_cvg_list_base')}}
),

hsp_account as (
  select * from {{ref('hsp_account_base')}}
)

select     a.hsp_account_id, a.pat_id, a. "1_cvg_id", a. "1_payor_id", a. "1_plan_id", b. "2_cvg_id", b. "2_payor_id", b. "2_plan_id", c. "3_cvg_id", 

                      c. "3_payor_id", c. "3_plan_id", d. "4_cvg_id", d. "4_payor_id", d. "4_plan_id"

from         (select     h.hsp_account_id, h.pat_id, h.coverage_id as "1_cvg_id", h.primary_payor_id as "1_payor_id", 

                                              h.primary_plan_id as "1_plan_id", cm.mem_eff_from_date, cm.mem_eff_to_date

                       from          hsp_account as h inner join

                                              coverage_mem_list as cm on cm.coverage_id = h.coverage_id and h.pat_id = cm.pat_id inner join

                                              coverage as c on c.coverage_id = cm.coverage_id) as a left outer join

                          (select     h.hsp_account_id, h.pat_id, hc.coverage_id as "2_cvg_id", c.payor_id as "2_payor_id", c.plan_id as "2_plan_id"

                            from          hsp_account as h inner join

                                                   hsp_acct_cvg_list as hc on h.hsp_account_id = hc.hsp_account_id and hc.line = 2 inner join

                                                   coverage_mem_list as cm on cm.coverage_id = hc.coverage_id and h.pat_id = cm.pat_id inner join

                                                   coverage as c on c.coverage_id = cm.coverage_id

                            where      (cm.mem_eff_from_date is null) and (cm.mem_eff_to_date is null) or

                                                   (cm.mem_eff_from_date <= h.adm_date_time) and (cm.mem_eff_to_date is null) or

                                                   (cm.mem_eff_from_date <= h.adm_date_time) and (cm.mem_eff_to_date >= h.adm_date_time)) as b on 

                      a.hsp_account_id = b.hsp_account_id and a.pat_id = b.pat_id and b. "2_payor_id" <> a. "1_payor_id" left outer join

                          (select     h.hsp_account_id, h.pat_id, hc.coverage_id as "3_cvg_id", c.payor_id as "3_payor_id", c.plan_id as "3_plan_id"

                            from          hsp_account as h inner join

                                                   hsp_acct_cvg_list as hc on h.hsp_account_id = hc.hsp_account_id and hc.line = 3 inner join

                                                   coverage_mem_list as cm on cm.coverage_id = hc.coverage_id and h.pat_id = cm.pat_id inner join

                                                   coverage as c on c.coverage_id = cm.coverage_id

                            where      (cm.mem_eff_from_date is null) and (cm.mem_eff_to_date is null) or

                                                   (cm.mem_eff_from_date <= h.adm_date_time) and (cm.mem_eff_to_date is null) or

                                                   (cm.mem_eff_from_date <= h.adm_date_time) and (cm.mem_eff_to_date >= h.adm_date_time)) as c on 

                      a.hsp_account_id = c.hsp_account_id and a.pat_id = c.pat_id and c. "3_payor_id" <> a. "1_payor_id" and 

                      c. "3_payor_id" <> b. "2_payor_id" left outer join

                          (select     h.hsp_account_id, h.pat_id, hc.coverage_id as "4_cvg_id", c.payor_id as "4_payor_id", c.plan_id as "4_plan_id"

                            from          hsp_account as h inner join

                                                   hsp_acct_cvg_list as hc on h.hsp_account_id = hc.hsp_account_id and hc.line = 4 inner join

                                                   coverage_mem_list as cm on cm.coverage_id = hc.coverage_id and h.pat_id = cm.pat_id inner join

                                                   coverage as c on c.coverage_id = cm.coverage_id

                            where      (cm.mem_eff_from_date is null) and (cm.mem_eff_to_date is null) or

                                                   (cm.mem_eff_from_date <= h.adm_date_time) and (cm.mem_eff_to_date is null) or

                                                   (cm.mem_eff_from_date <= h.adm_date_time) and (cm.mem_eff_to_date >= h.adm_date_time)) as d on 

                      a.hsp_account_id = d.hsp_account_id and a.pat_id = d.pat_id and d. "4_payor_id" <> a. "1_payor_id" and 

                      d. "4_payor_id" <> b. "2_payor_id" and d. "4_payor_id" <> c. "3_payor_id"

                      

                      group by a.hsp_account_id, a.pat_id, a. "1_cvg_id", a. "1_payor_id", a. "1_plan_id", b. "2_cvg_id", b. "2_payor_id", b. "2_plan_id", c. "3_cvg_id", 

                      c. "3_payor_id", c. "3_plan_id", d. "4_cvg_id", d. "4_payor_id", d. "4_plan_id"



