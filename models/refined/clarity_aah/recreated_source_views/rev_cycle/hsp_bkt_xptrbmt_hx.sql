with

naahxtbl as (
    select * from {{ ref('hsp_bkt_naa_adj_hx_base') }}
)

/*copyright (c) 2011-2018 epic systems corporation
********************************************************************************
title:   v_arhb_hsp_bkt_xptrbmt_hx
purpose: this view builds the expected reimbursement history for a bucket out
         of the not allowed adjustment history.
author:  brian riese
properties: none
revision history: 
*bpr 07/11 dlg#207742 - created
*jpm 10/13 dlg#288236 - filter buckets with converted rows to avoid dupes,add sources
*jal 01/15 dlg#351333 - cast time component of xr_hx_update_dt
*jcn 07/16 dlg#430517 - remove reference to hsp_bkt_xptrb_repl and add sources 19,20
*acb 11/18 dlg#578378 - add sources 21 and 22
*jak 06/22 dlg#893360 - add sources 23 and 24
*gmw 10/22 dlg#i10400458 - add sources 25 and 26
********************************************************************************
*/
select
   naahxtbl.bucket_id,
   naahxtbl.line,
   naahxtbl.cm_phy_owner_id,
   naahxtbl.cm_log_owner_id,
   cast(naahxtbl.action_instant_dttm as date) as xr_hx_update_dt,
   naahxtbl.hlb_billed_amt      as xr_hx_bill_amt,
   naahxtbl.expect_allowed_amt  as xr_hx_xpctd_amt,
   naahxtbl.invoice_num         as xr_hx_invoice,
   naahxtbl.contract_id         as xr_hx_contract_id,
   naahxtbl.contract_eff_date   as xr_hx_cntrct_eff_dt,
   naahxtbl.hsp_account_id      as hsp_account_id,
   case
      when naahxtbl.source_c in (11, 107) then 1  --override contract
      when naahxtbl.source_c in (6, 108) then 2   --override amount
      else 0                                        --no override
   end                            as xr_hx_ovr_flag_c,  
   naahxtbl.comment_text        as xr_hx_comment,
   naahxtbl.user_id             as xr_hx_user_id  
from naahxtbl
where naahxtbl.source_c in (3, 5, 6, 7, 8, 10, 11, 17, 19, 20, 21, 22, 23, 24, 25, 26, 106, 107, 108)