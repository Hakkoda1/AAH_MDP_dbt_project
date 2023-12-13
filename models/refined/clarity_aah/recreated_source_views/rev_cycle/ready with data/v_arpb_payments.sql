/*copyright (c) 2017-2022 epic systems corporation
********************************************************************************
title:   v_arpb_payments
purpose: filters pb transactions down to payments in order to simplify payment reporting
author:  chris aplin
properties: 
revision history: 
*caplin 03/17 dlg#472269 - created
*twe    05/18 dlg#537867 - added front end, automated, and drilldown columns
*jjm    09/19 dlg#635244 - update logic of existing columns
*tpk    11/20 dlg#740395 - add mychart status
*jjm    02/22 dlg#855237 - add pat_pmt_coll_wkfl_c, myc_signin_method_c, hosp_parent_loc_id
*jcb    11/22 dlg#929597 - add two-way sms 12 to is_automated_bool
********************************************************************************
*/


with 

payment as (
	select * from {{ ref('arpb_transactions_base') }}
),

payment2 as (
	select * from {{ ref('arpb_transactions2_base') }}
),

payment3 as (
	select * from {{ ref('arpb_transactions3_base') }}
),

guarantor as (
	select * from {{ ref('account_base') }}
),

guarantor2 as (
	select * from {{ ref('account_2_base') }}
),

agpi as (
	select * from {{ ref('acct_guar_pat_info_base') }}
),

myc as (
	select * from {{ ref('f_pat_mychart_status_hx_base') }}
),

eob as (
	select * from {{ ref('pmt_eob_info_i_base') }}
),

eap as (
	select * from {{ ref('clarity_eap_base') }}
),

loc as (
	select * from {{ ref('clarity_loc_base') }}
)
select payment.tx_id,
	payment.post_date,
	payment.service_date as deposit_date,
	payment.account_id,
	guarantor.account_type_c,
	payment.department_id,
	payment.loc_id,
	payment.service_area_id as serv_area_id,
	payment.amount,
	payment.payor_id as tx_payor_id,
	payment2.post_source_c,
	payment.credit_src_module_c,
	payment.payment_source_c,
	case 
		when payment.payor_id is null
			then 1 --considered self pay if the payor is null
		else 0
		end as is_self_pay_bool,
	case 
		when payment2.first_etr_tx_id = payment.tx_id then 1
		else 0
		end as is_original_bool, 
	case 
		when payment2.is_pre_service_pmt_yn='y' then 1
		else 0
		end as is_pre_service_bool,  
	case 
		when payment2.post_source_c in (2, 3, 4, 5, 6, 12)
			then 1
		else 0
		end as is_automated_bool,
	case 
		when payment2.post_source_c in (2, 5)
			or payment.credit_src_module_c = 37
			then 1
		else 0
		end as is_front_end_collection_bool,
	eob.invoice_num,
	eap.proc_code,
	payment.original_fc_c,
        payment.pat_enc_csn_id,
        case 
                when guarantor2.mypt_id is not null 
                        then 1
                when myc.mychart_status_c = 1
                        then 1
                else 2
                end as myc_status_c,
	payment3.pat_pmt_coll_wkfl_c,
	payment3.myc_signin_method_c,
	coalesce(loc.hosp_parent_loc_id, payment.loc_id) hosp_parent_loc_id
from  payment
inner join  payment2 on payment.tx_id = payment2.tx_id
inner join  payment3 on payment.tx_id = payment3.tx_id
inner join  guarantor on payment.account_id = guarantor.account_id
inner join  guarantor2 on payment.account_id = guarantor2.acct_id
left join  agpi on guarantor.account_id = agpi.account_id 
        and agpi.guar_rel_to_pat_c = 15
left join  myc on agpi.pat_id = myc.pat_id 
        and payment.post_date >= myc.start_dttm
        and ( myc.end_dttm is null or payment.post_date < myc.end_dttm )
left join  eob on payment.tx_id = eob.tx_id
	and eob.line = 1
left join  eap on payment.proc_id = eap.proc_id
left join  loc on payment.loc_id = loc.loc_id
where payment.tx_type_c = 2