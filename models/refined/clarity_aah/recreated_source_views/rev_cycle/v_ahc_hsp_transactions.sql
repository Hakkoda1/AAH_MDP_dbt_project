
with

hsp_transactions as (
    select * from {{ref('hsp_transactions_base')}}
)




select   tx_id 

      , hsp_account_id 

      , acct_class_ha_c 

      , action_string 

      , allowed_amount 

      , billed_amount 

      , billing_prov_id 

      , bucket_id 

      , coinsurance_amount 

      , copay_amount 

      , cost_cntr_id 

      , cpt_code 

      , deductible_amount 

      , department 

      , dflt_cost_cntr_id 

      , dflt_fee_sched_id 

      , dflt_proc_desc 

      , dflt_ub_rev_cd_id 

      , facility_id 

      , filer_number 

      , fin_class_c 

      , gl_credit_num 

      , gl_debit_num 

      , inst_bill_comment 

      , inst_bill_dob 

      , inst_bill_emp_num 

      , inst_bill_pat_name 

      , inst_bill_sex_c 

      , inst_bill_ssn 

      , int_control_number 

      , is_system_adj_yn 

      , is_contested_yn 

      , is_hospitalist_yn 

      , is_late_charge_yn 

      , is_recoupment_yn 

      , line_num_in_htt 

      , modifiers 

      , old_hsp_account_id 

      , order_id 

      , orig_price 

      , orig_repost_tx_id 

      , orig_rev_tx_id 

      , pat_enc_csn_id 

      , payment_from 

      , payment_src_ha_c 

      , payor_id 

      , performing_prov_id 

      , post_batch_num 

      , prev_credits_act 

      , prim_fee_sched_id 

      , procedure_desc 

      , proc_id 

      , quantity 

      , reference_num 

      , remit_codes 

      , ub_rev_code_id 

      , revenue_loc_id 

      , rvu 

      , serv_area_id 

      , service_date 

      , session_number 

      , show_hcpcs_onub_yn 

      , start_date_time 

      , stop_date_time 

      , temp_tx_id 

      , total_charges_act 

      , tx_amount 

      , tx_batch_num 

      , tx_comment 

      , tx_filed_time 

      , tx_num_in_hospacct 

      , tx_post_date 

      , tx_source_ha_c 

      , tx_type_ha_c 

      , type_of_svc_ha_c 

      , user_id 

      , proc_mpi_id 

      , xfer_liab_adj_yn 

      , chg_cred_orig_id 

      , late_crctn_orig_id 

      , allowance_adj_yn 

      , place_of_svc_id 

      , non_covered_yn 

      , ben_bkt_cvg_id 

      , ben_bkt_inc_str 

      , quick_pmt_type_c 

      , non_covered_amt 

      , line_level_info_yn 

      , is_refund_adj_yn 

      , cm_phy_owner_id 

      , cm_log_owner_id 

      , invoice_num 

      , collection_agency 

      , primary_plan_id 

      , hcpcs_code 

      , ndc_id 

      , hipps_code 

      , hipps_code_type_c 

      , hipps_code_desc 

      , addr_name 

      , addr_line1 

      , addr_line2 

      , addr_city 

      , state_c 

      , addr_zip 

      , rfnd_snd_to_c 

      , rfnd_guar_id 

      , rfnd_patient_id 

      , rfnd_coverage_id 

      , refund_plan_id 

      , chg_router_src_id 

      , reconciliation_num 

      , rfnd_cust_payee_c 

      , ce_src_dep_id 

      , ce_post_dt 

      , ce_filed_time 

      , ce_src_har_id 

      , ce_src_htr_id 

      , ce_src_hlb_id 

      , hom_clarity_flg_yn 

      , ce_hm_off_txtyp_c 

      , cash_id 

      , workstation_id 

      , pos_sessionid 

      , pos_txid 

      , pos_tx_line 

      , orig_etr_id 

      , pat_serial_num 

      , extern_ar_flag_yn 

      , erx_id 

      , sup_id 

      , bad_debt_flag_yn 

      , rvu_work 

      , rvu_overhead 

      , rvu_mlpract 

      , pmt_receipt_no 

      , rfnd_ap_date 

      , rfnd_ap_status_c 

      , optime_log_id 

      , ini_file_attempt_dt 

      , related_htr_id 

      , inst_bill_lnkpat_id 

      , imd_id 

      , duration_minutes 

      , eb_pmt_har_res_yn 

      , pmt_har_dis_from_dt 

      , pmt_har_dis_to_dt 

      , override_xover_yn 

      , comm_mod_distb_h_yn 

      , comm_mod_home_ad_yn 

      , cm_src_htr_dot_one 

      , research_study_id 

      , rsh_chg_orig_har_id 

      , payment_not_allowed 

      , eb_pmt_total_amount 

      , eb_pmt_post_type_c 

      , eb_prepmt_post_tp_c 

      , panel_id 

      , panel_dt 

      , mea_id_c 

      , panel_dat 

      , elec_pmt_aprvl_code 

      , elec_pmt_inst_time 

      , elec_pmt_evm_id 

      , elec_pmt_crd_brnd_c 

      , cost 

      , implant_id 

      , linked_htr_id 

  from  hsp_transactions 

  where hsp_transactions.serv_area_id in (1, 10, 16, 20,800)

