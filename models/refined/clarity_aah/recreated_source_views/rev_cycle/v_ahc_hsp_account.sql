



with

hsp_account as (
    select * from {{ ref('hsp_account_base') }}
)


select  hsp_account_id 

      , hsp_account_name 

      , acct_basecls_ha_c 

      , acct_billed_date 

      , acct_class_ha_c 

      , acct_close_date 

      , acct_fin_class_c 

      , acct_notifictn_dt 

      , acct_slfpyst_ha_c 

      , acct_billsts_ha_c 

      , acct_type_ha_c 

      , acct_zero_bal_dt 

      , assoc_authcert_id 

      , adm_date_time 

      , adm_deparment_id 

      , adm_loc_id 

      , adm_priority 

      , adm_prov_id 

      , archived_date 

      , archive_id 

      , attending_prov_id 

      , autopsy_done_yn 

      , autopsy_prov_id 

      , bad_debt_agency_id 

      , bad_debt_bucket_id 

      , bill_note_exp_date 

      , claim_id 

      , claim_user_chng_yn 

      , code_blue_ynu 

      , coll_grpr_ha_c 

      , combine_acct_id 

      , combine_comment 

      , combine_date_time 

      , combine_user_id 

      , completion_dt_tm 

      , completn_sts_ha_c 

      , coroner_case_yn 

      , coverage_id 

      , cpt_cd_new_info_yn 

      , cvg_list_select_yn 

      , disch_date_time 

      , disch_dept_id 

      , disch_destin_ha_c 

      , disch_loc_id 

      , disch_to 

      , drg_expected_reimb 

      , er_admit_date_time 

      , er_admit_src_ha_c 

      , er_admit_typ_ha_c 

      , er_dschg_date_time 

      , er_pat_sts_ha_c 

      , expiration_unit_id 

      , expird_in_house_yn 

      , final_drg_id 

      , follow_up 

      , frst_det_bill_date 

      , frst_dmnd_stmt_dt 

      , frst_stmt_date 

      , guar_addr_1 

      , guar_addr_2 

      , guar_city 

      , guar_country_c 

      , guar_county_c 

      , guar_dob 

      , guar_hm_phone 

      , guarantor_id 

      , guar_name 

      , guar_sex_c 

      , guar_ssn 

      , guar_state_c 

      , guar_wk_phone 

      , guar_zip 

      , high_risk_ynu 

      , hospice_indicator 

      , inst_of_update 

      , instruct_given_ynu 

      , is_active_yn 

      , is_called_911_ynu 

      , is_copy_flag 

      , is_cr_bal_flag 

      , is_insti_yn 

      , is_late_chg_flag 

      , is_pmtplan_amt_due 

      , last_det_bill_date 

      , last_dmnd_stmt_dt 

      , last_intrm_bill_dt 

      , last_stmt_date 

      , loc_id 

      , means_of_arrv_c 

      , next_stmt_date 

      , num_of_det_bills 

      , num_of_dmnd_stmts 

      , num_of_stmts_sent 

      , pat_addr_1 

      , pat_addr_2 

      , pat_city 

      , pat_country_c 

      , pat_county_c 

      , pat_dob 

      , pat_home_phone 

      , pat_id 

      , pat_name 

      , pat_sex_c 

      , pat_ssn 

      , pat_state_c 

      , pat_wrk_phn 

      , pat_zip 

      , plan_done_ynu 

      , police_involvd_ynu 

      , post_adm_exp_ha_c 

      , post_op_exp_ha_c 

      , prebill_bucket_id 

      , prim_svc_ha_c 

      , prior_admission 

      , psych_case_ynu 

      , readmission_indic 

      , record_create_date 

      , recur_parent_id 

      , recur_sts_ha_c 

      , referring_prov_id 

      , rehab_indicator 

      , scndry_svc_ha_c 

      , self_pay_bucket_id 

      , serv_area_id 

      , take_home_drug_ynu 

      , tot_acct_bal 

      , tot_adj 

      , tot_chgs 

      , tot_pmts 

      , transfer_from 

      , transfer_src_ha_c 

      , treatment_auth_num 

      , ub92_coins_days 

      , ub92_covered_days 

      , ub92_lifetime_days 

      , ub92_noncovrd_days 

      , ub92_tob_override 

      , undistrb_bucket_id 

      , patient_status_c 

      , admission_source_c 

      , admission_type_c 

      , primary_payor_id 

      , primary_plan_id 

      , patient_mrn 

      , num_of_charges 

      , sign_on_file_c 

      , sign_on_file_date 

      , extract_datetime 

      , prim_contact_ovrd 

      , coding_status_c 

      , coding_sts_user_id 

      , coding_datetime 

      , abstract_user_id 

      , old_recur_prnt_id 

      , old_recur_sts_c 

      , needs_repost_yn 

      , case_mix_grp_code 

      , last_cmg_code 

      , last_int_cvg_id 

      , birth_weight 

      , gestational_age 

      , discharge_weight 

      , organ_donor_yn 

      , premature_baby_yn 

      , coder_initials 

      , dnb_date 

      , admit_category_c 

      , prim_enc_csn_id 

      , prim_enc_date_real 

      , is_hospitalist_yn 

      , admit_dx_text 

      , mom_hsp_acct_id 

      , mom_patient_id 

      , first_billed_date 

      , init_coding_date 

      , last_coding_date 

      , exp_total_chg_amt 

      , exp_total_chg_cmt 

      , exp_pat_liab_cmt 

      , prorated_pat_liab 

      , prorated_pat_bal 

      , exp_noncvd_chg_amt 

      , bill_drg_idtype_id 

      , bill_drg_mdc_val 

      , bill_drg_weight 

      , bill_drg_ps 

      , bill_drg_rom 

      , bill_drg_short_los 

      , bill_drg_long_los 

      , bill_drg_amlos 

      , bill_drg_gmlos 

      , base_inv_num 

      , inv_num_seq_ctr 

      , research_id 

      , specialty_svc_c 

      , xfer_to_nurse_c 

      , xfer_to_acute_c 

      , death_type_c 

      , apgar_1_min 

      , apgar_5_min 

      , gravida 

      , para 

      , birth_cert_sent_yn 

      , failed_vbac_yn 

      , delivery_date_time 

      , prenatal_prov_id 

      , deliver_prov_id 

      , hold_status_c 

      , gest_age_baby 

      , cm_phy_owner_id 

      , cm_log_owner_id 

      , hom_clarity_flg_yn 

      , acct_followup_dt 

  from   hsp_account 

  where hsp_account.serv_area_id in (1, 10, 16, 20,800)

