with 

patient as (
    select * from {{ ref('patient_base') }}
),

f_img_study as (
    select * from {{ ref('f_img_study_base') }}
),

ord_perf_chrg as (
    select * from {{ ref('ord_perf_chrg_base') }}
),

ope_info as (
    select * from {{ ref('ope_info_base') }}
),

clarity_eap as (
    select * from {{ ref('clarity_eap_base') }}
),

clarity_eap_ot as (
    select * from {{ ref('clarity_eap_ot_base') }}
),

linked_chargeables as (
    select * from {{ ref('linked_chargeables_base') }}
),

in_line_eap as (
    select
        eap.proc_id,
        max(eap.proc_name) proc_name,
        max(eap.proc_code) proc_code,
        max(coalesce(eap.ec_ovrd_proc_cat_id, eap.proc_cat_id)) proc_cat_id,
        count(distinct eapot.cpt_code) cpt_count
    from
        clarity_eap eap 
        left outer join linked_chargeables chgeap on eap.proc_id = chgeap.proc_id
        left outer join clarity_eap_ot eapot on eapot.proc_id = chgeap.linked_chrg_id 
    where
        not exists ( 
            select 1
            from linked_chargeables chgeap_more_recent
            where chgeap_more_recent.proc_id = chgeap.proc_id 
                and chgeap_more_recent.contact_date_real > chgeap.contact_date_real ) 
            and not exists ( 
                select 1
                from clarity_eap_ot eapot_more_recent
                where eapot_more_recent.proc_id = eapot.proc_id 
                    and eapot_more_recent.contact_date_real > eapot.contact_date_real )
    group by
        eap.proc_id
)

select
    fimg.order_id order_id,
    fimg.pat_id pat_id,
    ept.pat_mrn_id pat_mrn_id, 
    ept.pat_name pat_name,
    case when ept.pat_name is null then
        case when ept.pat_mrn_id is null then
            '*unnamed patient [mrn not specified]'
        else
            '*unnamed patient [' || ept.pat_mrn_id || ']'
        end
    else
        case when ept.pat_mrn_id is null then
            ept.pat_name || ' [mrn not specified]'
        else
            ept.pat_name || ' [' || ept.pat_mrn_id || ']'
        end
    end pat_nm_wmrn,
    ept.sex_c pat_sex_c,
    fimg.pat_age_at_exam pat_age_at_exam,
    fimg.accession_num accession_num,
    fimg.ordering_login_dep_id ordering_login_dep_id,
    fimg.ordering_contact_dep_id ordering_contact_dep_id,
    fimg.ordering_csn_id ordering_csn_id,
    fimg.performing_csn_id performing_csn_id,
    fimg.performing_dep_id performing_dep_id,    
    fimg.performing_prov_id performing_prov_id,
    fimg.study_status_c study_status_c,
    fimg.ordering_prov_id ordering_prov_id,
    fimg.authorizing_prov_id authorizing_prov_id,
    fimg.ordering_dttm ordering_dttm,
    fimg.end_exam_dttm end_exam_dttm,
    fimg.finalizing_dttm finalizing_dttm,
    fimg.finalizing_prov_id finalizing_prov_id,
    fimg.log_id log_id,
    coalesce(ope.performable_id, fimg.proc_id) proc_id,  --if using orderable/performable, show performable
    coalesce(perf.line, 1) proc_line, 
    inlineeap.proc_name proc_name,
    inlineeap.proc_code proc_code,                                            
    inlineeap.proc_cat_id proc_cat_id,
    inlineeap.cpt_count num_cpt_codes,                                                
    fimg.is_canceled_yn is_canceled_yn,
    fimg.begin_exam_dttm begin_exam_dttm,
    fimg.study_grp_order_id study_grp_order_id,
    fimg.original_order_id original_order_id,
    fimg.referral_id referral_id,
    fimg.pat_class_c pat_class_c,
    fimg.max_ord_date_real max_ord_date_real,
    fimg.tech_user_id tech_user_id,
    fimg.dictating_user_id dictating_user_id,
    fimg.dictating_dttm dictating_dttm,
    fimg.trans_user_id trans_user_id,
    fimg.trans_dttm trans_dttm,
    fimg.sched_exam_dttm sched_exam_dttm,
    fimg.sched_on_dttm sched_on_dttm,
    fimg.proc_id ord_proc_id,
    fimg.checkin_dttm checkin_dttm,
    fimg.pps_start_dttm pps_start_dttm,
    fimg.pps_end_dttm pps_end_dttm,
    fimg.canceling_dttm canceling_dttm,
    fimg.canceling_user_id canceling_user_id,
    fimg.result_note_csn result_note_csn,
    fimg.abnormal_yn abnormal_yn,
    fimg.interesting_study_c interesting_study_c,
    fimg.order_priority_c order_priority_c,
    fimg.hsp_account_id hsp_account_id,
    fimg.performing_loc_id performing_loc_id,
    fimg.prelim_user_id prelim_user_id,
    fimg.prelim_dttm prelim_dttm
from
    f_img_study fimg
    inner join patient ept 
        on fimg.pat_id = ept.pat_id
    left outer join ord_perf_chrg perf 
        on fimg.order_id = perf.order_id
    left outer join ope_info ope 
        on perf.performable_id = ope.ope_id
    left outer join in_line_eap inlineeap 
        on inlineeap.proc_id = coalesce(ope.performable_id, fimg.proc_id)
