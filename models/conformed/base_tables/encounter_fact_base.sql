{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}

select

    {# keys #}
    {{ dbt_utils.generate_surrogate_key(['pat_enc.pat_enc_csn_id']) }} as encounter_key, --PK

    {{ generate_optional_foreign_key( 'pat_enc.pat_id' ) }} as patient_key,
    {{ generate_optional_foreign_key( 'pat_enc.department_id' ) }} as location_key,
    {{ generate_optional_foreign_key( 'pat_enc.visit_prov_id' ) }} as admitting_provider_key,
    {{ generate_optional_foreign_key( 'pat_enc.referral_id' ) }} as referral_key,
    {{ generate_optional_foreign_key( 'pat_enc.account_id' ) }} as guarantor_key,
    {{ generate_optional_foreign_key( 'pat_enc.coverage_id' ) }} as primary_coverage_key,
    {{ generate_optional_foreign_key( 'pat_enc.hsp_account_id' ) }} as hospital_account_key,
    {{ generate_optional_foreign_key( 'pat_enc.attnd_prov_id' ) }} as attending_provider_key,

    {# metadata #}
    current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
    'CLARITY.dbo.PAT_ENC' as record_source, --Can we make this an array?

    {# id #} 
    pat_enc.pat_enc_csn_id::numeric(18) as encounter_epic_id, --encounter_key,

    {# dimensions #}
	pat_enc.pat_enc_date_real::float as contact_date_real,
    lkp_clr_disp_enc_type.name::nvarchar as encounter_type,
    pat_enc.enc_closed_yn::nvarchar as is_encounter_closed,
    lkp_clr_appt_status.name::nvarchar as appointment_status,
    lkp_clr_hosp_admsn_type.name::nvarchar as admission_type,
    lkp_clr_pat_service.name::nvarchar as hospital_service,
    
    {# date/time #}
    pat_enc.contact_date::date as contact_date,
    pat_enc.hosp_admsn_time::datetime as admission_instant,
	pat_enc.hosp_dischrg_time::datetime as discharge_instant,
    pat_enc_hsp.adt_arrival_time::datetime as arrival_instant,
    pat_enc_hsp.emer_adm_date::datetime as emergency_class_instant

from
    {{ ref("pat_enc_hsp_base") }} pat_enc_hsp
        left join {{ ref("pat_enc_base") }} pat_enc 
            on pat_enc_hsp.pat_enc_csn_id = pat_enc.pat_enc_csn_id       
        left join {{ ref("lkp_clr_disp_enc_type_base") }} lkp_clr_disp_enc_type 
            on lkp_clr_disp_enc_type.disp_enc_type_c = pat_enc.enc_type_c
        left join {{ ref("lkp_clr_hosp_admsn_type_base") }} lkp_clr_hosp_admsn_type 
            on lkp_clr_hosp_admsn_type.hosp_admsn_type_c = pat_enc.hosp_admsn_type_c
        left join {{ ref('lkp_clr_appt_status_base') }} lkp_clr_appt_status
            on lkp_clr_appt_status.appt_status_c = pat_enc.appt_status_c       
        left join {{ ref("lkp_clr_acuity_level_base") }} lkp_clr_acuity_level 
            on lkp_clr_acuity_level.acuity_level_c = pat_enc_hsp.acuity_level_c
        left join {{ ref("lkp_clr_arriv_means_base") }} lkp_clr_arriv_means 
            on lkp_clr_arriv_means.means_of_arrv_c = pat_enc_hsp.means_of_arrv_c
        left join {{ ref("lkp_clr_disch_disp_base") }} lkp_clr_disch_disp 
            on lkp_clr_disch_disp.disch_disp_c = pat_enc_hsp.disch_disp_c
        left join {{ ref("lkp_clr_ed_disposition_base") }} lkp_clr_ed_disposition 
            on lkp_clr_ed_disposition.ed_disposition_c = pat_enc_hsp.ed_disposition_c
        left join {{ ref("lkp_clr_pat_service_base") }} lkp_clr_pat_service 
            on lkp_clr_pat_service.hosp_serv_c = pat_enc_hsp.hosp_serv_c
        left join {{ ref("f_ed_encounters_base") }} f_ed_encounters
            on f_ed_encounters.pat_enc_csn_id = pat_enc.pat_enc_csn_id
        left join {{ ref("pat_enc_2_base") }} pat_enc_2
            on pat_enc_2.pat_enc_csn_id = pat_enc.pat_enc_csn_id
        left join {{ ref("pat_enc_3_base") }} pat_enc_3
            on pat_enc_3.pat_enc_csn = pat_enc.pat_enc_csn_id
        left join {{ ref("pat_enc_hsp_2_base") }} pat_enc_hsp_2
            on pat_enc_hsp_2.pat_enc_csn_id = pat_enc_hsp.pat_enc_csn_id
        left join {{ ref("referral_apt_base") }} referral_apt
            on referral_apt.serial_number = pat_enc.pat_enc_csn_id
        left join {{ ref("v_sched_appt") }} v_sched_appt
            on v_sched_appt.pat_enc_csn_id = pat_enc.pat_enc_csn_id
