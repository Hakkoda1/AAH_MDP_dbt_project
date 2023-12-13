{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite',
        post_hook = [ "{{insert_default_key_to_dim_table( 'patient_dim_base', 
                                                          'person_key' )}}" ]
    )
}}

    select 
    
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['patient.pat_id']) }} as person_key, 

        {# id #}
        patient.pat_id::string as patient_epic_id,
        patient.pat_mrn_id::varchar as primary_mrn,

        {# dimensions #}
        patient.pat_last_name::varchar as patient_last_name,
        patient.pat_first_name::varchar as patient_first_name,
        lkp_sex.name::varchar as sex, 
        lkp_sex_asgn_at_birth.name::varchar as sex_assigned_at_birth,
        concat( ifnull(patient.add_line_1, ''), ifnull(patient.add_line_2, '' ) )::varchar as addresss,
        patient.city::varchar as city,
        lkp_state.name::varchar as state, 
        patient.zip::varchar as zip,
        lkp_patient_status.name::varchar as patient_status,
        fc.financial_class_name::varchar as primary_financial_class, 
        coalesce( v_dw_test_patients.is_test_pat_yn, patient_3.is_test_pat_yn )::varchar as is_test_patient, 

        {# date/time #}
        patient.birth_date::datetime as birth_date,
        to_date(patient.death_date)::date as death_date,

        {# metadata #}
        current_timestamp as mdp_effective_date, 
        'CLARITY.dbo.PATIENT' as record_source      
    from
        {{ ref('patient') }} patient
        left join {{ ref('lkp_clr_state') }} lkp_state
            on lkp_state.state_c = patient.state_c
        left join {{ ref('lkp_clr_patient_status') }} lkp_patient_status
            on lkp_patient_status.patient_status_c = patient.pat_status_c
        left join {{ ref('lkp_clr_sex') }} lkp_sex
            on lkp_sex.rcpt_mem_sex_c = patient.sex_c
        left join {{ ref('v_dw_test_patients') }} v_dw_test_patients 
            on v_dw_test_patients.pat_id = patient.pat_id   
        left join {{ ref('patient_4') }} patient_4
            on patient_4.pat_id = patient.pat_id
        left join {{ ref('lkp_clr_sex_asgn_at_birth') }} lkp_sex_asgn_at_birth
            on lkp_sex_asgn_at_birth.sex_asgn_at_birth_c = patient_4.sex_asgn_at_birth_c
        left join {{ ref('clarity_fc') }} fc
            on fc.financial_class = patient.def_fin_class_c
        left join {{ ref('patient_3') }} patient_3
            on patient_3.pat_id = patient.pat_id
{#         left join {{ ref('patient_2_base') }} patient_2
            on patient_2.pat_id = patient.pat_id
        left join {{ ref('patient_5_base') }} patient_5
            on patient_5.pat_id = patient.pat_id
        left join {{ ref('patient_type_base') }} patient_type
            on patient_type.pat_id = patient.pat_id
        left join {{ ref('lkp_clr_patient_type_base') }} lkp_patient_type
            on lkp_patient_type.patient_type_c = patient_type.patient_type_c 
        left join {{ ref('identity_id_base') }} identity_id
            on identity_id.pat_id = patient.pat_id
        left join {{ ref('identity_id_type_base') }} identity_id_type
            on identity_id_type.id_type = identity_id.identity_type_id
        left join {{ ref('identity_id_hx_base') }} identity_id_hx
            on identity_id_hx.pat_id = identity_id.pat_id #}

