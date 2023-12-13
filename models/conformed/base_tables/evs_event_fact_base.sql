{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}


with 

evsevent as (

    select 
    
        {# keys #}
        {{ dbt_utils.generate_surrogate_key( [ 'v_adt_evs.clean_id' ] ) }} as evs_event_key, -- PK
        
        {{ generate_optional_foreign_key( 'cl_bev_all.linked_event_id' ) }} as adt_event_key,
        {{ generate_optional_foreign_key( 'cl_bev_all.bed_id' ) }} as location_key,
        {{ generate_optional_foreign_key( 'cl_bev_all.ept_id' ) }} as patient_key,
        {{ generate_optional_foreign_key( 'cl_bev_all.ept_csn' ) }} as encounter_key,
        {{ generate_optional_foreign_key( 'v_adt_evs.hkr_id' ) }} as employee_key,

        {# id #}
        lkp_clr_evs_type.name::varchar as evs_type, -- PK
        lkp_clr_active_2.name::varchar as epic_clean_id,

        {# dimensions #}        
        v_adt_evs.was_delayed_yn::varchar as was_delayed,
        lkp_clr_event_source.name::varchar as event_source,
        lkp_clr_cleaning_protcl.name::varchar as isolation_type,
        lkp_clr_esc_reason.name::varchar as escalation_reason,
        task_templates.task_name::varchar as clean_stage_name,
        lkp_clr_priority_2.name::varchar as event_priority,
        lkp_clr_hold_reason_2.name::varchar as hold_reason,
        lkp_clr_delay_reason.name::varchar as delay_reason,
        
        {# date/time #}
        v_adt_evs.clean_start_dttm::datetime as clean_start_instant,
        v_adt_evs.clean_asgn_dttm::datetime as clean_assigned_instant,
        v_adt_evs.clean_inp_dttm::datetime as clean_in_progress_instant,
        v_adt_evs.clean_comp_dttm::datetime as clean_completed_instant,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        'CLARITY' as record_source --Can we make this an array?
        
               
    from
    
        {{ ref('cl_bev_all_base') }} cl_bev_all
        
        left join {{ ref('patient_base') }} patient
            on cl_bev_all.ept_id = patient.pat_id

        left join {{ ref('pat_enc_base') }} pat_enc
            on cl_bev_all.ept_csn = pat_enc.pat_enc_csn_id

        left join {{ ref('clarity_bed_base') }} clarity_bed
            on cl_bev_all.bed_id = clarity_bed.bed_id
        
        left join {{ ref('task_templates_base') }} task_templates
            on cl_bev_all.cur_stage_id = task_templates.task_id

        left join {{ ref('v_adt_evs') }} v_adt_evs
            on cl_bev_all.record_id = v_adt_evs.clean_id

        left join {{ ref('lkp_clr_priority_2_base') }} lkp_clr_priority_2
            on cl_bev_all.priority_c = lkp_clr_priority_2.priority_2_c
        
        left join {{ ref('cl_bev_isolations_base') }} cl_bev_isolations
            on cl_bev_isolations.record_id = v_adt_evs.clean_id
        
        left join {{ ref('lkp_clr_cleaning_protcl_base') }} lkp_clr_cleaning_protcl
            on cl_bev_isolations.isolation_c = lkp_clr_cleaning_protcl.cleaning_protcl_c

        left join {{ ref('lkp_clr_event_source_base') }} lkp_clr_event_source
            on lkp_clr_event_source.event_source_c = v_adt_evs.event_source_c

        left join {{ ref('lkp_clr_active_2_base') }} lkp_clr_active_2
            on v_adt_evs.active_c = lkp_clr_active_2.active_2_c
        
        left join {{ ref('cl_bev_esc_audit_base') }} cl_bev_esc_audit
            on cl_bev_esc_audit.record_id = v_adt_evs.clean_id
        
        left join {{ ref('cl_bev_events_all_base') }} cl_bev_events_all
            on cl_bev_events_all.record_id = v_adt_evs.clean_id

        left join {{ ref('lkp_clr_evs_type_base') }} lkp_clr_evs_type
            on lkp_clr_evs_type.evs_type_c = v_adt_evs.evs_type_c

        left join {{ ref('lkp_clr_hold_reason_2_base') }} lkp_clr_hold_reason_2
            on cl_bev_events_all.hold_reason_c = lkp_clr_hold_reason_2.hold_reason_2_c
        
        left join {{ ref('lkp_clr_delay_reason_base') }} lkp_clr_delay_reason
            on cl_bev_events_all.delay_reason_c = lkp_clr_delay_reason.delay_reason_c

        left join {{ ref('lkp_clr_esc_reason_base') }} lkp_clr_esc_reason
            on lkp_clr_esc_reason.esc_reason_c = cl_bev_esc_audit.esc_reason_audit_c

)

select * from evsevent

