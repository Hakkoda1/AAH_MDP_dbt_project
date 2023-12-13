{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}

with 

    pend_action as (
        select *
        from {{ ref('pend_action_base') }}
    ),

    clarity_adt as (
        select * from {{ ref('clarity_adt_base') }}
    ),

    lkp_clr_pend_event_type_base as (
        select * from {{ ref('lkp_clr_pend_event_type_base') }}
    ),

    f_adt_bed_request_times_base as (
        select * from {{ ref('f_adt_bed_request_times_base') }}
    ),

    adtpendedeventfact as (
        
        select

            {# keys #}
            {{ dbt_utils.generate_surrogate_key( ['pend_action.pend_id'] ) }} as adt_pended_event_key, -- PK
            {{ generate_optional_foreign_key( 'pend_action.event_record_id' ) }} as adt_event_key,
            {{ generate_optional_foreign_key( 'pend_action.pat_enc_csn_id' ) }} as encounter_key,
            {{ generate_optional_foreign_key( 'pend_action.pat_id' ) }} as patient_key,

            {# ids #}
            pend_action.pend_id::varchar as pended_event_id,
            
            {# dimensions #}
            lcpet.name::varchar as pended_event_type,
            pend_action.completed_yn::varchar as is_completed,

            {# dates/time #}
            pend_action.request_time::datetime as requested_instant,
            pend_action.assigned_time::datetime as assigned_instant,

            {# metadata #}
            current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
            'CLARITY.dbo.adt_pended_event' as record_source --Can we make this an array?

        from

            pend_action

            left join clarity_adt
                on clarity_adt.event_id = pend_action.linked_event_id

            left join lkp_clr_pend_event_type_base lcpet
                on pend_action.pend_event_type_c = lcpet.pend_event_type_c

            left join f_adt_bed_request_times_base fabrt
                on pend_action.pend_id = fabrt.pend_id

    )

    select * from adtpendedeventfact