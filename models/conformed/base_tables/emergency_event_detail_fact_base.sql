{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}


select

    {# keys #}
    {{ dbt_utils.generate_surrogate_key(['ievev.event_id']) }} as emergency_event_key, --PK,01
    {{ generate_optional_foreign_key( 'ievev.event_dept_id' ) }} as emergency_event_detail_location_key,
    {{ generate_optional_foreign_key( 'ievev.adt_event_id' ) }} as emergency_event_detail_adt_event_key,
    {{ generate_optional_foreign_key( 'ievev.event_prov_id' ) }} as emergency_event_detail_provider_key,

    {# ids #}
    ievev.line::varchar as line, --PK,02

    {# dimensions #}    
    ev.record_name::varchar as emergency_event_detail_type,
    ievev.event_display_name::varchar as emergency_event_detail_name,
    ievev.event_cmt::varchar as emergency_event_detail_comment,

    {# date/time #}
    ievev.event_time::datetime as emergency_event_detail_instant,
    ievev.event_record_time::datetime as emergency_event_detail_recorded_instant,


    {# metadata #}
    current_timestamp as mdp_effective_date, --should this be the latest change on any column or the last time the table was built?
    'clarity' as record_source --can we make this an array?


from {{ ref("ed_iev_event_info_base") }} ievev 
    left join {{ ref('ed_event_tmpl_info_base') }} ev 
        on ev.record_id = ievev.event_type

