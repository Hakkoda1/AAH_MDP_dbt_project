{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}
--        materialized='incremental',
--        unique_key='adt_event_key'

with 
    adt as (
    select *
    from {{ ref('clarity_adt_base') }} t

    {#
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where t.mdp_effective_datetime > (select max(t2.mdp_effective_date) from {{ this }} t2)

    {% endif %}
    
    
    #}
    )
    ,

    adteventfact as (

        select
            {# keys #}
            {{ dbt_utils.generate_surrogate_key( ['v_adt_ed_admits.event_id'] ) }} as adt_event_key, -- pk

            {{ generate_optional_foreign_key( 'v_adt_ed_admits.xfer_in_event_id' ) }} as admit_adt_event_key,
            {{ generate_optional_foreign_key( 'v_adt_ed_admits.pat_id' ) }} as patient_key, 
            {{ generate_optional_coalesce_foreign_key( [ 'adt.pat_enc_csn_id', 'v_adt_ed_admits.pat_enc_csn_id' ] ) }} as encounter_key,
            {{ generate_optional_coalesce_foreign_key( [ 'adt.bed_id', 'adt.room_id', 'adt.department_id', 'v_adt_ed_admits.department_id' ] ) }} as location_key,

            {# ids #}
            adt.event_id::number(18,0) as adt_event_epic_id, -- pk

            {# dimensions #}
            lkp_event_type.name::varchar as event_type,
            adt.accommodation_c::varchar as accommodation,
            lkp_bed_status.name::varchar as bed_status,

            {# date/time #}
            adt.effective_time::datetime as event_instant,
            v_adt_ed_admits.board_start_dttm::datetime as board_start_instant,
            v_adt_ed_admits.board_end_dttm::datetime as board_end_instant,

            {# metadata #}
            current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
            'CLARITY.dbo.adt_event' as record_source --Can we make this an array?
            
        from 
            adt
            left join {{ ref('lkp_clr_event_type_base') }} lkp_event_type
                on lkp_event_type.event_type_c = adt.event_type_c
            left join {{ ref('lkp_clr_bed_status_base') }} lkp_bed_status
                on lkp_bed_status.bed_status_c = adt.status_of_bed_c
            left join {{ ref('v_adt_ed_admits') }} v_adt_ed_admits
                on v_adt_ed_admits.event_id = adt.event_id

    )

select * from adteventfact
