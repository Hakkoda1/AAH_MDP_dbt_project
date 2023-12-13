{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}

select

    {# keys #}
    {{ dbt_utils.generate_surrogate_key(['ievpat.pat_id']) }} as patient_key, -- PK
    {{ generate_optional_foreign_key( 'ievpat.pat_enc_csn_id' ) }} as encounter_key,
    {{ generate_optional_foreign_key( 'ievpat.dept_event_dep_id' ) }} as location_key,
    {{ generate_optional_foreign_key( 'ievpat.adt_transfer_link' ) }} as adt_event_key,

    {# dimensions #}
        edev.record_name::varchar as emergency_event_type,

    {# metadata #}
    current_timestamp as mdp_effective_date, --should this be the latest change on any column or the last time the table was built?
    'clarity' as record_source --can we make this an array?

from {{ ref('ed_iev_pat_info_base') }} ievpat
    left join {{ ref('ed_event_tmpl_info_base') }} edev 
        on edev.record_id = ievpat.type_id