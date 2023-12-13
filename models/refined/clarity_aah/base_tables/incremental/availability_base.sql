{% set clarity_table_name = "availability" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['DEPARTMENT_ID', 'SLOT_BEGIN_TIME', 'PROV_ID', 'APPT_NUMBER']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}