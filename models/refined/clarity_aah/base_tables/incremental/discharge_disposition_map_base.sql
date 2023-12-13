{{
    config(
        materialized='incremental',
        unique_key= ['facility_id', 'line']
    )
}}

{% set clarity_table_name = "discharge_disposition_map" %}
{% set primary_keys = ['facility_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}