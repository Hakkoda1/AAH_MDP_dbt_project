{{
    config(
        materialized='incremental',
        unique_key='loc_id'
    )
}}

{% set clarity_table_name = "or_loc" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}