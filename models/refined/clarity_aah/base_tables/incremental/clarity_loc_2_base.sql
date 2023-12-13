{{
    config(
        materialized='incremental',
        unique_key='loc_id'
    )
}}

{% set clarity_table_name = "clarity_loc_2" %}
{% set primary_keys = ['loc_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}