{{
    config(
        materialized='incremental',
        unique_key= ['pend_id', 'line']
    )
}}

{% set clarity_table_name = "bed_plan_hx" %}
{% set primary_keys = ['pend_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}