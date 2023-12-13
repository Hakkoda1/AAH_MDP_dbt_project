{{
    config(
        materialized='incremental',
        unique_key=['record_id','line']
    )
}}

{% set clarity_table_name = "cl_bev_esc_audit" %}
{% set primary_keys = ['record_id','line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}