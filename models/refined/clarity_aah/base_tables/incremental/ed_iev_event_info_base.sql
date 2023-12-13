{{
    config(
        materialized='incremental',
        unique_key=['event_id','line']
    )
}}

{% set clarity_table_name = "ed_iev_event_info" %}
{% set primary_keys = ['event_id','line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}