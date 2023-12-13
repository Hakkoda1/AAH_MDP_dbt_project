{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

{% set clarity_table_name = "ed_iev_pat_info" %}
{% set primary_keys = ['event_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}