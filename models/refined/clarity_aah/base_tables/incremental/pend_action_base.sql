{{
    config(
        materialized='incremental',
        unique_key='pend_id'
    )
}}

{% set clarity_table_name = "pend_action" %}
{% set primary_keys = ['pend_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}