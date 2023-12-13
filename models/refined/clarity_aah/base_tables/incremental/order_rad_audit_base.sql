{{
    config(
        materialized='incremental',
        unique_key=['order_proc_id', 'line']
    )
}}

{% set clarity_table_name = "order_rad_audit" %}
{% set primary_keys = ['order_proc_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
