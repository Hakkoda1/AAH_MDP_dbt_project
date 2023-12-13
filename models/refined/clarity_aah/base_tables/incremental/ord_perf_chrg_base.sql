{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'line']
    )
}}

{% set clarity_table_name = "ord_perf_chrg" %}
{% set primary_keys = ['order_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}