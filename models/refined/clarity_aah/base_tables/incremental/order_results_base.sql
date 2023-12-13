{% set clarity_table_name = 'order_results' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['order_proc_id', 'line', 'ord_date_real']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}