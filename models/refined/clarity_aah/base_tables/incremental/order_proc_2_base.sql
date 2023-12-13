{{
    config(
        materialized='incremental',
        unique_key='order_proc_id'
    )
}}

{% set clarity_table_name = "order_proc_2" %}
{% set primary_keys = ['order_proc_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}