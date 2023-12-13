{{
    config(
        materialized='incremental',
        unique_key= ['order_proc_id', 'line']
    )
}}

{% set clarity_table_name = "ord_appt_srl_num" %}
{% set primary_keys = ['order_proc_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}