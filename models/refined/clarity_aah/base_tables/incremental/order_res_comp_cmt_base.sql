{% set clarity_table_name = 'order_res_comp_cmt' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'contact_date_real', 'line_comp', 'line_comment']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}