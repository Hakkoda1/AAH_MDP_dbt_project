{{
    config(
        materialized='incremental',
        unique_key='ope_id'
    )
}}

{% set clarity_table_name = "ope_info" %}
{% set primary_keys = ['ope_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}