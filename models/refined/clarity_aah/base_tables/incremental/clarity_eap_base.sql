{{
    config(
        materialized='incremental',
        unique_key='proc_id'
    )
}}

{% set clarity_table_name = "clarity_eap" %}
{% set primary_keys = ['proc_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}