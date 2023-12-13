{{
    config(
        materialized='incremental',
        unique_key='record_id'
    )
}}

{% set clarity_table_name = "cl_bev_all" %}
{% set primary_keys = ['record_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}