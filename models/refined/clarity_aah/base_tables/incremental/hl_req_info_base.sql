{{
    config(
        materialized='incremental',
        unique_key='hlr_id'
    )
}}

{% set clarity_table_name = "hl_req_info" %}
{% set primary_keys = ['hlr_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}