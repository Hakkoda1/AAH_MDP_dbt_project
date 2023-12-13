{{
    config(
        materialized='incremental',
        unique_key= ['hlr_id', 'line']
    )
}}

{% set clarity_table_name = "hl_req_status_audit" %}
{% set primary_keys = ['hlr_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}