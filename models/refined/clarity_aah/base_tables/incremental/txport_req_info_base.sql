{{
    config(
        materialized='incremental',
        unique_key='transport_id'
    )
}}

{% set clarity_table_name = "txport_req_info" %}
{% set primary_keys = ['transport_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}