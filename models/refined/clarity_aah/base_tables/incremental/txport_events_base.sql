{{
    config(
        materialized='incremental',
        unique_key= ['txport_id', 'line']
    )
}}

{% set clarity_table_name = "txport_events" %}
{% set primary_keys = ['txport_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}