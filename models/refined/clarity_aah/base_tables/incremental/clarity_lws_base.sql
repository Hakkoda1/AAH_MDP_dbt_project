{{
    config(
        materialized='incremental',
        unique_key='workstation_id'
    )
}}

{% set clarity_table_name = "clarity_lws" %}
{% set primary_keys = ['workstation_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}