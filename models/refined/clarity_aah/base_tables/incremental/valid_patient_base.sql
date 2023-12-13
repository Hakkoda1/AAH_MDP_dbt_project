{{
    config(
        materialized='incremental',
        unique_key='pat_id'
    )
}}

{% set clarity_table_name = "valid_patient" %}
{% set primary_keys = ['pat_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}