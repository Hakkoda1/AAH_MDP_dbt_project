{{
    config(
        materialized='incremental',
        unique_key=['pat_id', 'line']
    )
}}

{% set clarity_table_name = "patient_race" %}
{% set primary_keys = ['pat_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}