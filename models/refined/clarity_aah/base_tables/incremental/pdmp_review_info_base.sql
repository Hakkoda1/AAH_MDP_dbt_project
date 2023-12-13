{% set clarity_table_name = 'pdmp_review_info' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['pat_id', 'line']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}