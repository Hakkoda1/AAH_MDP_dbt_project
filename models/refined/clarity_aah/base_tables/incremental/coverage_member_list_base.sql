
{% set clarity_table_name = "coverage_member_list" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['coverage_id', 'line']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}

