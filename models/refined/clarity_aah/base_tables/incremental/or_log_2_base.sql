{% set clarity_table_name = "or_log_2" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['log_id'],
        tags=["not_enabled_uat"]
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}