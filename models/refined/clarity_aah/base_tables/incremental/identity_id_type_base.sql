{{
    config(
        materialized='incremental',
        unique_key='id_type'
    )
}}

{% set clarity_table_name = "identity_id_type" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}