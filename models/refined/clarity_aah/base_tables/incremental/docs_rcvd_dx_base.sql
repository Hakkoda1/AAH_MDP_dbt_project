{% set clarity_table_name = 'docs_rcvd_dx' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['document_id', 'contact_date_real', 'line']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}