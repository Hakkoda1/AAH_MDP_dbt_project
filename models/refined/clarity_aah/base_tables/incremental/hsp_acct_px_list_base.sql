{% set clarity_table_name = 'hsp_acct_px_list' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['hsp_account_id', 'line']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}