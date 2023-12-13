{{
    config(
        materialized='incremental',
        unique_key=['hsp_account_id','line']
    )
}}

{% set clarity_table_name = "hsp_acct_dx_list" %}
{% set primary_keys = ['hsp_account_id','line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}