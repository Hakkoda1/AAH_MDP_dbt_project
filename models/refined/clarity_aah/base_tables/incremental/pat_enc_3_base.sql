{{
    config(
        materialized='incremental',
        unique_key='pat_enc_csn'
    )
}}

{% set clarity_table_name = "pat_enc_3" %}
{% set primary_keys = ['pat_enc_csn'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}