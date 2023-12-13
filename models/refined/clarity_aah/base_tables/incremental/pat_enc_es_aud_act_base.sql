{{
    config(
        materialized='incremental',
        unique_key= ['pat_enc_csn_id', 'line']
    )
}}

{% set clarity_table_name = "pat_enc_es_aud_act" %}
{% set primary_keys = ['pat_enc_csn_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}