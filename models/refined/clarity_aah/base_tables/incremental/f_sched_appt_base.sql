{{
    config(
        materialized='incremental',
        unique_key='pat_enc_csn_id'
    )
}}

{% set clarity_table_name = "f_sched_appt" %}
{% set primary_keys = ['pat_enc_csn_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}