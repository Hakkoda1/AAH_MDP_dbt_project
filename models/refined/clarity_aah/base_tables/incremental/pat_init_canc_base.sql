{{
    config(
        materialized='incremental',
        unique_key=['sched_def_id','line']
    )
}}

{% set clarity_table_name = "pat_init_canc" %}
{% set primary_keys = ['sched_def_id','line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}