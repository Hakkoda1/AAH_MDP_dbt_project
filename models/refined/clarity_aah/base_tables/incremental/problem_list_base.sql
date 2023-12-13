{{
    config(
        materialized='incremental',
        unique_key='problem_list_id'
    )
}}

{% set clarity_table_name = "problem_list" %}
{% set primary_keys = ['problem_list_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}