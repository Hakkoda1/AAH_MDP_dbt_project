{{
    config(
        materialized='incremental',
        unique_key='user_id'
    )
}}

{% set clarity_table_name = "clarity_emp" %}
{% set primary_keys = ['user_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}