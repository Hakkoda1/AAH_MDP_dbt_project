{{
    config(
        materialized='incremental',
        unique_key='department_id'
    )
}}

{% set clarity_table_name = "clarity_dep" %}
{% set primary_keys = ['department_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}