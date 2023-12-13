{{
    config(
        materialized='incremental',
        unique_key='task_id'
    )
}}

{% set clarity_table_name = "task_templates" %}
{% set primary_keys = ['task_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}