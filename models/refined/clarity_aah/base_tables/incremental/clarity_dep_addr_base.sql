{#{
    config(
        materialized='incremental',
        unique_key=['department_id', 'line']
    )
}}

{% set clarity_table_name = "clarity_dep_addr" %}
{% set primary_keys = ['department_id', 'line'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}

{% set clarity_table_name = "clarity_dep_addr" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key= ['department_id', 'line']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}