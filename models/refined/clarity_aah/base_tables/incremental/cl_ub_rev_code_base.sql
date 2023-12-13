{#{
    config(
        materialized='incremental',
        unique_key='ub_rev_code_id'
    )
}}

{% set clarity_table_name = "cl_ub_rev_code" %}
{% set primary_keys = ['ub_rev_code_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}

{% set clarity_table_name = "cl_ub_rev_code" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['ub_rev_code_id'],
        tags=["not_enabled_uat"]
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}