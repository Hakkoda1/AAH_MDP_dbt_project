{#{
    config(
        materialized='incremental',
        unique_key='icd_px_id'
    )
}}

{% set clarity_table_name = "cl_icd_px" %}
{% set primary_keys = ['icd_px_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}

{% set clarity_table_name = "cl_icd_px" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['icd_px_id'],
        tags=["not_enabled_uat"]
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}