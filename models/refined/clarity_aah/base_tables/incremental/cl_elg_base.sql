{#{
    config(
        materialized='incremental',
        unique_key='allergen_id'
    )
}}

{% set clarity_table_name = "cl_elg" %}
{% set primary_keys = ['allergen_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}

{% set clarity_table_name = "cl_elg" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['allergen_id'],
        tags=["not_enabled_uat"]
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}