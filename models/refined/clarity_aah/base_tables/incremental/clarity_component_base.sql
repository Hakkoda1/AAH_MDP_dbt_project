{#{
    config(
        materialized='incremental',
        unique_key='component_id'
    )
}}

{% set clarity_table_name = "clarity_component" %}
{% set primary_keys = ['component_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}

{% set clarity_table_name = "clarity_component" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['component_id']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}