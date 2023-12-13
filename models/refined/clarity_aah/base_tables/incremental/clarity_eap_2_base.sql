{#{
    config(
        materialized='incremental',
        unique_key='proc_id'
    )
}}

{% set clarity_table_name = "clarity_eap_2" %}
{% set primary_keys = ['proc_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}

{% set clarity_table_name = "clarity_eap_2" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['proc_id']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}