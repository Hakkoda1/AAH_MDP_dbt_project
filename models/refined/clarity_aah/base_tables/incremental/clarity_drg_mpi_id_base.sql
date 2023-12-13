{#{
    config(
        materialized='incremental',
        unique_key='drg_id'
    )
}}

{% set clarity_table_name = "clarity_drg_mpi_id" %}
{% set primary_keys = ['drg_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }#}


{% set clarity_table_name = "clarity_drg_mpi_id" %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

{{
    config(
        materialized='incremental',
        unique_key=['drg_id', 'line']
    )
}}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}