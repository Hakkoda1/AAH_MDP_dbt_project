{{
    config(
        materialized='incremental',
        unique_key= ['pharmacy_id']
    )
}}

{% set clarity_table_name = "rx_phr" %}
{% set primary_keys = ['pharmacy_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}