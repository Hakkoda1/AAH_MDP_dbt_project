{{
    config(
        materialized='incremental',
        unique_key='care_area_id'
    )
}}

{% set clarity_table_name = "ed_care_area_info" %}
{% set primary_keys = ['care_area_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}