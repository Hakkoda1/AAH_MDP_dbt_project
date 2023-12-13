{{
    config(
        materialized='incremental',
        unique_key=['prov_id', 'line']
    )
}}

{% set clarity_table_name = "clarity_ser_spec" %}
{% set primary_keys = ["prov_id", "line"] %}

{{ clarity_refined_base_incremental_table(clarity_table_name=clarity_table_name,primary_keys=primary_keys) }}