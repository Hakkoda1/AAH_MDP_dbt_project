{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

{% set clarity_table_name = "f_img_study" %}
{% set primary_keys = ['order_id'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}