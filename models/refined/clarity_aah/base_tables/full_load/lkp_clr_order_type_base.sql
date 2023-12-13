{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'zc_order_type' %}

{{ clarity_refined_base_full_load_table(clarity_table_name=clarity_table_name) }}
