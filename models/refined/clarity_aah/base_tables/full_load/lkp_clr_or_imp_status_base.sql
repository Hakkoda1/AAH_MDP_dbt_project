{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'zc_or_imp_status' %}

{{ clarity_refined_base_full_load_table(clarity_table_name=clarity_table_name) }}
