{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'zc_admin_route' %}

{{ clarity_refined_base_full_load_table(clarity_table_name=clarity_table_name) }}
