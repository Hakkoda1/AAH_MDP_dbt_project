{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'zc_rxnorm_code_level' %}

{{ clarity_refined_base_full_load_table(clarity_table_name=clarity_table_name) }}