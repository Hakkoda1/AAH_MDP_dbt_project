{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = "f_log_based" %}
-- {% set primary_keys = ['allergen_id'] %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}
{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}