{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'res_vld_audit' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}
{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}
