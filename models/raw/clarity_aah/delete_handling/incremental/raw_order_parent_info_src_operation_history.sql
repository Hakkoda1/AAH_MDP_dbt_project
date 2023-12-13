{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'order_parent_info' %}
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %}
{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}