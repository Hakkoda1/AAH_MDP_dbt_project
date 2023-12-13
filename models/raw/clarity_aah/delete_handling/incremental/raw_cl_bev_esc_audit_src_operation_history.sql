{% set clarity_table_name = "cl_bev_esc_audit" %}
{% set primary_keys = ["record_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}