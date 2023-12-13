{% set clarity_table_name = "order_rad_audit" %}
{% set primary_keys = ["order_proc_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}