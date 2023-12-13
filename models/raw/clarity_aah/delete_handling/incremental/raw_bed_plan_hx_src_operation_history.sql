{% set clarity_table_name = "bed_plan_hx" %}
{% set primary_keys = ["pend_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}