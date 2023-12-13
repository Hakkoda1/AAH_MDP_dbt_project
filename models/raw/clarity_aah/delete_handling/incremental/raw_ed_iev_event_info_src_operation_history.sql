{% set clarity_table_name = "ed_iev_event_info" %}
{% set primary_keys = ["event_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}