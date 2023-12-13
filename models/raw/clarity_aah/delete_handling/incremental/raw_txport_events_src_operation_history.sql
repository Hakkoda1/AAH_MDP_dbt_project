{% set clarity_table_name = "txport_events" %}
{% set primary_keys = ["txport_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}