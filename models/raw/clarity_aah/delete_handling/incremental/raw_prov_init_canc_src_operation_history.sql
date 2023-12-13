{% set clarity_table_name = "prov_init_canc" %}
{% set primary_keys = ["sched_def_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}