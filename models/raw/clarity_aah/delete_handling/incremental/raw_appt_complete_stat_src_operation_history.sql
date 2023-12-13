{% set clarity_table_name = "appt_complete_stat" %}
{% set primary_keys = ["sched_def_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}