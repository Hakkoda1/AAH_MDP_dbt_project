{% set clarity_table_name = "task_templates" %}
{% set primary_keys = ["task_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}