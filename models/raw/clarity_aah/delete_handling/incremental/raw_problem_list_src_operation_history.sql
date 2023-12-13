{% set clarity_table_name = "problem_list" %}
{% set primary_keys = ["problem_list_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}