{% set clarity_table_name = "patient" %}
{% set primary_keys = ["pat_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}