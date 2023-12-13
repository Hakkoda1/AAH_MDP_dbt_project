{% set clarity_table_name = "patient_type" %}
{% set primary_keys = ["pat_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}