{% set clarity_table_name = "identity_id" %}
{% set primary_keys = ["pat_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}