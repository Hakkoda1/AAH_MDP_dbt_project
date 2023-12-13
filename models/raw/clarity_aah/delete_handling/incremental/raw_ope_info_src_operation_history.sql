{% set clarity_table_name = "ope_info" %}
{% set primary_keys = ["ope_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}