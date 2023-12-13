{% set clarity_table_name = "clarity_emp" %}
{% set primary_keys = ["user_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}