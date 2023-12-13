{% set clarity_table_name = "clarity_dep_2" %}
{% set primary_keys = ["department_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}