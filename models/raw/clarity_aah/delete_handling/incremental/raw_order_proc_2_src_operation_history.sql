{% set clarity_table_name = "order_proc_2" %}
{% set primary_keys = ["order_proc_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}