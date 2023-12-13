{% set clarity_table_name = "ord_perf_chrg" %}
{% set primary_keys = ["order_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}