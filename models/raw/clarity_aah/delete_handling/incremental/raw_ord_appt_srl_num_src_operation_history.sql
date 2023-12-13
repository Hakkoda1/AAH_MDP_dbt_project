{% set clarity_table_name = "ord_appt_srl_num" %}
{% set primary_keys = ["order_proc_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}