{% set clarity_table_name = "ed_iev_pat_info" %}
{% set primary_keys = ["event_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}