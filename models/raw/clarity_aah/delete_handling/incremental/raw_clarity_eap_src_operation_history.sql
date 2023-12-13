{% set clarity_table_name = "clarity_eap" %}
{% set primary_keys = ["proc_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}