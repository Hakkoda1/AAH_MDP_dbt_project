{% set clarity_table_name = "clarity_lws" %}
{% set primary_keys = ["workstation_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}