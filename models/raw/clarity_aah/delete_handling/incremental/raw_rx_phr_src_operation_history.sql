{% set clarity_table_name = "rx_phr" %}
{% set primary_keys = ["pharmacy_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}