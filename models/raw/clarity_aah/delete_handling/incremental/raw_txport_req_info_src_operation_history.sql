{% set clarity_table_name = "txport_req_info" %}
{% set primary_keys = ["transport_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}