{% set clarity_table_name = "hl_req_info" %}
{% set primary_keys = ["hlr_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}