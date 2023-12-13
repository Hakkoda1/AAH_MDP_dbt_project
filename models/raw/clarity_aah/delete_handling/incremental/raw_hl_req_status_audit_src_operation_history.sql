{% set clarity_table_name = "hl_req_status_audit" %}
{% set primary_keys = ["hlr_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}