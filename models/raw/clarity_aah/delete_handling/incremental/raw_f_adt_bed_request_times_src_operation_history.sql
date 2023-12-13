{% set clarity_table_name = "f_adt_bed_request_times" %}
{% set primary_keys = ["pend_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}