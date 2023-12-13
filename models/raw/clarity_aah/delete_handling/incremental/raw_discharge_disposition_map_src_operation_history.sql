{% set clarity_table_name = "discharge_disposition_map" %}
{% set primary_keys = ["facility_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}