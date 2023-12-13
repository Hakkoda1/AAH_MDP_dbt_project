{% set clarity_table_name = "clarity_ser_spec" %}
{% set primary_keys = ["prov_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name=clarity_table_name,primary_keys=primary_keys) }}