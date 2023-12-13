{% set clarity_table_name = "clarity_adt" %}
{% set primary_keys = ["event_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}