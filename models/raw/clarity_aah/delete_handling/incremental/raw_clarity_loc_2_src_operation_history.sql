{% set clarity_table_name = "clarity_loc_2" %}
{% set primary_keys = ["loc_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}