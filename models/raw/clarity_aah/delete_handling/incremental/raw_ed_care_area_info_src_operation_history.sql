{% set clarity_table_name = "ed_care_area_info" %}
{% set primary_keys = ["care_area_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}