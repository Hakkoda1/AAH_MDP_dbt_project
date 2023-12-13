{% set clarity_table_name = "f_img_study" %}
{% set primary_keys = ["order_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}