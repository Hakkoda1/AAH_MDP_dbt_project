{% set clarity_table_name = "pat_enc_3" %}
{% set primary_keys = ["pat_enc_csn"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}