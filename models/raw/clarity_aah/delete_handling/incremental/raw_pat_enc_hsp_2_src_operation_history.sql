{% set clarity_table_name = "pat_enc_hsp_2" %}
{% set primary_keys = ["pat_enc_csn_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}