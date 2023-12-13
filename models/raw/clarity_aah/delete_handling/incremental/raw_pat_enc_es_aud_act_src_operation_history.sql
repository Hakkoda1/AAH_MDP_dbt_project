{% set clarity_table_name = "pat_enc_es_aud_act" %}
{% set primary_keys = ["pat_enc_csn_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}