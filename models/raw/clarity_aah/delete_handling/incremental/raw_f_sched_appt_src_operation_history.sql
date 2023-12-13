{% set clarity_table_name = "f_sched_appt" %}
{% set primary_keys = ["pat_enc_csn_id"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}