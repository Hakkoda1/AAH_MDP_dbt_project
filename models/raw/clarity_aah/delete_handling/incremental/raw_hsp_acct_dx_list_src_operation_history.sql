{% set clarity_table_name = "hsp_acct_dx_list" %}
{% set primary_keys = ["hsp_account_id", "line"] %}

{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}