
    {% set clarity_table_name = "cl_rmt_svc_dat_inf" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}
    

