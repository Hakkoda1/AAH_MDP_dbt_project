
    {% set clarity_table_name = "arpb_tx_actions" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}
    
