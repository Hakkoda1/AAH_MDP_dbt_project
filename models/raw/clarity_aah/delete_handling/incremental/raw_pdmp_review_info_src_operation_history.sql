{% set clarity_table_name = 'pdmp_review_info' %} 
{% set primary_keys = table_primary_key_extraction(clarity_table_name) %} 
{{ clarity_src_operation_history_table(clarity_table_name, primary_keys) }}
