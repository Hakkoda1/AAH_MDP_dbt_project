
    {% set clarity_table_name = "transplant_info" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{
        config(
            materialized='incremental',
            unique_key=['summary_block_id']
        )
    }}

    {{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
    

