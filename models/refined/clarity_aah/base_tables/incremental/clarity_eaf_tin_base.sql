
    {% set clarity_table_name = "clarity_eaf_tin" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{
        config(
            materialized='incremental',
            unique_key=['line','facility_id']
        )
    }}

    {{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
    

