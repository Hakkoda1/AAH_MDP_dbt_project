
    {% set clarity_table_name = "eaf_inst_guar_types" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{
        config(
            materialized='incremental',
            unique_key=['line','serv_area_id']
        )
    }}

    {{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
    

