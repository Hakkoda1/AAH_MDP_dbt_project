
    {% set clarity_table_name = "hsp_exec_summary" %}
    {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}

    {{
        config(
            materialized='incremental',
            unique_key=['bkt_type_ha_c','contact_date','loc_id','fin_class_c','prod_type_c','benefit_plan_id','acct_base_cls_ha_c','payor_id','serv_area_id','deployment_id']
        )
    }}

    {{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
    

