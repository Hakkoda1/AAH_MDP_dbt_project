{{
    config(
        materialized='incremental',
        unique_key= ['proc_id', 'contact_date_real']
    )
}}

{% set clarity_table_name = "clarity_eap_ot" %}
{% set primary_keys = ['proc_id', 'contact_date_real'] %}

{{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}

{#
-- CODE SHOULD BE
-- {% set clarity_table_name = "clarity_eap_ot" %}
-- {% set primary_keys = table_primary_key_extraction(clarity_table_name) %}
-- {{
--     config(
--         materialized='incremental',
--         unique_key=['proc_id', 'contact_date_real']
--     )
-- }}


-- {{ clarity_refined_base_incremental_table( clarity_table_name, primary_keys ) }}
#}