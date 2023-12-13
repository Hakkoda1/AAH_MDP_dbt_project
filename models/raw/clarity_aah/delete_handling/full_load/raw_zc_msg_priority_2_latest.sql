{{ config(
    tags=["not_enabled_uat"]
) }}
{%- set source_model_name = 'zc_msg_priority_2' -%}
{%- set instance_name = 'clarity_aah' -%}

{{ clarity_full_load_delete_handling(source_model_name=source_model_name,instance_name=instance_name) }}
