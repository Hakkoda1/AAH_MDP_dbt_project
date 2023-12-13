{%- set source_model_name = 'zc_smoking_tob_use' -%}
{%- set instance_name = 'clarity_aah' -%}

{{ clarity_full_load_delete_handling(source_model_name=source_model_name,instance_name=instance_name) }}
