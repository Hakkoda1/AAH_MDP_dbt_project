
    {%- set source_model_name = "zc_brand_name_code" -%}
    {%- set instance_name = "clarity_aah" -%}

    {{ clarity_full_load_delete_handling(source_model_name=source_model_name,instance_name=instance_name) }}
    

