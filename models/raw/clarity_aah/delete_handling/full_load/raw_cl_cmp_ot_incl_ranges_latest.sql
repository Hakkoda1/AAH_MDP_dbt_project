
    {%- set source_model_name = "cl_cmp_ot_incl_ranges" -%}
    {%- set instance_name = "clarity_aah" -%}

    {{ clarity_full_load_delete_handling(source_model_name=source_model_name,instance_name=instance_name) }}
    

