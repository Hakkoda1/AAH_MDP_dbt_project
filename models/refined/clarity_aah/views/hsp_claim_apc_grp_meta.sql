
    {% set clarity_table_name = "hsp_claim_apc_grp_meta" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

