
{% set clarity_table_name = "lkp_clr_epm_rpt_grp_14" %}

{% set except_columns = [] %}

{{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}


