{# Takes an input clarity table name and columns that should be removed from the refined layer for a given clarity table and generates sql for the refined layer. #}
{% macro clarity_refined_base_full_load_table(clarity_table_name, except_columns = []) %}
{% set default_except_column = ["src_operation", "metadata_filename", "metadata_file_row_number", "_dbt_copied_at"] %}
{% set except_columns_with_additions = except_columns + default_except_column %}
select 
    {# source columns #}
    {{ dbt_utils.star(from=ref("raw_" ~ clarity_table_name ~ "_latest"), except=except_columns_with_additions, quote_identifiers=False) }},

    {# metadata columns #}
    CURRENT_TIMESTAMP as mdp_effective_datetime,
    'CLARITY.dbo.{{clarity_table_name|upper}}' as record_source,
    'N' as deleted_yn
    
from {{ ref("raw_" ~ clarity_table_name ~ "_latest") }} 
{% endmacro %}