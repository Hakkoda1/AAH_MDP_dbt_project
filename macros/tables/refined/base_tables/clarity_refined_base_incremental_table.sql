{# Takes an input clarity table name, its primary key(s) and instance (i.e. aah, atrium) for a raw clarity table loaded to Snowflake and generates sql for the refined base layer. #}
{% macro clarity_refined_base_incremental_table(clarity_table_name, primary_keys) %}
{# set additional variables to be used based on the clarity table and primary key - this can be moved to a macro #}

{%- set except_columns = ["src_operation", "metadata_filename", "metadata_file_row_number"] -%}
{%- set raw_clarity_table = "raw_" ~ clarity_table_name -%}
{%- set raw_clarity_operation_history_table = "raw_" ~ clarity_table_name ~ "_src_operation_history" -%}
{%- set addtl_except_columns = ["src_update_dt", "_dbt_copied_at"] -%} 
{%- set except_columns_with_src_update_dt = except_columns + addtl_except_columns -%}
{%- set primary_key =  primary_keys|join(',') -%}
{%- set column_list_for_union_all = dbt_utils.get_filtered_columns_in_relation(from=source("raw_clarity_aah", raw_clarity_table|upper), except=except_columns_with_src_update_dt) -%}

with raw as (
    select 
        {{ dbt_utils.star(from=source("raw_clarity_aah", raw_clarity_table|upper), except=except_columns, quote_identifiers=False) }}
    from {{ source("raw_clarity_aah", raw_clarity_table|upper) }}
    
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where _dbt_copied_at > (select IFNULL(max(mdp_effective_datetime),'2023-01-01 00:00:00.000'::timestamp_ntz) from {{ this }})

    {% endif %}
    
    {{ qualify_latest_record(primary_key) }}


),
src_operation_history as (
    select *,
    iff(src_operation = 'DELETE', lag(src_update_dt) over (partition by {{primary_key}} order by src_update_dt), src_update_dt) src_update_dt_for_join
    from {{ ref(raw_clarity_operation_history_table) }}
    
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where mdp_load_datetime > (select IFNULL(max(mdp_effective_datetime),'2023-01-01 00:00:00.000'::timestamp_ntz) from {{ this }})

    {% endif %}

    {{ qualify_latest_record(primary_key) }}
)
select 
    {# source columns #}
    {{ dbt_utils.star(from=source("raw_clarity_aah", raw_clarity_table|upper), except=except_columns_with_src_update_dt, relation_alias='a', quote_identifiers=False) }},

    {# src_update_dt column - selected from operation_history because deletes will not exist in raw cte #}
    b.src_update_dt,

    {# metadata columns #}
    b.mdp_load_datetime as mdp_effective_datetime, 
    'CLARITY.dbo.{{clarity_table_name|upper}}' as record_source,
    iff(b.src_operation = 'DELETE', 'Y', 'N') deleted_yn
from raw a 
inner join src_operation_history b on {% for col in primary_keys %}a.{{col}} = b.{{col}}{%- if not loop.last %} and {% endif -%}{% endfor %} and a.src_update_dt = b.src_update_dt_for_join
{% if is_incremental() %}
where b.src_operation <> 'DELETE'
union all 
select 
    {# source columns #}
    {% for column in column_list_for_union_all %}
    {% if column|lower in primary_keys %}b.{% else %}a.{% endif %}{{ column }},
    {%- endfor -%}
    
    {# src_update_dt column - selected from operation_history because deletes will not exist in raw cte #}
    b.src_update_dt,

    {# metadata columns #}
    b.mdp_load_datetime as mdp_effective_datetime, 
    'CLARITY.dbo.{{clarity_table_name|upper}}' as record_source,
    iff(b.src_operation = 'DELETE', 'Y', 'N') deleted_yn
from src_operation_history b 
left join raw a on {% for col in primary_keys %}a.{{col}} = b.{{col}}{%- if not loop.last %} and {% endif -%}{% endfor %} and a.src_update_dt = b.src_update_dt_for_join
where b.src_operation = 'DELETE'
{% endif %}
{% endmacro %}