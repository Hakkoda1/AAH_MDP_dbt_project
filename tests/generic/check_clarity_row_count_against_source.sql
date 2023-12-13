-- depends_on: {{ ref('source_table_size_latest') }}
{% test check_clarity_row_count_against_source(model) %}

{{ config(
    fail_calc = "row_count_test_severity_check",
    severity = "error",
    error_if = "=1",
    warn_if = ">1"
) }}

{% set row_count_validation_query %}

    with source_row_count as 
    (select total_row_count
    from {{ ref('source_table_size_latest') }}
    where table_name = replace( split_part(upper('{{ model }}'), '.',3) , 'LKP_CLR' , 'ZC' ) 
                OR table_name = split_part(upper('{{ model }}'), '.',3)), 
                
    clarity_row_count as 
    (select count(*) row_count
    from {{ model }}), 
    
    row_count_validation as 
    (select a.row_count - b.total_row_count as diff
    from clarity_row_count a
    inner join source_row_count b on 1=1)

    select diff from row_count_validation

{% endset %}
    
{% if execute %}
{%- set results = run_query(row_count_validation_query) -%}
{%- set diff = results.columns[0].values()[0] -%}

    {% if diff == 0 %}
        select 0 as row_count_test_severity_check
    {% else %}

        with source_row_date as (
            select as_of_time as date_clarity_as_of_time
            from {{ ref('source_table_size_latest') }}
            where table_name = replace( split_part(upper('{{ model }}'), '.',3) , 'LKP_CLR' , 'ZC' ) 
                OR table_name = split_part(upper('{{ model }}'), '.',3)
        )
        , table_status_validation as (
            select is_enabled, is_backfill_complete
            from {{ source('general_mgmt', 'source_table_config') }}
            where source_key = 1 and (table_name = replace( split_part(upper('{{ model }}'), '.',3) , 'LKP_CLR' , 'ZC' ) 
                    OR table_name = split_part(upper('{{ model }}'), '.',3))
        )
        , date_model as (
            select max(src_update_dt) as date_update_model,  max(mdp_effective_datetime) as date_dbt_mdp_load_datetime
            from {{ model }}
        )
        , validation_errors as (
            select
            case 
                when is_backfill_complete = TRUE and is_enabled = TRUE and (date_dbt_mdp_load_datetime <= date_clarity_as_of_time) then 1 --Backfill complete and table enabled, error
                when is_backfill_complete = TRUE and is_enabled = TRUE and (date_dbt_mdp_load_datetime > date_clarity_as_of_time) then 2 --Clarity Row Count check not yet updated, warn
                when is_backfill_complete = FALSE and (date_dbt_mdp_load_datetime <= date_clarity_as_of_time) then 3 --Backfill not complete, warn
                when is_enabled = FALSE and (date_dbt_mdp_load_datetime <= date_clarity_as_of_time) then 4 --Table not enabled, warn
                else 99 --Other cases, warn
            end as row_count_test_severity_check
            from table_status_validation 
            inner join source_row_date on 1=1
            inner join date_model on 1=1
        )
        select * from validation_errors

    {% endif %}
{% endif %}
    

{% endtest %}

