{% macro views_row_count_query() %}
    {%set refined_database =  var("refined_database")%}

    {% set query %}
            with column_cte as(
            select * from {{refined_database}}.information_schema.columns
            where column_name = 'SRC_UPDATE_DT'
            )

            select listagg(xx, ' union all ')
        from (
            select 
                case 
                    when src_update_dt_flag is not null then 'select \'' || x || '\' view, count(*) row_count, max(src_update_dt) as max_src_update_dt from ' || x || '\n'
                    else 'select \'' || x || '\' view, count(*) row_count, null as max_src_update_dt from ' || x || '\n' end as xx
            from  (
                select t1.TABLE_CATALOG ||'.'|| t1.TABLE_SCHEMA ||'."'||t1.TABLE_NAME||'"' x, t2.column_name as src_update_dt_flag
                from {{refined_database}}.INFORMATION_SCHEMA.VIEWS t1
                left join column_cte t2 on t1.table_name = t2.table_name
                where t1.TABLE_SCHEMA = 'CLARITY_AAH'
                
            )
        )
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set sql_code =  results.columns[0].values()[0] %}
        {{ sql_code }} 

    {%- endif -%}



{% endmacro %}