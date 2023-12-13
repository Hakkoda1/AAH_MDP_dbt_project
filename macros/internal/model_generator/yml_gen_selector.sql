-- depends_on: {{ ref('column_config') }}
-- yml_col_gen('Clarity_POC',['LIST of values to exclude'])
{% macro yml_col_gen_selector(table,model_yaml,pk_flag) %}

    
    --if primary_key_only
    {% if pk_flag %}
        {% set column_metadata_query %}
        select 
            table_name,
            column_name,
            case  --4
                when data_type is null 
                    then 'VARCHAR'
                when data_type = 'TIMESTAMP' || 'DATETIME' then
                    (case 
                        when hour_format = 'DATE ONLY' then --STILL TIMESTAMPS IN RAW, RECAST DOWNSTREAM
                            'TIMESTAMP'
                        when hour_format = 'DATETIME 24HR INCL SECONDS' then
                            'TIMESTAMP'
                        when hour_format = 'DATETIME 12HR' then
                            'TIMESTAMP'
                        when hour_format = 'TIME ONLY 24HR' then
                            'TIME'
                        else 
                            'TIMESTAMP' end)
                when data_type = 'NUMERIC' then
                    data_type || '(' || ifnull(clarity_precision, 38)::number || ',' || ifnull(clarity_scale, 37)::number  || ')'
                when data_type = 'VARCHAR' then
                    'VARCHAR'
                when clarity_precision is not null
                    then data_type || '(' || clarity_precision || ')'
                when clarity_precision is null
                    then data_type
                end as data_type_input
        from {{ref('column_config')}}
        where
            table_name = UPPER('{{table}}')
            and primary_key = true
        order by pk_line,line, column_name
        {% endset %}

    {% set results = run_query(column_metadata_query) %}
    
    {% else %}
        {% set column_metadata_query %}
        select 
            table_name,
            column_name,
            case  --4
                when data_type is null 
                    then 'VARCHAR'
                when data_type = 'TIMESTAMP' || 'DATETIME' then
                    (case 
                        when hour_format = 'DATE ONLY' then --STILL TIMESTAMPS IN RAW, RECAST DOWNSTREAM
                            'TIMESTAMP'
                        when hour_format = 'DATETIME 24HR INCL SECONDS' then
                            'TIMESTAMP'
                        when hour_format = 'DATETIME 12HR' then
                            'TIMESTAMP'
                        when hour_format = 'TIME ONLY 24HR' then
                            'TIME'
                        else 
                            'TIMESTAMP' end)
                when data_type = 'NUMERIC' then
                    data_type || '(' || ifnull(clarity_precision, 38)::number || ',' || ifnull(clarity_scale, 37)::number  || ')'
                when data_type = 'VARCHAR' then
                    'VARCHAR'
                when clarity_precision is not null
                    then data_type || '(' || clarity_precision || ')'
                when clarity_precision is null
                    then data_type
                end as data_type_input
        from {{ref('column_config')}}
        where
            table_name = UPPER('{{table}}')
        order by line, column_name
        {% endset %}

    {% set results = run_query(column_metadata_query) %}
    {%- endif -%}

    

    {% if execute %}
    {%- set column_list = results.columns[1].values() -%}
    {%- set data_type_list = results.columns[2].values() -%}
    {%- endif -%}

    {% do model_yaml.append('    columns:') %}
    {% for (column,data_type) in zip(column_list,data_type_list) %}
        {% do model_yaml.append('      - name: ' ~ column) %}
        {% do model_yaml.append('        data_type: ' ~ data_type) %}

    {%- endfor -%}

    --SRC_UPDATE_DT
    {% do model_yaml.append('      - name: SRC_OPERATION') %}
    {% do model_yaml.append('        data_type: VARCHAR') %}
    {% do model_yaml.append('      - name: SRC_UPDATE_DT') %}
    {% do model_yaml.append('        data_type: TIMESTAMP') %}
    {% do return(model_yaml) %}
{% endmacro %}


{% macro yml_gen_selector(include_list) %}
    {%set snowflake_notification_integration = var("snowflake_notification_integration") %}
    {% set model_yaml=[] %}

    {% do model_yaml.append('#dbt run-operation stage_external_sources #use only to create only new tables ') %}
    {% do model_yaml.append('#dbt run-operation stage_external_sources --vars "ext_full_refresh: true" #use to create all tables') %}
    {% do model_yaml.append('version: 2') %}
    {% do model_yaml.append('') %}
    {% do model_yaml.append('sources:') %}
    {% do model_yaml.append('- name: raw_clarity_aah') %}
    {% do model_yaml.append('  schema: clarity_aah') %}
    {% do model_yaml.append('  tables:') %}
    

        {# {%- for table in table_list -%} #}


        {% for x in include_list %}
            {% set table = x.table_name %}
            
            {% if x.type == 'incremental' %} 
                {% set incremental_flag = True %}
            {% else %} 
                {% set incremental_flag = False %}
            {% endif %}
                
                -- load full,incremental,backfill
                {% do model_yaml.append('  - name: RAW_' ~ table | upper) %}
                    {% do model_yaml.append('    external:' ~ description) %}
                    {% do model_yaml.append('      location: "@clarity_aah.parquet_stage/' ~ table ~ '"') %}
                    {% do model_yaml.append('      file_format: "clarity_aah.parquet_format"') %}
                    {# {% do model_yaml.append('      pattern: ".*/' ~ table | upper ~ '__.*[.]parquet"') %} #}
                    {% do model_yaml.append('      pattern: ".*/(backfill|full|incremental)/' ~ table | upper ~ '__.*[.]parquet"') %}
                    {% do model_yaml.append('') %}
                    {% do model_yaml.append('      snowpipe:') %}
                    {% do model_yaml.append('        auto_ingest: true') %}
                    {% do model_yaml.append('        integration: ' ~ snowflake_notification_integration) %}
                    {% do model_yaml.append('        copy_options: "on_error = continue, enforce_length = false"') %}
                    {% do model_yaml.append('') %}
                    {% set model_yaml = yml_col_gen_selector(table,model_yaml,false) %}
                
                {% if incremental_flag %} --need to create respective delete table
                    {% do model_yaml.append('  - name: RAW_' ~ table | upper ~ '_DELETE') %}
                    {% do model_yaml.append('    external:' ~ description) %}
                    {% do model_yaml.append('      location: "@clarity_aah.parquet_stage/' ~ table ~ '"') %}
                    {% do model_yaml.append('      file_format: "clarity_aah.parquet_format"') %}
                    {% do model_yaml.append('      pattern: ".*/delete/' ~ table | upper ~ '__.*[.]parquet"') %}
                    {% do model_yaml.append('') %}
                    {% do model_yaml.append('      snowpipe:') %}
                    {% do model_yaml.append('        auto_ingest: true') %}
                    {% do model_yaml.append('        integration: ' ~ snowflake_notification_integration) %}
                    {% do model_yaml.append('        copy_options: "on_error = continue, enforce_length = false"') %}
                    {% do model_yaml.append('') %}
                    {% set model_yaml = yml_col_gen_selector(table,model_yaml,true) %}
                {%- endif -%}

        {%- endfor -%}

        {% if execute %}

            {% set joined = model_yaml | join ('\n') %}
            {{ log(joined, info=True) }}
            {% do return(joined) %}

        {% endif %}
      

    --Loop through each table and fill in parameters
    /*
- name: raw_clarity_aah
  schema: clarity_aah
  description: "Loading raw clarity data files from azure"

  tables:
  - name: RAW_ZC_ARRIV_MEANS
    external:
      location: "@clarity_aah.parquet_stage"
      file_format: "clarity_aah.my_parquet_format"
      pattern: ".*ZC_ARRIV_MEANS__.*[.]parquet"

      snowpipe:
        auto_ingest: true
        integration: AZ_MDP_LANDING_NOTIFICATION
        copy_options: "on_error = continue, enforce_length = false"
      
    columns:
      - name: MEANS_OF_ARRV_C
        data_type: VARCHAR(66)
      - name: NAME
        data_type: VARCHAR(254)
      - name: TITLE
        data_type: VARCHAR(254)
      - name: ABBR
        data_type: VARCHAR(254)
      - name: INTERNAL_ID
        data_type: VARCHAR(66)


    */
{% endmacro %}