{% macro create_raw_schema_objects(rebuild_flag,raw_database,schema,snowflake_storage_integration,azure_blob_storage_url) %}
    --HELPER MACRO
    --creates schema if not exists, file format, and stages



{%- if rebuild_flag -%}

    {#
        creates schema
        {% set create_schema %}
            create or replace schema {{raw_database}}.{{schema}}
            ;
        {% endset %}

        {% do run_query(create_schema) %}
    #}
    --creates file format
    {% set create_file_format %}
            create or replace file format {{raw_database}}.{{schema}}.parquet_format
                type = parquet
            ;
        {% endset %}

        {% do run_query(create_file_format) %}

        --creates stage
        {% set create_stage %}
        create or replace stage {{raw_database}}.{{schema}}.parquet_stage
                storage_integration = {{snowflake_storage_integration}}
                url = '{{azure_blob_storage_url}}'
                file_format = {{raw_database}}.{{schema}}.parquet_format
                ;

        {% endset %}

        {% do run_query(create_stage) %}
        
        --creates high watermark logging table
        {% set create_high_watermark_table %}
        create or replace table {{raw_database}}.general_mgmt.target_table_log
            (
            target_database varchar,
            target_schema varchar,
            target_table varchar,
            max_src_update_dt timestamp,
            max_mdp_load_datetime timestamp,
            log_datetime timestamp
            )
            ;

    {% endset %}

    {% do run_query(create_high_watermark_table) %}

{%- else -%}

    {#
    creates schema
    {% set create_schema %}
        create schema if not exists {{raw_database}}.{{schema}}
        ;
    {% endset %}

    {% do run_query(create_schema) %}
    #}


    {% set create_file_format %}
        create file format if not exists {{raw_database}}.{{schema}}.parquet_format
            type = parquet
        ;
    {% endset %}

    {% do run_query(create_file_format) %}

    --creates stage
    {% set create_stage %}
       create stage if not exists {{raw_database}}.{{schema}}.parquet_stage
            storage_integration = {{snowflake_storage_integration}}
            url = '{{azure_blob_storage_url}}'
            file_format = {{raw_database}}.{{schema}}.parquet_format
            ;

    {% endset %}

    {% do run_query(create_stage) %}

    --creates high watermark logging table
    {% set create_high_watermark_table %}
       create table if not exists {{raw_database}}.general_mgmt.target_table_log
            (
            target_database varchar,
            target_schema varchar,
            target_table varchar,
            max_src_update_dt timestamp,
            max_mdp_load_datetime timestamp,
            log_datetime timestamp
            )
            ;

    {% endset %}

    {% do run_query(create_high_watermark_table) %}

{%- endif -%}

{% endmacro %}


--dbt run-opereation snowpipe_buck_config --args '{schema: SCHEMA}'