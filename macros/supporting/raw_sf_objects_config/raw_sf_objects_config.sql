{% macro raw_sf_objects_config(rebuild_flag) %}
--This macro will loop through the schema list and generate schema if doesnt not exist, file format, and stages

{%set schema_list =  var("raw_schema_list")%}
{%set raw_database =  var("raw_database")%}
{%set snowflake_storage_integration =  var("snowflake_storage_integration")%}
{%set azure_blob_storage_url =  var("azure_blob_storage_url")%}

    {% for schema in schema_list %}
        {{ create_raw_schema_objects(rebuild_flag,raw_database,schema,snowflake_storage_integration,azure_blob_storage_url) }}
    {%- endfor -%}

{% endmacro %}


--dbt run-operation raw_sf_objects_config