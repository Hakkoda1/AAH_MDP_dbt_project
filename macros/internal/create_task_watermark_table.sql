{% macro create_task_watermark_table(mode,layer) %}
{%set raw_database =  var("raw_database")%}

{% if target.name == 'dev' %}
    {%set warehouse = 'MDP_DEVELOPMENT_WH'%}
{% elif target.name == 'test' %}
    {%set warehouse = 'MDP_TRANSFORMATIONS_TEST_WH'%}
{% elif target.name == 'uat' %}
    {%set warehouse = 'MDP_TRANSFORMATIONS_UAT_WH'%}
{% elif target.name == 'prod' %}
    {%set warehouse = 'MDP_TRANSFORMATIONS_PROD_WH'%}
{% endif %}

    {% if mode == 'default' %}
        
        --create if DNE
        
        {% if layer == 'raw' %}

            create task if not exists {{this}}_watermark_load
            warehouse = {{warehouse}}
            schedule = '2 minute'
            WHEN 
            SYSTEM$STREAM_HAS_DATA('{{this}}_stream') 
            as
                insert into {{raw_database}}.general_mgmt.DBT_MODEL_HIGH_WATERMARK_LOG
                    select
                        split_part('{{this}}', '.', 1),
                        split_part('{{this}}', '.', 2),
                        split_part('{{this}}', '.', 3),
                        max(src_update_dt),
                        null,
                        current_timestamp,
                        invocation_id
                from {{this}}_stream
                left join {{raw_database}}.GENERAL_MGMT.CURRENT_INVOCATION_ID
                group by all

        {% elif layer == 'refined' %}

            create task if not exists {{this}}_watermark_load
            warehouse = {{warehouse}}
            schedule = '2 minute'
            WHEN 
            SYSTEM$STREAM_HAS_DATA('{{this}}_stream') 
            as
                insert into {{raw_database}}.general_mgmt.DBT_MODEL_HIGH_WATERMARK_LOG
                    select 
                        split_part('{{this}}', '.', 1),
                        split_part('{{this}}', '.', 2),
                        split_part('{{this}}', '.', 3),
                        max(src_update_dt),
                        max(mdp_effective_datetime),
                        current_timestamp,
                        invocation_id
                from {{this}}_stream
                left join {{raw_database}}.GENERAL_MGMT.CURRENT_INVOCATION_ID
                group by all


        {% elif layer == 'conformed' %}

            create task if not exists {{this}}_watermark_load
            warehouse = {{warehouse}}
            schedule = '2 minute'
            WHEN 
            SYSTEM$STREAM_HAS_DATA('{{this}}_stream') 
            as
                insert into {{raw_database}}.general_mgmt.DBT_MODEL_HIGH_WATERMARK_LOG
                    select 
                        split_part('{{this}}', '.', 1),
                        split_part('{{this}}', '.', 2),
                        split_part('{{this}}', '.', 3),
                        null,
                        max(mdp_effective_date),
                        current_timestamp,
                        invocation_id
                from {{this}}_stream
                left join {{raw_database}}.GENERAL_MGMT.CURRENT_INVOCATION_ID
                group by all

        {% endif %}
        

    {% elif mode == 'refresh' %}

        --create or replace
        {% if layer == 'raw' %}

            create or replace task {{this}}_watermark_load
            warehouse = {{warehouse}}
            schedule = '2 minute'
            WHEN 
            SYSTEM$STREAM_HAS_DATA('{{this}}_stream') 
            as
                insert into {{raw_database}}.general_mgmt.DBT_MODEL_HIGH_WATERMARK_LOG
                    select
                        split_part('{{this}}', '.', 1),
                        split_part('{{this}}', '.', 2),
                        split_part('{{this}}', '.', 3),
                        max(src_update_dt),
                        null,
                        current_timestamp
                        invocation_id
                from {{this}}_stream
                left join {{raw_database}}.GENERAL_MGMT.CURRENT_INVOCATION_ID
                group by all

        {% elif layer == 'refined' %}

            create or replace task {{this}}_watermark_load
            warehouse = {{warehouse}}
            schedule = '2 minute'
            WHEN 
            SYSTEM$STREAM_HAS_DATA('{{this}}_stream') 
            as
                insert into {{raw_database}}.general_mgmt.DBT_MODEL_HIGH_WATERMARK_LOG
                    select 
                        split_part('{{this}}', '.', 1),
                        split_part('{{this}}', '.', 2),
                        split_part('{{this}}', '.', 3),
                        max(src_update_dt),
                        max(mdp_effective_datetime),
                        current_timestamp,
                        invocation_id
                from {{this}}_stream
                left join {{raw_database}}.GENERAL_MGMT.CURRENT_INVOCATION_ID
                group by all


        {% elif layer == 'conformed' %}

            create or replace task {{this}}_watermark_load
            warehouse = {{warehouse}}
            schedule = '2 minute'
            WHEN 
            SYSTEM$STREAM_HAS_DATA('{{this}}_stream') 
            as
                insert into {{raw_database}}.general_mgmt.DBT_MODEL_HIGH_WATERMARK_LOG
                    select 
                        split_part('{{this}}', '.', 1),
                        split_part('{{this}}', '.', 2),
                        split_part('{{this}}', '.', 3),
                        null,
                        max(mdp_effective_date),
                        current_timestamp,
                        invocation_id
                from {{this}}_stream
                left join {{raw_database}}.GENERAL_MGMT.CURRENT_INVOCATION_ID
                group by all

        {% endif %}

    {% elif mode == 'drop' %}

        --drop task
        drop task if exists {{this}}_watermark_load

    {% endif %}


{% endmacro %}
