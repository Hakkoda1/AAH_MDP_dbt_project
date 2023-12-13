{% macro create_stream_watermark(mode) %}
{%set raw_database =  var("raw_database")%}


    {% if mode == 'default' %}

        --IF REGULAR
        CREATE STREAM IF NOT EXISTS {{this}}_stream ON TABLE {{this}}

    {% elif mode == 'refresh' %}

        --FULL REFRESH
        CREATE OR REPLACE STREAM {{this}}_stream ON TABLE {{this}}

    {% elif mode == 'drop' %}

        --CLEAR
        DROP STREAM IF EXISTS {{this}}_stream

    {% endif %}

    

{% endmacro %}



--pass in paramter as which type of load we want to be done on it
--pass in paramter for the raw landing 