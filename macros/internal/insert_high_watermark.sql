{% macro insert_high_watermark(layer) %}
{%set raw_database =  var("raw_database")%}

    {% if layer == 'raw' %}
        insert into {{raw_database}}.general_mgmt.target_table_log
    select
        split_part('{{this}}', '.', 1),
        split_part('{{this}}', '.', 2),
        split_part('{{this}}', '.', 3),
        max(src_update_dt),
        null,
        current_timestamp
    from {{this}}

    {% elif layer == 'refined' %}
        insert into {{raw_database}}.general_mgmt.target_table_log
        select 
            split_part('{{this}}', '.', 1),
            split_part('{{this}}', '.', 2),
            split_part('{{this}}', '.', 3),
            max(src_update_dt),
            max(mdp_effective_datetime),
            current_timestamp
        from {{this}}

    {% elif layer == 'conformed' %}
        insert into {{raw_database}}.general_mgmt.target_table_log
        select 
            split_part('{{this}}', '.', 1),
            split_part('{{this}}', '.', 2),
            split_part('{{this}}', '.', 3),
            null,
            max(mdp_effective_date),
            current_timestamp
        from {{this}}
    {% endif %}
{% endmacro %}
