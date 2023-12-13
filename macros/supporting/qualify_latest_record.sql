
{% macro qualify_latest_record(primary_key, alias) %}
  {# Takes an input primary key for a raw table and generate a qualify row_number() = 1 statement to get the latest record. #}
{% if alias %}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {{alias}}.{{ primary_key }} order by {{alias}}.src_update_dt desc) = 1
    {% else %}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} order by src_update_dt desc) = 1
    {% endif %}
{% endmacro %}