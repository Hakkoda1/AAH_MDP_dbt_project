{# Takes an input source model and instance (i.e. aah, atrium) for a raw clarity table loaded to Snowflake via full load and generates sql to get the latest records. #}
{% macro clarity_full_load_delete_handling(source_model_name, instance_name) %}
{%- set source_model = source_model_name | upper -%}
{%- set raw_source_model = 'RAW_' ~ source_model -%}
{%- set raw_instance_name = 'raw_' ~ instance_name -%}
select *
from {{ source(raw_instance_name, raw_source_model) }}
where src_update_dt in (
    select max(src_update_dt) src_update_dt
    from {{ source(raw_instance_name, raw_source_model) }}
)
{% endmacro %}