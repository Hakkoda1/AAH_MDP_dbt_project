{% macro get_json_path(model,json_column) %}

{# /* get json keys and paths with the FLATTEN function supported by Snowflake */ #}
with low_level_flatten as (
	select f.key as json_key, f.path as json_path, 
	f.value as json_value
	from {{ model }}, 
	lateral flatten(input => {{ json_column }}, recursive => true ) f
)

	{# /* get the unique and flattest paths  */ #}
	select distinct json_path
	from low_level_flatten
	where not contains(json_value, '{')

{% endmacro %}