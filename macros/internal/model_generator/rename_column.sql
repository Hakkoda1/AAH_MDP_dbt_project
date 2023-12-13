{% macro rename_column(column) %}

{%-set replace_underscore = var("replace_underscore") -%}
{%-set add_suffix = var("add_suffix") -%}
{%-set suffix = var("suffix")-%}
{%-set add_prefix =  var("add_prefix")-%}
{%-set prefix =  var("prefix")-%}


{%- if add_prefix == true -%}
    {%- set column = prefix + column -%}
{%- endif -%}

{%- if add_suffix == true -%}
    {% set column = column + suffix -%}
{%- endif -%}

{%- if replace_underscore == true -%}
    {% set column = column|replace("_", "__") %}
{%- endif -%}


{{- return(column) -}}

{% endmacro %}