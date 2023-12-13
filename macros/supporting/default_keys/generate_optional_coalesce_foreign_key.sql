{% macro generate_optional_coalesce_foreign_key(column_names) %}

    {# Takes a list of column names and verify if the column has a null value,
        if it does returns the hash value of '-1',
        if not returns the hash value of the column
    #}

    {% set foreign_keys = [] %}  -- Initialize an empty list to store the foreign keys
    {% set counter = 0 %}

    {% for column in column_names %}
        {% set foreign_key = generate_optional_foreign_key( column ) %}  -- Generate foreign key using your generate_optional_foreign_key macro
        {% set _ = foreign_keys.append(foreign_key) %}  -- Add the foreign key to the list
        {% set counter = counter + 1 %}
    {% endfor %}

    case
        {% for column in column_names %}
            when {{ column }} is not null then {{ foreign_keys[loop.index0] }}  -- check each column for null values and return the corresponding foreign key
        {% endfor %}
            else {{ foreign_keys[counter - 1] }}  -- If all columns are null, return the foreign key of the last column
    end

{% endmacro %}
