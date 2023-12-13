{% macro test_default_key (fact_table, fact_table_key, dim_table, dim_table_key ) %}
    {# Takes both fact and dim tables, an acording to their respective keys verifies that all rows have their corresponding join record #}
    with

        full_table as (
            select * from {{ ref( fact_table ) }}
        ),

        defaults_in_fact as (
            select *
            from {{ ref( fact_table ) }}
            where {{ fact_table_key }} = '6bb61e3b7bce0931da574d19d1d82c88'
        ),

        non_defaults_in_fact as (
            select *
            from {{ ref( fact_table ) }}
            where {{ fact_table_key }} <> '6bb61e3b7bce0931da574d19d1d82c88'
        ),

        join_fact_dim as (
            select * 
            from {{ ref( fact_table ) }} fact
                inner join {{ ref( dim_table ) }} dim 
                    on dim.{{ dim_table_key }} = fact.{{ fact_table_key }}
        ),

        records_not_in_join as (
        select *
        from {{ ref( fact_table ) }} fact
        left outer join {{ ref( dim_table ) }} dim
            on dim.{{ dim_table_key }} = fact.{{ fact_table_key }}
        where dim.{{ dim_table_key }} is null
        )

    select
        (select count(*) from full_table) as all_rows_in_fact, -- count of all rows in the fact table
        (select count(*) from defaults_in_fact) as all_defaults_in_fact, -- count of al defaults in the selected key
        (select count(*) from non_defaults_in_fact) as non_defaults_in_fact, -- count of al defaults in the selected key
        (select count(*) from join_fact_dim) as all_joins, -- count of al rows after the inner join using the selected keys for dim and fact
        (select count(*) from records_not_in_join) as not_joined -- count of rows not joined 

{% endmacro %}