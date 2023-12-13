{# Takes an input clarity table name, its primary key(s) and instance (i.e. aah, atrium) for a raw clarity table loaded to Snowflake and generates sql for the src operation history table. #}
{% macro clarity_src_operation_history_table(clarity_table_name, primary_keys) %}
{# set additional variables to be used based on the clarity table and primary key - this can be moved to a macro #}
{% set raw_clarity_table = "raw_" ~ clarity_table_name %}

with raw_incremental_and_deletes as (
    select {% for col in primary_keys %}
        {{col}},{% endfor %}
        src_operation,
        src_update_dt,
        _dbt_copied_at mdp_load_datetime
    from {{ source("raw_clarity_aah", raw_clarity_table|upper) }}

    {% if is_incremental() %}
    
        -- this filter will only be applied on an incremental run
        where _dbt_copied_at > (select IFNULL(max(mdp_load_datetime),'2023-01-01 00:00:00.000'::timestamp_ntz) from {{ this }})
    
    {% endif %}

    union all
    
    select {% for col in primary_keys %}
        {{col}},{% endfor %}
        src_operation,
        src_update_dt,
        _dbt_copied_at mdp_load_datetime
    from {{ source("raw_clarity_aah", raw_clarity_table|upper ~ "_DELETE") }}
    
    {% if is_incremental() %}
    
        -- this filter will only be applied on an incremental run
        where _dbt_copied_at > (select IFNULL(max(mdp_load_datetime),'2023-01-01 00:00:00.000'::timestamp_ntz) from {{ this }})
    
    {% endif %}
)

select
    *
from raw_incremental_and_deletes
{% endmacro %}