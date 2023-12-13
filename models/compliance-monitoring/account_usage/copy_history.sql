{{
    config(
        materialized='incremental'
    )
}}

select
*
from
snowflake.account_usage.copy_history

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where date_trunc(day, last_load_time) > (select max(date_trunc(day, last_load_time)) from {{ this }})

{% endif %}
