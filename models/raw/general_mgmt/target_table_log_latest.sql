{{ config(
    materialized='view'
    ) 
}}

select *
from {{ source('high_watermark', 'target_table_log') }} t
qualify row_number() over (partition by target_table order by log_datetime desc) = 1