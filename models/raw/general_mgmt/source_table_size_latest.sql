{{ config(
    materialized='view'
    ) 
}}

select sts.table_name, sts.total_row_count, sts.as_of_time
from {{ source('general_mgmt', 'source_table_size') }} sts
join {{ source('general_mgmt', 'source_table') }} st 
    on sts.table_key = st.table_key and st.source_key = 1
qualify row_number() over (partition by sts.table_key order by as_of_time desc) = 1
