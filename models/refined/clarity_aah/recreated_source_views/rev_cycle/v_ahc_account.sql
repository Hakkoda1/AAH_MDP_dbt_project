with

account as (
    select * from {{ ref('account_base') }}
)


select *

from 

account

where 

serv_area_id in (1, 10, 16, 20,800)

