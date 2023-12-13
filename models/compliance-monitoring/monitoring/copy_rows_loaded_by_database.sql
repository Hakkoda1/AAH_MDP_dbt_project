{{
    config(
        materialized='incremental',
        unique_key= ['MDP_Database','load_day']
    )
}}

with tmp as(
select
sum(ROW_COUNT) as rows_loaded,
TABLE_CATALOG_NAME
from
{{ ref('copy_history') }}
--mdp_audit_monitoring.account_usage.copy_history
where
last_load_time >= DATEADD(day, -30, current_date())
group by
TABLE_CATALOG_NAME
order by
rows_loaded desc
) ,
data as (
select ifnull(tmp.TABLE_CATALOG_NAME, 'Other') MDP_Database,
date_trunc(day, last_load_time) load_day,
count(*) as total_copy_jobs,
sum(
case when error_count > 0 then 1 else 0 end
) as error_jobs,
sum(ROW_COUNT) as rows_loaded,
sum(file_size) / power(1024, 2) as total_MB_loaded,
sum(file_size) / power(1024, 3) as total_GB
from
{{ ref('copy_history') }} c
--mdp_audit_monitoring.account_usage.copy_history c
left join tmp on c.TABLE_CATALOG_NAME = tmp.TABLE_CATALOG_NAME
where
last_load_time >= DATEADD(day, -30, current_date())
group by 1, 2 )

select
*
from
data
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where load_day >= (select max(load_day) from {{ this }})

{% endif %}

--order by 1, 2;