select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    to_date(start_time) as start_date,
    datediff(hour, start_time, end_time) as warehouse_operation_hours,
    to_time(start_time) as time_of_day
from {{ source("account_usage", "warehouse_metering_history") }}
order by to_date(start_time) desc

