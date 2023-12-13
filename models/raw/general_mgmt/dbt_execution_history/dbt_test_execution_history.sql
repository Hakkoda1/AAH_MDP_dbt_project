with query_history as (
    select split_part(query_tag, '|', 1) invocation_id
    ,split_part(query_tag, '|', 2) dbt_model_name
    ,query_id 
    ,session_id
    ,query_text 
    ,query_type
    ,user_name 
    ,role_name
    ,warehouse_name
    ,warehouse_size 
    ,execution_status 
    ,error_message
    ,start_time
    ,end_time 
    ,total_elapsed_time 
    ,bytes_scanned 
    ,rows_produced
    ,partitions_scanned 
    ,partitions_total 
    ,bytes_spilled_to_local_storage
from {{ source('account_usage', 'query_history') }}
where 1=1
and split_part(query_tag, '|', 2) <> ''
)
select executions.*,
dim_tests.name,
dim_tests.depends_on_nodes,
dim_tests.test_path,
dim_tests.tags,
query_history.query_id,
query_history.session_id,
query_history.query_text,
query_history.query_type,
query_history.user_name,
query_history.role_name,
query_history.warehouse_name,
query_history.warehouse_size,
query_history.execution_status, 
query_history.error_message,
query_history.start_time,
query_history.end_time,
query_history.total_elapsed_time, 
query_history.bytes_scanned, 
query_history.rows_produced,
query_history.partitions_scanned, 
query_history.partitions_total,
query_history.bytes_spilled_to_local_storage
from {{ ref('fct_dbt__test_executions') }} executions
inner join {{ref('dim_dbt__tests')}} dim_tests 
    on executions.test_execution_id = dim_tests.test_execution_id
    and executions.command_invocation_id = dim_tests.command_invocation_id
left join query_history 
    on executions.command_invocation_id = query_history.invocation_id 
    and dim_tests.name = query_history.dbt_model_name
order by executions.run_started_at desc