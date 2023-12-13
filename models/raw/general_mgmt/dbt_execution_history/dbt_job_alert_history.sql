{{
    config(
        materialized='incremental',
        transient=false,
        post_hook="{{ create_stream_on_dbt_job_alert_history() }}"
    )
}}

select 
    case when invocation_status = 'dbt test warn' then concat('MDP dbt Alert: dbt Cloud job ', dbt_cloud_job_name ,' has completed with test warnings.')
    else concat('MDP dbt Alert: dbt Cloud job ', ifnull(dbt_cloud_job_name,'') ,' has failed.') end email_summary,
object_construct(
      'command_invocation_id'
      ,command_invocation_id
      ,'dbt_version'
      ,dbt_version
      ,'run_started_at'
      ,run_started_at
      ,'dbt_cloud_job_id'
      ,hist.dbt_cloud_job_id
      ,'dbt_cloud_job_name'
      ,dbt_cloud_job_name
      ,'dbt_cloud_job_run_url'
      ,concat('https://cloud.getdbt.com/deploy/143150/projects/', dbt_cloud_project_id, '/runs/', dbt_cloud_run_id)
      ,'full_refresh_flag'
      ,full_refresh_flag
      ,'target_name'
      ,target_name
      ,'target_threads'
      ,target_threads
      ,'dbt_cloud_run_reason'
      ,dbt_cloud_run_reason
      ,'invocation_status'
      ,invocation_status
      ,'number_of_models'
      ,number_of_models
      ,'number_of_failed_models'
      ,number_of_failed_models
      ,'number_of_tests'
      ,number_of_tests
      ,'number_of_tests_failed'
      ,number_of_tests_failed
      ,'number_of_tests_warned'
      ,number_of_tests_warned
    ) email_description,
    run_started_at
from {{ref('dbt_job_execution_history')}} hist 
left join {{ref('dbt_cloud_job_names')}} names on hist.dbt_cloud_job_id = names.dbt_cloud_job_id
where dbt_cloud_run_reason <> 'azure_pull_request'
and invocation_status <> 'success'
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  and run_started_at > (select max(this.run_started_at) from {{ this }} as this)

{% endif %}