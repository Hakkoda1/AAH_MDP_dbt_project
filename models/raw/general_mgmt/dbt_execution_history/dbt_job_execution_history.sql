-- depends_on: [ {{ ref('adt_event_fact') }}, {{ ref('adt_pended_event_fact') }}, {{ ref('date_dim') }}, {{ ref('emergency_encounter_fact') }}, {{ ref('encounter_fact') }}, {{ ref('evs_event_fact') }}, {{ ref('inpatient_encounter_fact') }}, {{ ref('location_dim') }}, {{ ref('outpatient_encounter_fact') }},{{ ref('patient_dim') }}, {{ ref('provider_dim') }}]
with invocation as (
    select *
    from {{ ref('fct_dbt__invocations') }}
),
models as (
    select invocation.command_invocation_id,
    count(models.node_id) as number_of_models,
    count_if(models.status = 'success') as number_of_successful_models,
    count_if(models.status <> 'success') as number_of_failed_models
    from invocation
    inner join {{ ref('dbt_model_execution_history') }} models on invocation.command_invocation_id = models.command_invocation_id
    group by invocation.command_invocation_id
),
tests as (
    select invocation.command_invocation_id,
    count(tests.node_id) as number_of_tests,
    count_if(tests.status = 'pass') as number_of_tests_passed,
    count_if(tests.status = 'fail') as number_of_tests_failed,
    count_if(tests.status = 'warn') as number_of_tests_warned
    from invocation
    inner join {{ ref('dbt_test_execution_history') }} tests on invocation.command_invocation_id = tests.command_invocation_id
    group by invocation.command_invocation_id
)
select invocation.command_invocation_id,
invocation.dbt_version,
invocation.project_name,
invocation.run_started_at,
invocation.dbt_command,
invocation.full_refresh_flag,
invocation.target_name,
invocation.target_threads,
invocation.dbt_cloud_project_id,
invocation.dbt_cloud_job_id,
invocation.dbt_cloud_run_id,
invocation.dbt_cloud_run_reason_category,
invocation.dbt_cloud_run_reason,
invocation.env_vars,
invocation.dbt_vars,
invocation.invocation_args,
invocation.dbt_custom_envs,
models.number_of_models,
models.number_of_successful_models,
models.number_of_failed_models,
tests.number_of_tests,
tests.number_of_tests_passed,
tests.number_of_tests_failed,
tests.number_of_tests_warned,
case 
    when number_of_failed_models > 0 and number_of_tests_failed > 0 then 'dbt run fail'
    when number_of_failed_models = 0 and number_of_tests_failed > 0 then 'dbt test fail'
    when number_of_failed_models = 0 and number_of_tests_failed = 0 and number_of_tests_warned > 0 then 'dbt test warn'
    else 'success' end as invocation_status
from invocation
left join models on invocation.command_invocation_id = models.command_invocation_id
left join tests on invocation.command_invocation_id = tests.command_invocation_id