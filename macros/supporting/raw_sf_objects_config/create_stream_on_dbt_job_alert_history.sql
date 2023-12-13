{% macro create_stream_on_dbt_job_alert_history() %}
create stream if not exists {{var("raw_database")}}.general_mgmt.stream_dbt_job_alert_history on table {{var("raw_database")}}.general_mgmt.dbt_job_alert_history
{% endmacro %}