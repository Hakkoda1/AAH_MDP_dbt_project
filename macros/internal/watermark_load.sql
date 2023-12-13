 -- insert_ghost_record_to_satellite.sql
 -- {{ insert_ghost_record_to_satellite('sat_vetspire_stg_locations_location') }}
{% macro watermark_load(mode,layer) %}
  {# Takes input metadata for Satellite name to generate and execute the insert statement of a ghost record to a satellite. #}

{% if target.name == 'test_slim_ci' %}
    {{ return('') }}
{% endif %}


{% set stream_query %}
    {{ create_stream_watermark(mode) }}
{% endset %}

{% set results = run_query(stream_query) %}


{% set task_query %}
   {{ create_task_watermark_table(mode,layer) }}
{% endset %}

{% set results = run_query(task_query) %}


{% set task_resume_query %}
   ALTER TASK IF EXISTS {{this}}_watermark_load RESUME
{% endset %}

{% set results = run_query(task_resume_query) %}

{#

{% set results = run_query(stream_query) %}

{% if execute %}
{% set hkey_list = results.columns[0].values() %}
{% else %}
{% set hkey_list = [] %}
{% endif %}

#}



{% endmacro %}