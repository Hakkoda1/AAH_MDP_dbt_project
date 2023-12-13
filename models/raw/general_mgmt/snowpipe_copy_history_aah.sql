{{ config(
    materialized='view'
    ) 
}}

select file_name,
    stage_location,
    last_load_time,
    row_count,
    row_parsed,
    file_size,
    first_error_message,
    first_error_line_number,
    first_error_character_pos,
    first_error_column_name,
    error_count,
    error_limit,
    status,
    table_id,
    table_name,
    table_schema_id,
    table_schema_name,
    table_catalog_id,
    table_catalog_name,
    pipe_catalog_name,
    pipe_schema_name,
    pipe_name,
    pipe_received_time,
    first_commit_time,
    c2,
    c3,
    case
        when (c3 = c2 and not c3 is null) and not c2 is null or c3 is null and c2 is null
        then cast(1 as integer)
        else cast(0 as integer)
    end as c1
from 
(
    select otbl.file_name,
        otbl.stage_location,
        otbl.last_load_time,
        otbl.row_count,
        otbl.row_parsed,
        otbl.file_size,
        otbl.first_error_message,
        otbl.first_error_line_number,
        otbl.first_error_character_pos,
        otbl.first_error_column_name,
        otbl.error_count,
        otbl.error_limit,
        otbl.status,
        otbl.table_id,
        otbl.table_name,
        otbl.table_schema_id,
        otbl.table_schema_name,
        otbl.table_catalog_id,
        otbl.table_catalog_name,
        otbl.pipe_catalog_name,
        otbl.pipe_schema_name,
        otbl.pipe_name,
        otbl.pipe_received_time,
        otbl.first_commit_time,
        otbl.c2,
        itbl.c1 as c3
    from 
    (
        select file_name,
            stage_location,
            last_load_time,
            row_count,
            row_parsed,
            file_size,
            first_error_message,
            first_error_line_number,
            first_error_character_pos,
            first_error_column_name,
            error_count,
            error_limit,
            status,
            table_id,
            table_name,
            table_schema_id,
            table_schema_name,
            table_catalog_id,
            table_catalog_name,
            pipe_catalog_name,
            pipe_schema_name,
            pipe_name,
            pipe_received_time,
            first_commit_time,
            { fn convert(c1, SQL_DATE) } as c2
        from 
        (
            select file_name,
                stage_location,
                last_load_time,
                row_count,
                row_parsed,
                file_size,
                first_error_message,
                first_error_line_number,
                first_error_character_pos,
                first_error_column_name,
                error_count,
                error_limit,
                status,
                table_id,
                table_name,
                table_schema_id,
                table_schema_name,
                table_catalog_id,
                table_catalog_name,
                pipe_catalog_name,
                pipe_schema_name,
                pipe_name,
                pipe_received_time,
                first_commit_time,
                last_load_time as c1
            from {{ source('account_usage', 'copy_history') }}
        ) as itbl
    ) as otbl
    left outer join 
    (
        select table_name,
            max(c2) as c1
        from 
        (
            select file_name,
                stage_location,
                last_load_time,
                row_count,
                row_parsed,
                file_size,
                first_error_message,
                first_error_line_number,
                first_error_character_pos,
                first_error_column_name,
                error_count,
                error_limit,
                status,
                table_id,
                table_name,
                table_schema_id,
                table_schema_name,
                table_catalog_id,
                table_catalog_name,
                pipe_catalog_name,
                pipe_schema_name,
                pipe_name,
                pipe_received_time,
                first_commit_time,
                { fn convert(c1, SQL_DATE) } as c2
            from 
            (
                select file_name,
                    stage_location,
                    last_load_time,
                    row_count,
                    row_parsed,
                    file_size,
                    first_error_message,
                    first_error_line_number,
                    first_error_character_pos,
                    first_error_column_name,
                    error_count,
                    error_limit,
                    status,
                    table_id,
                    table_name,
                    table_schema_id,
                    table_schema_name,
                    table_catalog_id,
                    table_catalog_name,
                    pipe_catalog_name,
                    pipe_schema_name,
                    pipe_name,
                    pipe_received_time,
                    first_commit_time,
                    last_load_time as c1
                from {{ source('account_usage', 'copy_history') }}
            ) as itbl
        ) as itbl
        group by table_name
    ) as itbl on ((otbl.table_name = itbl.table_name and not otbl.table_name is null) and not itbl.table_name is null or otbl.table_name is null and itbl.table_name is null)
) as itbl

where 
stage_location like '{% if target.name == 'dev' %}%claritynonprod%{% elif target.name == 'test' %}%claritynonprod%{% elif target.name == 'uat' %}%clarityprod%{% elif target.name == 'prod' %}%clarityprod%{% endif %}' 

