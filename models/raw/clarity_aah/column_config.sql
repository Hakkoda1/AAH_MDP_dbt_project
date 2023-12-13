{{ config(
    materialized= "view",
    schema = "GENERAL_MGMT"
) }}
{#
with base_clarity_col_cte as(
    {{ flatten_json_table( source('raw_clarity_aah', 'RAW_CLARITY_COL') )}}
),
base_clarity_tbl_cte as(
    {{ flatten_json_table( source('raw_clarity_aah', 'RAW_CLARITY_TBL') )}}
),
base_clarity_tbl_pk_cte as(
    {{ flatten_json_table( source('raw_clarity_aah', 'RAW_CLARITY_TBL_PK') )}}
),
base_clarity_tbl_cols_cte as(
    {{ flatten_json_table( source('raw_clarity_aah', 'RAW_CLARITY_TBL_COLS') )}}
),
base_clarity_tbl_pk as(
    {{ flatten_json_table( source('raw_clarity_aah', 'RAW_CLARITY_TBL_PK') )}}
),
clarity_col_cte as(
    select
        value_column_id::string as column_id, --PK
        value_data_type::string as data_type,
        value_description::string as description,
        value_discontinued_item_yn::string as discontinued_item_yn,
        value_table_id::string as table_id, --FK
        value_col_descriptor_ovr::string as col_descriptor_ovr,
        value_column_name::string as column_name,
        value_col_descriptor::string as col_descriptor,
        value_format_ini::string as ini,
        value_format_item::string as item,
        value_clarity_scale::string as clarity_scale,
        value_clarity_precision::number as clarity_precision,
        value__update_dt::timestamp as update_dt,
        value_hour_format::string as hour_format,
        value_last_modified_by_epic_dttm::timestamp as last_modified_by_epic_dttm,
        value_deprecated_yn::string as deprecated_yn,
        value_is_extracted_yn::string as is_extracted_yn
    from base_clarity_col_cte
    where 1=1
    -- and (deprecated_yn = 'N' or deprecated_yn is null) 
    -- and is_extracted_yn = 'Y' 
    --and last_modified_by_epic_dttm is not null
),
clarity_tbl_cte as(
    select  --TABLES TABLE
        VALUE_EXTRACT_FILENAME::string as extract_file_name,
        VALUE_TABLE_ID::string as table_id, --pk
        VALUE_LOAD_FREQUENCY::string as load_frequency,
        VALUE_LOAD_TYPE::string as load_type,
        VALUE_TABLE_NAME::string as table_name,
        VALUE_TBL_DESCRIPTOR::string as tbl_descriptor
    from base_clarity_tbl_cte
    where table_id like 'C%'
),
clarity_tbl_pk_cte as(
    select
        VALUE_TABLE_ID::string as table_id,
        VALUE_PK_COLUMN_ID::string as pk_column_id,
        VALUE__UPDATE_DT::timestamp as update_dt
    from base_clarity_tbl_pk_cte
    qualify row_number() over (partition by table_id,pk_column_id order by update_dt desc) = 1
),
clarity_tbl_cols_cte as(
    select
        VALUE_COLUMN_ID::string as column_id,
        VALUE_TABLE_ID::string as table_id,
        VALUE_LINE::number as line
    from base_clarity_tbl_cols_cte
),
clarity_tbl_pk as(
    select
        VALUE_TABLE_ID::string as table_id,
        VALUE_PK_COLUMN_ID::string as pk_column_id,
        VALUE_LINE::number as line
    from base_clarity_tbl_pk
)
#}

with base_clarity_col_cte as(
    select * from  {{ ref('raw_clarity_col_latest') }}
),
base_clarity_tbl_cte as(
    select * from {{ ref('raw_clarity_tbl_latest') }}
),
base_clarity_tbl_pk_cte as(
    select * from {{ ref('raw_clarity_tbl_pk_latest') }}
),
base_clarity_tbl_cols_cte as(
    select * from {{ ref('raw_clarity_tbl_cols_latest') }}
),
{# base_clarity_tbl_pk as(
    select * from {{ ref('raw_clarity_tbl_pk_latest') }}
    
), 
#}
clarity_col_cte as(
    select
        column_id::string as column_id, --PK
        data_type::string as data_type,
        description::string as description,
        discontinued_item_yn::string as discontinued_item_yn,
        table_id::string as table_id, --FK
        col_descriptor_ovr::string as col_descriptor_ovr,
        column_name::string as column_name,
        col_descriptor::string as col_descriptor,
        format_ini::string as ini,
        format_item::string as item,
        clarity_scale::string as clarity_scale,
        clarity_precision::number as clarity_precision,
        -- _update_dt::timestamp as update_dt,
        SRC_UPDATE_DT::timestamp as src_update_dt,
        hour_format::string as hour_format,
        last_modified_by_epic_dttm::timestamp as last_modified_by_epic_dttm,
        deprecated_yn::string as deprecated_yn,
        is_extracted_yn::string as is_extracted_yn
    from base_clarity_col_cte
    where 1=1
    -- and (deprecated_yn = 'N' or deprecated_yn is null) 
    -- and is_extracted_yn = 'Y' 
    --and last_modified_by_epic_dttm is not null
),
clarity_tbl_cte as(
    select  --TABLES TABLE
        EXTRACT_FILENAME::string as extract_file_name,
        TABLE_ID::string as table_id, --pk
        LOAD_FREQUENCY::string as load_frequency,
        LOAD_TYPE::string as load_type,
        TABLE_NAME::string as table_name,
        TBL_DESCRIPTOR::string as tbl_descriptor
    from base_clarity_tbl_cte
    where table_id like 'C%'
),
clarity_tbl_pk_cte as(
    select
        TABLE_ID::string as table_id,
        PK_COLUMN_ID::string as pk_column_id,
        --_UPDATE_DT::timestamp as update_dt
        SRC_UPDATE_DT::timestamp as src_update_dt
    from base_clarity_tbl_pk_cte
    qualify row_number() over (partition by table_id,pk_column_id order by SRC_UPDATE_DT desc) = 1
),
clarity_tbl_cols_cte as(
    select
        COLUMN_ID::string as column_id,
        TABLE_ID::string as table_id,
        LINE::number as line
    from base_clarity_tbl_cols_cte
),
clarity_tbl_pk as(
    select
        TABLE_ID::string as table_id,
        PK_COLUMN_ID::string as pk_column_id,
        LINE::number as line
    from base_clarity_tbl_pk_cte
)

select
    t.table_id,
    t.table_name,
    t.load_type,
    t.load_frequency,
    t.tbl_descriptor,
    c.column_id,
    tb.line,
    case when
        p.pk_column_id is not null then true else false end as primary_key,
    tp.line as pk_line,
    c.column_name,
    c.data_type,
    c.clarity_precision,
    c.clarity_scale,
    c.src_update_dt,
    c.hour_format,
    c.description,
    c.ini,
    c.item,
    c.last_modified_by_epic_dttm,
    c.deprecated_yn,
    c.is_extracted_yn,
    t.extract_file_name
from 
    clarity_tbl_cte t left join clarity_col_cte c on 
        t.table_id = c.table_id
    left join clarity_tbl_pk_cte p on
        t.table_id = p.table_id and c.column_id = p.pk_column_id
    inner join clarity_tbl_cols_cte tb on 
        t.table_id = tb.table_id and c.column_id = tb.column_id
    left join clarity_tbl_pk tp on
        t.table_id = tp.table_id and c.column_id = tp.pk_column_id
--where t.table_name = 'PAT_ENC'

--qualify row_number() over (partition by t.table_id,c.column_id order by c.update_dt desc) = 1
qualify row_number() over (partition by t.table_name,c.column_name order by c.src_update_dt desc) = 1
order by line asc