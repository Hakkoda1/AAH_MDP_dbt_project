{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite',
        post_hook = [ "{{insert_default_key_to_dim_table( 'location_dim_base', 
                                                          'Location_Key' )}}" ]
    )
}}


with 

clarity_dep as(
    select * from {{ ref('clarity_dep_base') }}
),
clarity_dep_2 as(
    select * from {{ ref('clarity_dep_2_base') }}
),
clarity_rom as(
    select * from {{ ref('clarity_rom_base') }}
),
clarity_loc as(
    select * from {{ ref('clarity_loc_base') }}
),
clarity_sa as(
    select * from {{ ref('clarity_sa_base') }}
),
clarity_bed as(
    select * from {{ ref('clarity_bed_base') }}
),
ed_care_area_info as(
    select * from {{ ref('ed_care_area_info_base') }}
),
ed_care_area_room as(
    select * from {{ ref('ed_care_area_room_base') }}
),
lkp_clr_dep_rpt_grp_13_base as (
    select * from {{ ref('lkp_clr_dep_rpt_grp_13_base') }}
),
lkp_clr_dep_rpt_grp_14_base as (
    select * from {{ ref('lkp_clr_dep_rpt_grp_14_base') }}
),
lkp_clr_dep_specialty as (
    select * from {{ ref('lkp_clr_dep_specialty_base') }}
),

service_area as(
    select
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['SERV_AREA_ID']) }} as Location_Key,
        'ServiceArea'::VARCHAR as Key_Type,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        record_source as record_source, --Can we make this an array?

        {# id #}{# dimensions #}

        SERV_AREA_ID::NUMBER(18) as Service_Area_EpicId,
        SERV_AREA_NAME::VARCHAR as Service_Area_Name,
        null as Location_Epic_Id,
        null as Location_Name,
        null as Department_Epic_Id,
        null as Department_Name,
        null as Care_Area_Epic_Id,
        null as Care_Area_Name,
        null as Room_Epic_Id,
        null as Room_Name,
        null as Bed_Epic_Id,
        null as Bed_Name,
        null as Department_External_Name,
        null as Department_Abbreviation,
        null as Department_Specialty,
        null as Department_Region,
        null as Department_Psa,
        null as Department_Zip_Code,
        null as Is_Bed,
        null as Is_Room,
        null as Is_Surgical_Group,
        null as Is_Care_Area,
        null as Is_Department,
        null as Is_Location,
        True::TINYINT as Is_Service_Area
        
    from clarity_sa
),

location as(
    select
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['LOC_ID']) }} as Location_Key,
        'Location'::VARCHAR as Key_Type,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        loc.record_source as record_source, --Can we make this an array?

        {# id #}{# dimensions #}
        sa.SERV_AREA_ID::NUMBER(18) as Service_Area_EpicId,
        sa.SERV_AREA_NAME::VARCHAR as Service_Area_Name,
        loc.LOC_ID::NUMBER(18) as Location_Epic_Id,
        loc.LOC_NAME::VARCHAR as Location_Name,
        null as Department_Epic_Id,
        null as Department_Name,
        null as Care_Area_Epic_Id,
        null as Care_Area_Name,
        null as Room_Epic_Id,
        null as Room_Name,
        null as Bed_Epic_Id,
        null as Bed_Name,
        null as Department_External_Name,
        null as Department_Abbreviation,
        null as Department_Specialty,
        null as Department_Region,
        null as Department_Psa,
        null as Department_Zip_Code,
        null as Is_Bed,
        null as Is_Room,
        null as Is_Surgical_Group,
        null as Is_Care_Area,
        null as Is_Department,
        True::TINYINT as Is_Location,
        null as Is_Service_Area
        
    
    from clarity_loc loc
        left join clarity_sa sa
            on loc.serv_area_id = sa.serv_area_id
),

department as(
    select
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['dep.DEPARTMENT_ID']) }} as Location_Key,
        'Department'::VARCHAR as Key_Type,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        dep.record_source as record_source, --Can we make this an array?

        {# id #}{# dimensions #}
        sa.SERV_AREA_ID::NUMBER(18) as Service_Area_Epic_Id,
        sa.SERV_AREA_NAME::VARCHAR as Service_Area_Name,
        loc.LOC_ID::NUMBER(18) as Location_Epic_Id,
        loc.LOC_NAME::VARCHAR as Location_Name,
        dep.DEPARTMENT_ID::VARCHAR as Department_Epic_Id,
        dep.DEPARTMENT_NAME::VARCHAR as Department_Name,
        null as Care_Area_Epic_Id,
        null as Care_Area_Name,
        null as Room_Epic_Id,
        null as Room_Name,
        null as Bed_Epic_Id,
        null as Bed_Name,
        dep.EXTERNAL_NAME::VARCHAR as Department_External_Name,
        dep.DEPT_ABBREVIATION::VARCHAR as Department_Abbreviation,
        lcds.NAME::VARCHAR as Department_Specialty,
        lcdrg13.NAME::VARCHAR as Department_Region,
        lcdrg14.NAME::VARCHAR as Department_Psa,
        dep_2.ADDRESS_ZIP_CODE::VARCHAR as Department_Zip_Code,
        null as Is_Bed,
        null as Is_Room,
        null as Is_Surgical_Group,
        null as Is_Care_Area,
        True::TINYINT as Is_Department,
        null as Is_Location,
        null as Is_Service_Area

    from clarity_dep dep
        left join clarity_dep_2 dep_2 
            on dep.department_id = dep_2.department_id
        left join clarity_loc loc
            on dep.rev_loc_id = loc.loc_id
        left join clarity_sa sa
            on loc.serv_area_id = sa.serv_area_id
        left join lkp_clr_dep_specialty lcds 
            on dep.specialty_dep_c = lcds.dep_specialty_c
        left join lkp_clr_dep_rpt_grp_13_base lcdrg13
            on lcdrg13.rpt_grp_thirteen_c = dep.rpt_grp_thirteen_c
        left join lkp_clr_dep_rpt_grp_14_base lcdrg14
            on lcdrg14.rpt_grp_fourteen_c = dep.rpt_grp_fourteen_c

),

room as(
    select
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['room.ROOM_ID']) }} as Location_Key,
        'Room'::VARCHAR as Key_Type,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        room.record_source as record_source, --Can we make this an array?

        {# id #}{# dimensions #}
        sa.SERV_AREA_ID::NUMBER(18) as Service_Area_Epic_Id,
        sa.SERV_AREA_NAME::VARCHAR as Service_Area_Name,
        loc.LOC_ID::NUMBER(18) as Location_Epic_Id,
        loc.LOC_NAME::VARCHAR as Location_Name,
        dep.DEPARTMENT_ID::VARCHAR as Department_Epic_Id,
        dep.DEPARTMENT_NAME::VARCHAR as Department_Name,
        care_area.CARE_AREA_ID::NUMBER(18) as Care_Area_Epic_Id,
        care_area.CARE_AREA_NAME::VARCHAR as Care_Area_Name,
        room.ROOM_ID::VARCHAR as Room_Epic_Id,
        room.ROOM_NAME::VARCHAR as Room_Name,
        null as Bed_Epic_Id,
        null as Bed_Name,
        dep.EXTERNAL_NAME::VARCHAR as Department_External_Name,
        dep.DEPT_ABBREVIATION::VARCHAR as Department_Abbreviation,
        lcds.NAME::VARCHAR as Department_Specialty,
        lcdrg13.NAME::VARCHAR as Department_Region,
        lcdrg14.NAME::VARCHAR as Department_Psa,
        dep_2.ADDRESS_ZIP_CODE::VARCHAR as Department_Zip_Code,
        null as Is_Bed,
        True::TINYINT as Is_Room,
        null as Is_Surgical_Group,
        null as Is_Care_Area,
        null as Is_Department,
        null as Is_Location,
        null as Is_Service_Area
    from clarity_rom room
            left join clarity_dep dep 
                on room.department_id = dep.department_id
            left join clarity_dep_2 dep_2 
                on dep.department_id = dep_2.department_id
            left join ed_care_area_room care_area_room
                on room.room_id = care_area_room.room_id
            left join ed_care_area_info care_area
                on care_area_room.care_area_id = care_area.care_area_id
            left join clarity_loc loc
                on dep.rev_loc_id = loc.loc_id
            left join clarity_sa sa
                on loc.serv_area_id = sa.serv_area_id
            left join lkp_clr_dep_specialty lcds 
                on dep.specialty_dep_c = lcds.dep_specialty_c
            left join lkp_clr_dep_rpt_grp_13_base lcdrg13
                on lcdrg13.rpt_grp_thirteen_c = dep.rpt_grp_thirteen_c
            left join lkp_clr_dep_rpt_grp_14_base lcdrg14
                on lcdrg14.rpt_grp_fourteen_c = dep.rpt_grp_fourteen_c
),

bed as(
    select
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['bed.BED_ID']) }} as Location_Key,
        'Bed'::VARCHAR as Key_Type,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        bed.record_source as record_source, --Can we make this an array?

        {# id #}{# dimensions #}
        sa.SERV_AREA_ID::NUMBER(18) as Service_Area_Epic_Id,
        sa.SERV_AREA_NAME::VARCHAR as Service_Area_Name,
        loc.LOC_ID::NUMBER(18) as Location_Epic_Id,
        loc.LOC_NAME::VARCHAR as Location_Name,
        dep.DEPARTMENT_ID::VARCHAR as Department_Epic_Id,
        dep.DEPARTMENT_NAME::VARCHAR as Department_Name,
        care_area.CARE_AREA_ID::NUMBER(18) as Care_Area_Epic_Id,
        care_area.CARE_AREA_NAME::VARCHAR as Care_Area_Name,
        room.ROOM_ID::VARCHAR as Room_Epic_Id,
        room.ROOM_NAME::VARCHAR as Room_Name,
        bed.BED_ID::VARCHAR as Bed_Epic_Id,
        bed.BED_LABEL::VARCHAR as Bed_Name,
        dep.EXTERNAL_NAME::VARCHAR as Department_External_Name,
        dep.DEPT_ABBREVIATION::VARCHAR as Department_Abbreviation,
        lcds.NAME::VARCHAR as Department_Specialty,
        lcdrg13.NAME::VARCHAR as Department_Region,
        lcdrg14.NAME::VARCHAR as Department_Psa,
        dep_2.ADDRESS_ZIP_CODE::VARCHAR as Department_Zip_Code,
        True::TINYINT as Is_Bed,
        null as Is_Room,
        null as Is_Surgical_Group,
        null as Is_Care_Area,
        null as Is_Department,
        null as Is_Location,
        null as Is_Service_Area
       
    from clarity_bed bed
        left join clarity_rom room
            on bed.room_id = room.room_id
        left join clarity_dep dep 
            on room.department_id = dep.department_id
        left join clarity_dep_2 dep_2 
            on dep.department_id = dep_2.department_id
        left join ed_care_area_room care_area_room
            on room.room_id = care_area_room.room_id
        left join ed_care_area_info care_area
            on care_area_room.care_area_id = care_area.care_area_id
        left join clarity_loc loc
            on dep.rev_loc_id = loc.loc_id
        left join clarity_sa sa
            on loc.serv_area_id = sa.serv_area_id
        left join lkp_clr_dep_specialty lcds 
            on dep.specialty_dep_c = lcds.dep_specialty_c
        left join lkp_clr_dep_rpt_grp_13_base lcdrg13
            on lcdrg13.rpt_grp_thirteen_c = dep.rpt_grp_thirteen_c
        left join lkp_clr_dep_rpt_grp_14_base lcdrg14
            on lcdrg14.rpt_grp_fourteen_c = dep.rpt_grp_fourteen_c
),

care_area as(
    select
        {# keys #}
        {{ dbt_utils.generate_surrogate_key(['care_area.CARE_AREA_ID']) }} as Location_Key,
        'CareArea'::VARCHAR as Key_Type,

        {# metadata #}
        current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
        care_area.record_source as record_source, --Can we make this an array?

        {# id #}{# dimensions #}

        sa.SERV_AREA_ID::NUMBER(18) as Service_Area_Epic_Id,
        sa.SERV_AREA_NAME::VARCHAR as Service_Area_Name,
        loc.LOC_ID::NUMBER(18) as Location_Epic_Id,
        loc.LOC_NAME::VARCHAR as Location_Name,
        dep.DEPARTMENT_ID::VARCHAR as Department_Epic_Id,
        dep.DEPARTMENT_NAME::VARCHAR as Department_Name,
        care_area.CARE_AREA_ID::NUMBER(18) as Care_Area_Epic_Id,
        care_area.CARE_AREA_NAME::VARCHAR as Care_Area_Name,
        null as Room_Epic_Id,
        null as Room_Name,
        null as Bed_Epic_Id,
        null as Bed_Name,
        dep.EXTERNAL_NAME::VARCHAR as Department_External_Name,
        dep.DEPT_ABBREVIATION::VARCHAR as Department_Abbreviation,
        lcds.NAME::VARCHAR as Department_Specialty,
        lcdrg13.NAME::VARCHAR as Department_Region,
        lcdrg14.NAME::VARCHAR as Department_Psa,
        dep_2.ADDRESS_ZIP_CODE::VARCHAR as Department_Zip_Code,
        null as Is_Bed,
        null as Is_Room,
        null as Is_Surgical_Group,
        True::TINYINT as Is_Care_Area,
        null as Is_Department,
        null as Is_Location,
        null as Is_Service_Area
    from ed_care_area_info care_area 
        left join clarity_dep dep 
            on care_area.department_id = dep.department_id 
        left join clarity_dep_2 dep_2 
            on dep.department_id = dep_2.department_id
        left join clarity_loc loc
            on care_area.loc_id = loc.loc_id
        left join clarity_sa sa
            on loc.serv_area_id = sa.serv_area_id
        left join lkp_clr_dep_specialty lcds 
            on dep.specialty_dep_c = lcds.dep_specialty_c
        left join lkp_clr_dep_rpt_grp_13_base lcdrg13
            on lcdrg13.rpt_grp_thirteen_c = dep.rpt_grp_thirteen_c
        left join lkp_clr_dep_rpt_grp_14_base lcdrg14
            on lcdrg14.rpt_grp_fourteen_c = dep.rpt_grp_fourteen_c
),

location_dim as(
    select * from service_area
    union all 
    select * from location
    union all 
    select * from department
    union all
    select * from room
    union all
    select * from bed
    union all
    select * from care_area
)

select *
from location_dim
