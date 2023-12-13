{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite',
        post_hook = [ "{{insert_default_key_to_dim_table( 'provider_dim_base', 
                                                          'provider_key' )}}" ]
    )
}}



select

    {# keys #}
    {{ dbt_utils.generate_surrogate_key(['ser.prov_id']) }} as provider_key,

    {# metadata #}
    current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
    'CLARITY.dbo.CLARITY_SER' as record_source, --Can we make this an array?

    {# id #}
    ser.prov_id::varchar as provider_id,
    ser.user_id::varchar as employee_id,

    {# dimensions #}
    ser.prov_name::varchar as provider_name,
    ser_2.npi::varchar as npi,
    dep.department_id::varchar as primary_department,
    dep.department_name::varchar as primary_department_name,
    lkp_note_ser.name::varchar as provider_type,
    lkp_spec.name::varchar as primary_specialty,
    ser.meds_auth_prov_yn::varchar as can_authorize_meds,
    ser.ords_auth_prov_yn::varchar as can_authorize_orders,
    ser.doctors_degree::varchar as doctor_degree,
    ser.clinician_title::varchar as clinican_title,
    lkp_staff_res.name::varchar as staff_resource,

    {# date/time #}
    ser.prov_start_date::date as start_date

from {{ ref("clarity_ser_base") }} ser
left join {{ ref('clarity_ser_2_base') }} ser_2 on ser.prov_id = ser_2.prov_id
left join {{ ref('lkp_clr_note_ser_base') }} lkp_note_ser on ser.provider_type_c = lkp_note_ser.service_type_c
left join {{ ref('lkp_clr_staff_resource_base') }} lkp_staff_res on lkp_staff_res.staff_resource_c = ser.staff_resource_c
left join {{ ref('lkp_clr_license_display_base') }} lkp_lic_disp on ser_2.cur_cred_c = lkp_lic_disp.license_display_c 
left join {{ ref('clarity_ser_spec_base') }} ser_spec on ser_spec.prov_id = ser.prov_id and ser_spec.line = 1
left join {{ ref('lkp_clr_specialty_base')}} lkp_spec on ser_spec.specialty_c = lkp_spec.specialty_c
left join {{ ref('clarity_dep_base') }} dep on dep.department_id = ser_2.primary_dept_id