{{
    config(
        materialized='incremental',
        unique_key=['cluster_key','encounter_key']
    )
}}


with pat_enc as (
    select *
    from {{ ref('pat_enc') }} 
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where mdp_effective_datetime > (select max(t2.mdp_effective_date) from {{ this }} t2)

    {% endif %}
    
),
order_proc_2 as (
    select pat_enc.pat_enc_csn_id,
        ord_appt_srl_num.appts_scheduled,
        order_proc_2.order_proc_id,
        order_proc_2.pat_enc_csn_id as pat_enc_csn_id_order_proc,
        order_proc_2.prioritized_inst_tm,
        order_proc_2.appt_status_c,
        order_proc_2.removal_reason_c
    from pat_enc
    left join {{ ref('ord_appt_srl_num') }} ord_appt_srl_num
        on pat_enc.appt_serial_no = ord_appt_srl_num.appts_scheduled
    left join {{ ref('order_proc_2') }} order_proc_2
        on order_proc_2.order_proc_id = ord_appt_srl_num.order_proc_id
    qualify row_number() over (partition by pat_enc.pat_enc_csn_id order by order_proc_2.prioritized_inst_tm asc) = 1
)
select
    -- {# keys #}
    {{ dbt_utils.generate_surrogate_key(['pat_enc.pat_enc_csn_id']) }} as encounter_key, --PK

    {{ generate_optional_foreign_key( 'order_proc_2.appts_scheduled' ) }} as first_scheduled_encounter_key,
    {{ generate_optional_foreign_key( 'order_proc_2.order_proc_id' ) }} as procedure_order_key,
    {# {{ generate_optional_foreign_key( 'f_sched_appt.appt_serial_num' ) }} as first_scheduled_encounter_key, duplicate #}
    {{ generate_optional_foreign_key( 'f_sched_appt.appt_entry_user_id' ) }} as scheduled_by_employee_key,
    {{ generate_optional_foreign_key( 'pat_enc.referral_id' ) }} as referral_key,
    {{ generate_optional_foreign_key( 'pat_enc.account_id' ) }} as guarantor_key,
    {{ generate_optional_foreign_key( 'pat_enc.coverage_id' ) }} as primary_coverage_key,
    {{ generate_optional_foreign_key( 'pat_enc.hsp_account_id' ) }} as hospital_account_key,
    --pat_enc.pat_enc_csn_id::numeric(18) as encounter_key, PK duplicate
    {{ generate_optional_foreign_key( 'pat_enc.visit_prov_id' ) }} as admitting_provider_key,
    {{ generate_optional_foreign_key( 'pat_enc.department_id' ) }} as location_key,
    {{ generate_optional_foreign_key( 'pat_enc.pat_id' ) }} as patient_key,

    {# metadata #}
    current_timestamp as mdp_effective_date, 
    'CLARITY.dbo.PAT_ENC' as record_source, 
    trunc(pat_enc.pat_enc_csn_id, -3) as cluster_key,
    
    {# id #}
    pat_enc.pat_enc_csn_id,

    {# dimensions #}
    pat_enc.bp_systolic::float as systolic_bp,
	pat_enc.bp_diastolic::float as diastolic_bp,
	pat_enc.temperature::float as temperature,
	pat_enc.pulse::integer as pulse,
	pat_enc.weight::float as weight,
    pat_enc.height::nvarchar as height,
    pat_enc.appt_prc_id::nvarchar as appointment_type,
    lkp_clr_proc_category.name::nvarchar as appointment_report_type,
    lkp_clr_cancel_reason.name::nvarchar as appointment_cancel_reason,
	pat_enc.pat_enc_date_real::float as contact_date_real,
    lkp_clr_disp_enc_type.name::nvarchar as encounter_type,
    pat_enc.enc_closed_yn::nvarchar as is_encounter_closed,
    lkp_clr_appt_status.name::nvarchar as appointment_status,

    {# date/time #}
    pat_enc.contact_date::date as contact_date, --PK
    f_sched_appt.appt_made_date::date as appointment_made_date

from
    pat_enc
    {# {{ ref('availability_base') }} availability	--show provider was available but didn't see any patients
    --     left join {{ ref('pat_enc_base') }} pat_enc	
    --         on availability.pat_enc_csn_id = pat_enc.pat_enc_csn_id #}
        {# left join {{ ref('patient') }} patient --This join isn't needed in the fact  
            on pat_enc.pat_id = patient.pat_id 
        left join {{ ref('clarity_ser') }} clarity_ser --This join isn't needed in the fact 
            on pat_enc.visit_prov_id = clarity_ser.prov_id #}
        left join order_proc_2
            on pat_enc.pat_enc_csn_id = order_proc_2.pat_enc_csn_id and pat_enc.appt_serial_no = order_proc_2.appts_scheduled
        left join {{ ref('f_sched_appt') }} f_sched_appt 
            on f_sched_appt.pat_enc_csn_id = pat_enc.pat_enc_csn_id
        {# --left join {{ ref('f_sched_appt_stats') }} f_sched_appt_stats
        --    on f_sched_appt_stats.prov_id = clarity_ser.prov_id
        --left join {{ ref('clarity_dep') }} clarity_dep
        --    on f_sched_appt_stats.department_id::varchar = clarity_dep.department_id::varchar
        --left join {{ ref('pat_enc_appt') }} pat_enc_appt
            --on pat_enc_appt.pat_enc_csn_id = pat_enc.pat_enc_csn_id
        --left join {{ ref('pat_enc_es_aud_act') }} pat_enc_es_aud_act
        --    on pat_enc_es_aud_act.pat_enc_csn_id = pat_enc.pat_enc_csn_id #}
        left join {{ ref('lkp_clr_appt_status') }} lkp_clr_appt_status	
            on lkp_clr_appt_status.appt_status_c = pat_enc.appt_status_c
        left join {{ ref('lkp_clr_ord_appt_status') }} ord_appt_status
            on order_proc_2.appt_status_c = ord_appt_status.ord_appt_status_c
        left join {{ ref('lkp_clr_removal_reason') }} lkp_clr_removal_reason
            on order_proc_2.removal_reason_c = lkp_clr_removal_reason.removal_reason_c
        left join {{ ref('clarity_prc_2') }} clarity_prc_2	
            on clarity_prc_2.prc_id = f_sched_appt.prc_id 
        left join {{ ref('lkp_clr_proc_category') }} lkp_clr_proc_category
            on lkp_clr_proc_category.report_category_c = clarity_prc_2.report_category_c
        left join {{ ref('lkp_clr_cancel_reason') }} lkp_clr_cancel_reason	
            on lkp_clr_cancel_reason.cancel_reason_c = pat_enc.cancel_reason_c
        left join {{ ref('lkp_clr_disp_enc_type') }} lkp_clr_disp_enc_type
            on lkp_clr_disp_enc_type.disp_enc_type_c = pat_enc.enc_type_c


{#
    ord_appt_srl_num.appts_scheduled::numeric(18) as first_scheduled_encounter_key,
    ord_appt_srl_num.order_proc_id::numeric(18) as procedure_order_key,
    f_sched_appt.appt_serial_num::numeric(18) as first_scheduled_encounter_key,
    f_sched_appt.pat_enc_csn_id::numeric(18) as encounter_key, --PK
    f_sched_appt.appt_entry_user_id::nvarchar(18) as scheduled_by_employee_key,
    pat_enc.referral_id::numeric as referral_key,
    pat_enc.account_id::numeric as guarantor_key,
    pat_enc.coverage_id::numeric as primary_coverage_key,
    pat_enc.hsp_account_id::numeric as hospital_account_key,
    --pat_enc.pat_enc_csn_id::numeric(18) as encounter_key, PK duplicate
    pat_enc.visit_prov_id::nvarchar(18) as admitting_provider_key,
    pat_enc.department_id::nvarchar(18) as location_key,
    pat_enc.pat_id::nvarchar(18) as patient_key,
    current_timestamp as effective_date, --Should this be the latest change on any column or the last time the table was built?
    'CLARITY.dbo.PAT_ENC' as record_source, --Can we make this an array?
    pat_enc.bp_systolic::float as systolic_bp,
	pat_enc.bp_diastolic::float as diastolic_bp,
	pat_enc.temperature::float as temperature,
	pat_enc.pulse::integer as pulse,
	pat_enc.weight::float as weight,
    pat_enc.height::nvarchar(270) as height,
    pat_enc.appt_prc_id::nvarchar(18) as appointment_type,
    lkp_clr_proc_category.name::nvarchar(254) as appointment_report_type,
    lkp_clr_cancel_reason.name::nvarchar(254) as appointment_cancel_reason,
	pat_enc.pat_enc_date_real::float as contact_date_real,
    lkp_clr_disp_enc_type.name::nvarchar(254) as encounter_type,
    pat_enc.enc_closed_yn::nvarchar(1) as is_encounter_closed,
    lkp_clr_appt_status.name::nvarchar(254) as appointment_status,

    pat_enc.contact_date::date as contact_date, --PK
    f_sched_appt.appt_made_date::date as appointment_made_date
#}