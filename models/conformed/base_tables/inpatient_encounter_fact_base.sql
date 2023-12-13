{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}
--        materialized='incremental',
--        unique_key= 'inpatient_encounter_key'

with pat_enc_hsp as (
    select *
    from {{ ref("pat_enc_hsp_base") }} t

{#
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where t.mdp_effective_datetime > (select max(t2.mdp_effective_date) from {{ this }} t2)

    {% endif %}
#}
    
)

select
    {# keys #}
    {{ dbt_utils.generate_surrogate_key(['pat_enc_hsp.pat_enc_csn_id']) }} as inpatient_encounter_key, -- PK
    
    {{ generate_optional_foreign_key( 'pat_enc_hsp.pat_id') }} as patient_key,
    {{ generate_optional_foreign_key( 'pat_enc_hsp.department_id' ) }} as first_department_key,
    {{ generate_optional_foreign_key( 'pat_enc_hsp.admission_prov_id' ) }} as admitting_provider_key,
    {{ generate_optional_foreign_key( 'admit_event.user_id' ) }} as admitting_employee_key,
    {{ generate_optional_foreign_key( 'pat_enc_hsp.discharge_prov_id' ) }} as discharging_provider_key,
    {{ generate_optional_foreign_key( 'discharge_event.user_id' ) }} as discharging_employee_key,
    {{ generate_optional_foreign_key( 'problem_list.dx_id' ) }} as hospital_acquired_diagnosis_combo_key,

    {# metadata #}
    current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
    'clarity_pat_enc_hsp' as record_source, --Can we make this an array?

    {# id #}

    hsp_acct_dx_list.dx_id::numeric(18) as discharge_diagnosis_combo_key_id,
	hsp_acct_dx_list.line::integer as discharge_diagnosis_combo_key_line,
    pat_enc_hsp.hsp_account_id::numeric(18) as hospital_account_epic_id,


    {# dimensions #}

	--admit_event.first_ip_in_ip_yn::tinyint as is_first_inpatient_department,
    case 
        when admit_event.first_ip_in_ip_yn = 'Y' then 1::tinyint
        else 0::tinyint end as is_first_in_patient_department,

    --pat_enc_hosp_prob.principal_prob_yn::tinyint as is_primary_problem, --case as yes no
    case 
        when pat_enc_hosp_prob.principal_prob_yn = 'Y' then 1::tinyint
        else 0::tinyint 
            end as is_primary_problem,

    lkp_clr_financial_class.name::nvarchar as hospital_account_financial_class,
    lkp_clr_pat_class.name::nvarchar as patient_admission_class,
    lkp_clr_pat_service.name::nvarchar as hospital_service,
    lkp_clr_pend_event_type.name::nvarchar as admission_origin,
    lkp_clr_disch_disp.name::nvarchar as discharge_disposition,
    lkp_clr_problem_status.name::nvarchar as is_active, --not numeric, kept as more than 1 status (Deleted,Active,Resolved,NULL)


    {# date/time #}

	pat_enc_hsp.hosp_admsn_time::datetime as admission_instant,
	pat_enc_hsp.inp_adm_date::datetime as inpatient_admission_instant,
	pat_enc_hsp.hosp_disch_time::datetime as discharge_instant
	--pat_enc_hsp.order_inst::datetime as discharge_order_instant, --will be future table if order_proc is merged in


    
	
from
    pat_enc_hsp
        left join {{ ref("clarity_adt_base") }} admit_event --overview
            on pat_enc_hsp.inp_adm_event_id = admit_event.event_id
        left join {{ ref("clarity_adt_base") }} discharge_event --overview
            on pat_enc_hsp.dis_event_id = discharge_event.event_id
        left join {{ ref("pat_enc_hosp_prob_base") }} pat_enc_hosp_prob
            on pat_enc_hsp.pat_enc_csn_id = pat_enc_hosp_prob.pat_enc_csn_id
        left join {{ ref("problem_list_base") }} problem_list 
            on pat_enc_hosp_prob.problem_list_id = problem_list.problem_list_id
        
        left join {{ ref("hsp_acct_dx_list_base") }} hsp_acct_dx_list
            on pat_enc_hsp.hsp_account_id = hsp_acct_dx_list.hsp_account_id
        {#
        left join {{ ref("clarity_edg_base") }} hospital_problems
            on problem_list.dx_id = hospital_problems.dx_id
        left join  {{ ref("clarity_edg_base") }} hospital_diagnoses --joined twice?
            on hsp_acct_dx_list.dx_id = hospital_diagnoses.dx_id #}
        
        left join {{ ref("hsp_account_base") }} hsp_account
            on pat_enc_hsp.hsp_account_id = hsp_account.hsp_account_id
        
        left join {{ ref("lkp_clr_financial_class_base") }} lkp_clr_financial_class
            on hsp_account.acct_fin_class_c = lkp_clr_financial_class.financial_class
        left join {{ ref("lkp_clr_pat_class_base") }} lkp_clr_pat_class
            on pat_enc_hsp.adt_pat_class_c = lkp_clr_pat_class.adt_pat_class_c
        left join {{ ref("lkp_clr_pat_service_base") }} lkp_clr_pat_service
            on pat_enc_hsp.hosp_serv_c = lkp_clr_pat_service.hosp_serv_c
        left join {{ ref("pend_action_base") }} pend_action
            on admit_event.event_id = pend_action.linked_event_id
        left join {{ ref("lkp_clr_problem_status_base") }} lkp_clr_problem_status
            on problem_list.problem_status_c = lkp_clr_problem_status.problem_status_c
        left join {{ ref("lkp_clr_pend_event_type_base") }} lkp_clr_pend_event_type
            on pend_action.pend_event_type_c = lkp_clr_pend_event_type.pend_event_type_c
        left join {{ ref("clarity_dep_base") }} clarity_dep
            on admit_event.department_id = clarity_dep.department_id
        left join {{ ref("lkp_clr_disch_disp_base") }} lkp_clr_disch_disp
            on pat_enc_hsp.disch_disp_c = lkp_clr_disch_disp.disch_disp_c
