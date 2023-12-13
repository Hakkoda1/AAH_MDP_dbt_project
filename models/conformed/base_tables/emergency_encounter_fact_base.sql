{{
    config(
        materialized='incremental_custom',
        incremental_strategy='insert_overwrite'
    )
}}
--        materialized='incremental',
--        unique_key='ed_encounter_key'

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
    {{ dbt_utils.generate_surrogate_key( [ 'f_ed_encounters.pat_enc_csn_id' ] ) }} as ed_encounter_key, --PK

    {{ generate_optional_foreign_key( 'f_ed_encounters.pat_id' ) }} as patient_key,
    {{ generate_optional_foreign_key( 'f_ed_encounters.first_emergency_department_id' ) }}  as first_emergency_location_key,
    {{ generate_optional_foreign_key( 'f_ed_encounters.admission_event_id' ) }} as admission_adt_event_key,
    {{ generate_optional_foreign_key( 'v_adt_ed_admits.department_id' ) }} as location_key,
    {{ generate_optional_foreign_key( 'v_adt_ed_admits.xfer_in_event_id' ) }} as admit_adt_event_key,
    {{ generate_optional_foreign_key( 'v_adt_ed_admits.event_id' ) }}  as adt_event_key,

    {# metadata #}
    current_timestamp as mdp_effective_date, --Should this be the latest change on any column or the last time the table was built?
    'CLARITY.dbo.PAT_ENC' as record_source, --Can we make this an array?

    {# id #}

    {# dimensions #}
    lkp_clr_arriv_means.name::nvarchar as arrival_means,
    lkp_clr_acuity_level.name::nvarchar as acuity_level,
    lkp_clr_ed_disposition.name::nvarchar as ed_disposition,
    lkp_clr_disch_disp.name::nvarchar as discharge_disposition,
	
    {# date/time #}
    f_ed_encounters.adt_arrival_date::datetime as ed_arrival_date,
    f_ed_encounters.adt_arrival_dttm::datetime as ed_arrival_instant,
	f_ed_encounters.emergency_admission_dttm::datetime as emergency_encounter_instant,
	f_ed_encounters.ed_disposition_dttm::datetime as emergency_disposition_instant,
	f_ed_encounters.ed_departure_dttm::datetime as emergency_departure_instant,
	f_ed_encounters.hospital_discharge_dttm::datetime as hospital_discharge_instant,
    v_adt_ed_admits.board_start_dttm::datetime as board_start_instant,
    v_adt_ed_admits.board_end_dttm::datetime as board_end_instant
    
    
from
    pat_enc_hsp --uses inpatient workflow instead of ambulatory workflow
        left join {{ ref("pat_enc_base") }} pat_enc 
            on pat_enc_hsp.pat_enc_csn_id = pat_enc.pat_enc_csn_id
        left join {{ ref("f_ed_encounters_base") }} f_ed_encounters
            on f_ed_encounters.pat_enc_csn_id = pat_enc.pat_enc_csn_id 
        -- left join ed_disp_events (used in source joins but no columns currently pulled)
        left join  {{ ref("v_adt_ed_admits") }} v_adt_ed_admits
            on v_adt_ed_admits.pat_enc_csn_id =  pat_enc_hsp.pat_enc_csn_id    
        left join {{ ref("pend_action_base") }} pend_action
            on v_adt_ed_admits.pend_id = pend_action.pend_id
        left join {{ ref("clarity_adt_base") }} clarity_adt
            on v_adt_ed_admits.event_id = clarity_adt.event_id
        left join {{ ref("lkp_clr_arriv_means_base") }} lkp_clr_arriv_means
            on pat_enc_hsp.means_of_arrv_c = lkp_clr_arriv_means.means_of_arrv_c
        left join {{ ref("lkp_clr_acuity_level_base") }} lkp_clr_acuity_level
            on pat_enc_hsp.acuity_level_c = lkp_clr_acuity_level.acuity_level_c
        left join {{ ref("lkp_clr_ed_disposition_base") }} lkp_clr_ed_disposition
            on pat_enc_hsp.ed_disposition_c = lkp_clr_ed_disposition.ed_disposition_c
        left join {{ ref("lkp_clr_disch_disp_base") }} lkp_clr_disch_disp
            on pat_enc_hsp.disch_disp_c = lkp_clr_disch_disp.disch_disp_c
