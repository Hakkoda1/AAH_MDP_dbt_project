with 

CL_BEV_ALL as (
    select * from {{ ref('cl_bev_all_base') }}
),

CLARITY_POS as (
    select * from {{ ref('clarity_pos_base') }}
),
 
CL_BEV_EVENTS_ALL as (
    select * from {{ ref('cl_bev_events_all_base') }}
),

CL_TSK_CLN_STAGES as (
    select * from {{ ref('cl_tsk_cln_stages_base') }}
),

VALID_PATIENT as (
    select * from {{ ref('valid_patient_base') }}
),

MAX_LINE as (

    select
        record_id
        ,max(line) line
    from
        CL_BEV_EVENTS_ALL
    group by
        record_id
),

HKR_USER as (

    select
        hkr_id
        ,userhx.record_id
    from
        CL_BEV_EVENTS_ALL userhx
        inner join MAX_LINE maxline
            on userhx.line = maxline.line and userhx.record_id = maxline.record_id
), 

FIRST_STAGE_DIRTY as (
    select
        firststagedirtysub.record_id
        ,min(firststagedirtysub.instant_tm) first_stage_dirty_dttm
    from
        CL_BEV_EVENTS_ALL firststagedirtysub
    where
        firststagedirtysub.status_c = 1
    group by
        firststagedirtysub.record_id
), 

MSC_DEF as (
    select
        mscdefsub.task_id
        ,max(mscdefsub.line) last_stage
    from
        CL_TSK_CLN_STAGES mscdefsub
    group by
        mscdefsub.task_id
), 

EVENT_RANGES as (
    select
        cl_bev_all.record_id
        ,base.instant_tm
        ,base.status_c
        ,min(nextevent.instant_tm) eventendtime
    from
        CL_BEV_ALL
        left outer join CL_BEV_EVENTS_ALL base
            on CL_BEV_ALL.record_id = base.record_id
        left outer join CL_BEV_EVENTS_ALL nextevent
            on base.record_id = nextevent.record_id
            and base.line < nextevent.line
            and base.instant_tm <= nextevent.instant_tm
    where
        CL_BEV_ALL.event_type_c = 0
        and CL_BEV_ALL.active_c = 0
    group by
        CL_BEV_ALL.record_id
        ,base.instant_tm
        ,base.status_c
        ,coalesce( CL_BEV_ALL.STAGE_NUMBER, 1 )
),

DURATIONS as (
    select
        eventranges.record_id
        ,eventranges.status_c
        ,sum( 
            coalesce( 
                round( 
                    datediff(
                        'minute', 
                        eventranges.instant_tm, 
                        eventranges.eventendtime) * 1440, 1
                    ), 0) ) eventtime
    from  EVENT_RANGES eventranges
    group by
        eventranges.record_id
        ,eventranges.status_c
)

select
	bevbase.record_id as clean_id
	,min(case when bevstshx.status_c = 1 then cast(bevstshx.instant_tm as date) end) as clean_start_date
	,max(case when loc.loc_type_c = 2 then loc.service_area_id else loc.pos_id end) as service_area_id
	,max(case when loc.loc_type_c = 2 then loc.pos_id else null end) as rev_loc_id
	,bevbase.active_c as active_c
	,bevbase.priority_c as priority_c
	,coalesce(bevbase.evs_type_c,1) as evs_type_c
	,min(case when bevstshx.status_c = 1 then bevstshx.instant_tm end) as clean_start_dttm
	,max(case when bevstshx.status_c = 2 then bevstshx.instant_tm end) as clean_asgn_dttm
	,max(case when bevstshx.status_c = 3 then bevstshx.instant_tm end) as clean_inp_dttm
	,max(case when bevstshx.status_c = 5 then bevstshx.instant_tm end) as clean_comp_dttm
	,bevbase.multi_stage_cln_id as multi_stage_cln_id
	,bevbase.cur_stage_id as cur_stage_id
	,bevbase.stage_number as stage_number
	,bevbase.first_stage_evt_id as first_stage_evt_id
	,firststagedirty.first_stage_dirty_dttm as multi_stage_start_dttm
	,max(case when bevbase.stage_number = mscdef.last_stage and bevstshx.status_c = 5 then bevstshx.instant_tm end) as multi_stage_comp_dttm
	,bevbase.event_source_c as event_source_c
	,max(case when bevstshx.status_c = 1 then durations.eventtime end) as total_unassigned_time
	,max(case when bevstshx.status_c = 2 then durations.eventtime end) as total_assigned_time
	,max(case when bevstshx.status_c = 3 then durations.eventtime end) as total_in_progress_time
	,max(case when bevstshx.status_c = 6 then durations.eventtime end) as total_delayed_time
	,max(case when bevstshx.status_c = 4 then durations.eventtime end) as total_on_hold_time
	,max(bevbase.dep_id) as department_id
	,max(hkruser.hkr_id) as hkr_id
	,case when max(bevstshx.delay_reason_c) is not null then 'y' else 'n' end as was_delayed_yn
	,max(round(bevbase.esc_resolution_time / 60 , 0 )) as total_esc_resolution_time
	,max(bevbase.hour_of_day_c) as hour_of_day

FROM
    CL_BEV_ALL bevbase
	left outer join CLARITY_POS loc
		on bevbase.eaf_id = loc.pos_id
	left outer join CL_BEV_EVENTS_ALL bevstshx
		on bevbase.record_id = bevstshx.record_id
	left outer join HKR_USER hkruser
		on bevbase.record_id = hkruser.record_id
	left outer join FIRST_STAGE_DIRTY firststagedirty
		on bevbase.first_stage_evt_id = firststagedirty.record_id
	left outer join MSC_DEF mscdef
		on bevbase.multi_stage_cln_id = mscdef.task_id
	left outer join DURATIONS durations
		on durations.record_id = bevstshx.record_id
		and bevstshx.status_c = durations.status_c
	left outer join VALID_PATIENT testpat
		on bevbase.ept_id = testpat.pat_id
where
	bevbase.event_type_c = 0
	and
		(testpat.is_valid_pat_yn <> 'n'
		or bevbase.ept_id is null)
group by
	bevbase.record_id
	,bevbase.active_c
	,bevbase.priority_c
	,bevbase.evs_type_c
	,bevbase.multi_stage_cln_id
	,bevbase.cur_stage_id
	,bevbase.stage_number
	,bevbase.first_stage_evt_id
	,firststagedirty.first_stage_dirty_dttm
	,bevbase.event_source_c