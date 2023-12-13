/*Copyright (C) 2011-2019 Epic Systems Corporation
********************************************************************************
TITLE:   V_SCHED_APPT
PURPOSE: Provides appointment information commonly needed in reports
AUTHOR:  Stuart Isaac
REVISION HISTORY:
*SOI 11/2010 DLG#191706 - created
*SOI 05/2011 200758 - remove references to external names, add dep-related
                      columns
*SOI 01/2012 224422 - add COMPLETED_STATUS_YN column
*SOI 04/2012 237213 - reduce complexity of cycle time columns
*SOI 12/2012 252303 - add CANCEL_LEAD_HOURS and HOUR_OF_DAY columns
*EYH 04/2015 349676 - don't reference V_ZC_CANCEL_REASON
*JLH 11/2015 397056 - add APPT_ARRIVAL_TIME
*EYH 12/2015 396967 - select correct cancel initiator when there is only 1 SDF record
*TWB 08/2016 430373 - Add UTC columns
*APC 12/2016 456450 - Add Online scheduled column, scheduling source
*CGA 05/2017 480737 - Add Echeckin columns
*ABF 03/2018 537018 - Add MyChart status columns
*JWG 11/2018 579991 - Fix PHONE_REM_STAT_NAME to actually return a value when there is one
*MAM 11/2018 575678 - Add LATE_CANCEL_YN and remove completed status dynamic parameter 
*TWB 11/2019 646561 - Remove reference to pe.APPT_CONF_PERS
*JAC 06/2020 697337 - Fix COMPLETED_STATUS_YN for EMFI customers. Put back explicit '2' check.
********************************************************************************/
SELECT
      mart.PAT_ENC_CSN_ID
    , mart.CONTACT_DATE
    , mart.PAT_ID
    , mart.APPT_STATUS_C
    , case
           when mart.APPT_STATUS_C = 1 and mart.SIGNIN_DTTM is not null then 'Present'
           when zcappt.NAME is not null then zcappt.NAME
           else
               case
                   when zcappt.APPT_STATUS_C is null then '*Unknown status'
                   else '*Unnamed status'
               end || ' [' || to_varchar( mart.APPT_STATUS_C) || ']'
      end as APPT_STATUS_NAME
    , mart.DEPARTMENT_ID
    , coalesce(dep.DEPARTMENT_NAME,
              case
                  when mart.DEPARTMENT_ID is null then '*Unspecified department'
                  else
                      case
                          when dep.DEPARTMENT_ID is null then '*Unknown department'
                          else '*Unnamed department'
                      end || ' [' || to_varchar( mart.DEPARTMENT_ID) || ']'
              end
      ) as DEPARTMENT_NAME
    , dep.specialty_dep_c as DEPT_SPECIALTY_C
    , case
          when dep.department_id is null then '*Unknown department'
          when dep.specialty is null then '*No specialty'
          else dep.specialty
      end as DEPT_SPECIALTY_NAME
    , dep.center_c as CENTER_C
    , coalesce(zccenter.name,
               case
                   when dep.department_id is null then '*Unknown department'
                   when dep.center_c is null then '*No center'
                   else '*Unknown center [' || dep.center_c || ']'
               end) as CENTER_NAME
    , dep.rev_loc_id as LOC_ID
    , coalesce(loc.loc_name,
               case
                   when dep.department_id is null then '*Unknown department'
                   when dep.rev_loc_id is null then '*No location'
                   else '*Unknown location [' || cast(dep.rev_loc_id as varchar(18)) || ']'
               end) as LOC_NAME
    , dep.serv_area_id as SERV_AREA_ID
    , coalesce(servarea.serv_area_name,
               case
                   when dep.department_id is null then '*Unknown department'
                   when dep.serv_area_id is null then '*No service area'
                   else '*Unknown service area [' || cast(dep.serv_area_id as varchar(18)) || ']'
               end) as SERV_AREA_NAME
    , mart.PROV_ID
    , case
        when mart.PROV_ID is null
            then '*Unspecified provider'
        else
            case
                when apptprov.prov_name is not null
                    then apptprov.prov_name
                when apptprov.prov_id is null
                    then '*Unknown provider'
                else '*Unnamed provider'
            end || ' [' || mart.prov_id || ']'
        end as PROV_NAME_WID
    , mart.PRC_ID
    , coalesce(prc.PRC_NAME,
               case
                    when mart.PRC_ID is null then '*Unspecified visit type'
                    else
                        case
                            when prc.PRC_ID is null then '*Unknown visit type'
                            else '*Unnamed visit type'
                    end || ' [' || mart.PRC_ID || ']'
                end
            ) as PRC_NAME
    , mart.APPT_MADE_DTTM
    , mart.APPT_MADE_DATE
    , mart.SAME_DAY_YN
    , mart.APPT_ENTRY_USER_ID
    , case
        when mart.APPT_ENTRY_USER_ID is null
            then '*Unspecified user'
        else
            case
                when entryemp.USER_ID is null
                    then '*Unknown user'
                when entryemp.NAME is null
                    then '*Unnamed user'
                else entryemp.NAME
            end || ' [' || mart.appt_entry_user_id || ']'
        end as APPT_ENTRY_USER_NAME_WID
    , mart.APPT_BLOCK_C
    , case
          when mart.APPT_BLOCK_C is null then null
          else coalesce(zcblock.NAME,
                        case
                            when zcblock.appt_block_c is null
                                then '*Unknown block'
                            else '*Unnamed block'
                        end || ' [' || to_varchar( mart.appt_block_c) || ']'
                    )
      end as APPT_BLOCK_NAME
    , mart.APPT_LENGTH
    , mart.APPT_DTTM
    , mart.SIGNIN_DTTM
    , mart.PAGED_DTTM
    , mart.BEGIN_CHECKIN_DTTM
    , mart.CHECKIN_DTTM
    , mart.ARVL_LIST_REMOVE_DTTM
    , mart.ROOMED_DTTM
    , mart.FIRST_ROOM_ASSIGN_DTTM
    , mart.NURSE_LEAVE_DTTM
    , mart.PHYS_ENTER_DTTM
    , mart.VISIT_END_DTTM
    , mart.CHECKOUT_DTTM
    , datediff(
        minute
        , mart.CHECKIN_DTTM
        , (LEAST((case when mart.arvl_list_remove_dttm >= mart.checkin_dttm then mart.arvl_list_remove_dttm else null end),
                        (case when mart.roomed_dttm >= mart.checkin_dttm then mart.roomed_dttm else null end),
                        (case when mart.first_room_assign_dttm >= mart.checkin_dttm then mart.first_room_assign_dttm else null end)
                        ) )
      ) as TIME_TO_ROOM_MINUTES
    , datediff(
        minute
        , (LEAST((case when mart.arvl_list_remove_dttm >= mart.checkin_dttm then mart.arvl_list_remove_dttm else null end),
                       (case when mart.roomed_dttm >= mart.checkin_dttm then mart.roomed_dttm else null end),
                       (case when mart.first_room_assign_dttm >= mart.checkin_dttm then mart.first_room_assign_dttm else null end)
                ) )
        , (case when mart.visit_end_dttm >=
                      (LEAST((case when mart.arvl_list_remove_dttm >= mart.checkin_dttm then mart.arvl_list_remove_dttm else null end),
                                   (case when mart.roomed_dttm >= mart.checkin_dttm then mart.roomed_dttm else null end),
                                   (case when mart.first_room_assign_dttm >= mart.checkin_dttm then mart.first_room_assign_dttm else null end)
                             ) )
                      and (mart.checkout_dttm is null or mart.visit_end_dttm <= mart.checkout_dttm)
                      then mart.visit_end_dttm
                when mart.checkout_dttm >=
                        (LEAST((case when mart.arvl_list_remove_dttm >= mart.checkin_dttm then mart.arvl_list_remove_dttm else null end),
                                     (case when mart.roomed_dttm >= mart.checkin_dttm then mart.roomed_dttm else null end),
                                     (case when mart.first_room_assign_dttm >= mart.checkin_dttm then mart.first_room_assign_dttm else null end)
                               ) )
                         and (mart.visit_end_dttm is null or mart.checkout_dttm < mart.visit_end_dttm)
                        then mart.checkout_dttm
                else null
          end)
      ) as TIME_IN_ROOM_MINUTES
    , DATEDIFF(
          minute
        , mart.CHECKIN_DTTM
        , case
            when mart.VISIT_END_DTTM >= mart.CHECKIN_DTTM
                 and (mart.CHECKOUT_DTTM is null
                    or mart.CHECKOUT_DTTM < mart.CHECKIN_DTTM
                    or mart.VISIT_END_DTTM <= mart.CHECKOUT_DTTM)
                 then mart.VISIT_END_DTTM
            when mart.CHECKOUT_DTTM >= mart.CHECKIN_DTTM
                 then mart.CHECKOUT_DTTM
            else null
          end) AS CYCLE_TIME_MINUTES
    , mart.APPT_CANC_USER_ID
    , case
        when mart.APPT_CANC_USER_ID is null then null
        else
            case
                when cancemp.USER_ID is null
                    then '*Unknown user'
                when cancemp.NAME is null
                    then '*Unnamed user'
                else cancemp.NAME
            end || ' [' || mart.appt_canc_user_id || ']'
        end as APPT_CANC_USER_NAME_WID
    , mart.APPT_CANC_DTTM
    , mart.APPT_CANC_DATE
    , mart.CANCEL_REASON_C
    , case
          when mart.CANCEL_REASON_C is null then null
          else coalesce(canc.NAME,
                        case
                            when canc.cancel_reason_c is null
                                then '*Unknown cancel reason'
                            else '*Unnamed cancel reason'
                        end || ' [' || to_varchar( mart.cancel_reason_c) || ']'
                    )
      end as CANCEL_REASON_NAME
    , case
          when mart.CANCEL_REASON_C is null then null
          when canc.CANCEL_REASON_C is null then '*Unknown cancel reason [' || to_varchar( mart.cancel_reason_c) || ']'
          when (select count(PAT_INIT_CANC_C) from {{ref('pat_init_canc_base')}}
                where PAT_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 then 'PATIENT'
          when (select count(PROV_INIT_CANC_C) from {{ref('prov_init_canc_base')}}
                where PROV_INIT_CANC_C=canc.CANCEL_REASON_C) >= 1 then 'PROVIDER'
          else 'OTHER'
      end as CANCEL_INITIATOR
    , mart.SAME_DAY_CANC_YN
    , mart.APPT_SERIAL_NUM
    , mart.RESCHED_APPT_CSN_ID
    , mart.REFERRAL_ID
    , mart.REFERRAL_REQ_YN
    , mart.REFERRING_PROV_ID
    , case
        when mart.REFERRING_PROV_ID is null then null
        else
            case
                when refprov.prov_name is not null
                    then refprov.prov_name
                when refprov.prov_id is null
                    then '*Unknown provider'
                else '*Unnamed provider'
            end || ' [' || mart.referring_prov_id || ']'
        end as REFERRING_PROV_NAME_WID
    , mart.ACCOUNT_ID
    , coalesce(mart.COVERAGE_ID, hsp.COVERAGE_ID) as COVERAGE_ID
    , mart.CHARGE_SLIP_NUMBER
    , mart.HSP_ACCOUNT_ID
    , mart.APPT_CONF_STAT_C
    , zcconf.NAME as APPT_CONF_STAT_NAME
    , mart.APPT_CONF_USER_ID
    ,case when confemp.USER_ID is null then '*Unknown user'
               else coalesce(confemp.NAME, '*Unnamed user')
               end || ' [' || mart.APPT_CONF_USER_ID || ']' AS APPT_CONF_USER_NAME
    , mart.APPT_CONF_DTTM
    , mart.SCHED_FROM_KIOSK_ID
    , case
          when mart.SCHED_FROM_KIOSK_ID is null then null
          else coalesce(schedlws.WORKSTATION_NAME,
                        case
                            when schedlws.WORKSTN_IDENTIFIER is null then '*Unknown kiosk'
                            else '*Unnamed kiosk [' || schedlws.WORKSTN_IDENTIFIER || ']'
                        end
                    )
      end as SCHED_FROM_KIOSK_NAME
    , mart.CHECK_IN_KIOSK_ID
    , case
          when mart.CHECK_IN_KIOSK_ID is null then null
          else coalesce(chkinlws.WORKSTATION_NAME,
                        case
                            when chkinlws.WORKSTN_IDENTIFIER is null then '*Unknown kiosk'
                            else '*Unnamed kiosk [' || chkinlws.WORKSTN_IDENTIFIER || ']'
                        end
                    )
      end as CHECK_IN_KIOSK_NAME
    , mart.CHECK_OUT_KIOSK_ID
    , case
          when mart.CHECK_OUT_KIOSK_ID is null then null
          else coalesce(chkoutlws.WORKSTATION_NAME,
                        case
                            when chkoutlws.WORKSTN_IDENTIFIER is null then '*Unknown kiosk'
                            else '*Unnamed kiosk [' || chkoutlws.WORKSTN_IDENTIFIER || ']'
                        end
                    )
      end as CHECK_OUT_KIOSK_NAME
    , mart.IP_DOC_CONTACT_CSN
    , mart.WALK_IN_YN
    , mart.SEQUENTIAL_YN
    , mart.CNS_WARNING_OVERRIDDEN_YN
    , mart.OVERBOOKED_YN
    , mart.OVERRIDE_YN
    , mart.UNAVAILABLE_TIME_YN
    , mart.NUMBER_OF_CALLS
    , mart.CHANGE_CNT
    , mart.JOINT_APPT_YN
    , mart.CM_CT_OWNER_ID
    , mart.PHONE_REM_STAT_C
    , case when mart.PHONE_REM_STAT_C is null then null
           when zcrem.PHONE_REM_STAT_C is null then '*Unknown phone reminder status'
           when zcrem.NAME is null then '*Unnamed phone reminder status [' || to_varchar( zcrem.PHONE_REM_STAT_C) || ']'
           else zcrem.NAME
      end as PHONE_REM_STAT_NAME
    , mart.COPAY_DUE
    , mart.COPAY_COLLECTED
    , mart.COPAY_USER_ID
    , case when mart.COPAY_USER_ID is null then null
           else
             case
                when copayemp.USER_ID is null
                    then '*Unknown user'
                when copayemp.NAME is null
                    then '*Unnamed user'
                else copayemp.NAME
            end || ' [' || mart.copay_user_id || ']'
      end as COPAY_USER_NAME_WID
    , case when mart.APPT_STATUS_C = 2 then 'Y'
           when (select count(COMPLETE_STATUS_C) from {{ref('appt_complete_stat_base')}}
           where COMPLETE_STATUS_C = mart.APPT_STATUS_C) >= 1 then 'Y'
           else 'N'
      end as COMPLETED_STATUS_YN
    , date_part(hour, mart.APPT_DTTM) as HOUR_OF_DAY
    , datediff(minute, mart.APPT_CANC_DTTM, mart.APPT_DTTM) / 60 as CANCEL_LEAD_HOURS
    , mart.APPT_ARRIVAL_DTTM
    , mart.APPT_UTC_DTTM
    , mart.APPT_CANC_UTC_DTTM
    , mart.APPT_MADE_UTC_DTTM
    , mart.APPT_SCHED_SOURCE_C
    , case when mart.APPT_SCHED_SOURCE_C is null then null
            else
                case
                    when aud.ES_AUDIT_FROM_C is null then '*Unknown scheduling source'
                    when aud.NAME is null then '*Unnamed scheduling source [' || to_varchar( aud.ES_AUDIT_FROM_C) || ']'
                    else aud.NAME
                end
      end as APPT_SCHED_SOURCE_NAME
    , mart.PAT_ONLINE_YN
    , mart.ECHKIN_STATUS_C
    , case when mart.ECHKIN_STATUS_C is null then null
            else
                 case
                     when echkinstat.ECHKIN_STEP_STAT_C is null then '*Unknown eCheck-in status'
                     when echkinstat.NAME is null then '*Unnamed eCheck-in status [' || to_varchar( echkinstat.ECHKIN_STEP_STAT_C) || ']'
                     else echkinstat.NAME
                 end
      end as ECHKIN_STATUS_NAME
    , mart.PAT_SCHED_MYC_STAT_C
    , case when mart.PAT_SCHED_MYC_STAT_C is null then null
            else
                 case
                     when mychartstat.MYCHART_STATUS_C is null then '*Unknown MyChart activation status'
                     when mychartstat.NAME is null then '*Unnamed MyChart activation status [' || to_varchar( mychartstat.MYCHART_STATUS_C) || ']'
                     else mychartstat.NAME
                 end
      end as PAT_SCHED_MYC_STAT_NAME
    , mart.LATE_CANCEL_YN
FROM
    {{ref('f_sched_appt_base')}} mart
    left outer join {{ref('hsp_account_base')}} hsp on mart.HSP_ACCOUNT_ID = hsp.HSP_ACCOUNT_ID
    left outer join {{ref('lkp_clr_appt_status_base')}} zcappt on mart.APPT_STATUS_C = zcappt.APPT_STATUS_C
    left outer join {{ref('pat_enc_base')}} pe on mart.PAT_ENC_CSN_ID = pe.PAT_ENC_CSN_ID
    left outer join {{ref('clarity_emp_base')}} confemp on mart.APPT_CONF_USER_ID = confemp.USER_ID
    left outer join {{ref('clarity_dep_base')}} dep on mart.DEPARTMENT_ID = dep.DEPARTMENT_ID
    left outer join {{ref('lkp_clr_center_base')}} zccenter on dep.center_c = zccenter.center_c
    left outer join {{ref('clarity_loc_base')}} loc on dep.rev_loc_id = loc.loc_id
    left outer join {{ref('clarity_sa_base')}} servarea on dep.serv_area_id = servarea.serv_area_id
    left outer join {{ref('clarity_ser_base')}} apptprov on mart.PROV_ID = apptprov.PROV_ID
    left outer join {{ref('clarity_prc_base')}} prc on mart.PRC_ID = prc.PRC_ID
    left outer join {{ref('clarity_emp_base')}} entryemp on mart.APPT_ENTRY_USER_ID = entryemp.USER_ID
    left outer join {{ref('lkp_clr_appt_block_base')}} zcblock on mart.APPT_BLOCK_C = zcblock.APPT_BLOCK_C
    left outer join {{ref('clarity_emp_base')}} cancemp on mart.APPT_CANC_USER_ID = cancemp.USER_ID
    left outer join {{ref('lkp_clr_cancel_reason_base')}} canc on mart.CANCEL_REASON_C = canc.CANCEL_REASON_C
    left outer join {{ref('clarity_ser_base')}} refprov on mart.REFERRING_PROV_ID = refprov.PROV_ID
    left outer join {{ref('clarity_lws_base')}} schedlws on mart.SCHED_FROM_KIOSK_ID = schedlws.workstation_id
    left outer join {{ref('clarity_lws_base')}} chkinlws on mart.CHECK_IN_KIOSK_ID = chkinlws.workstation_id
    left outer join {{ref('clarity_lws_base')}} chkoutlws on mart.CHECK_OUT_KIOSK_ID = chkoutlws.workstation_id
    left outer join {{ref('lkp_clr_appt_conf_stat_base')}} zcconf on mart.APPT_CONF_STAT_C = zcconf.APPT_CONF_STAT_C
    left outer join {{ref('lkp_clr_phone_rem_stat_base')}} zcrem on mart.PHONE_REM_STAT_C = zcrem.PHONE_REM_STAT_C
    left outer join {{ref('clarity_emp_base')}} copayemp on mart.COPAY_USER_ID = copayemp.USER_ID
    left outer join {{ref('lkp_clr_es_audit_from_base')}} aud on mart.APPT_SCHED_SOURCE_C = aud.ES_AUDIT_FROM_C
    left outer join {{ref('lkp_clr_echkin_step_stat_base')}} echkinstat ON mart.ECHKIN_STATUS_C = echkinstat.ECHKIN_STEP_STAT_C
    left outer join {{ref('lkp_clr_mychart_status_base')}} mychartstat ON mart.PAT_SCHED_MYC_STAT_C = mychartstat.MYCHART_STATUS_C