with CLARITY_ADT as
(
    select * 
    from {{ref('clarity_adt_base')}} 
),
PEND_ACTION as
(
    select * 
    from {{ref('pend_action_base')}} 
),
CLARITY_DEP as
(
    select * 
    from {{ref('clarity_dep_base')}} 
),
PAT_ENC_HSP as
(
    select * 
    from {{ref('pat_enc_hsp_base')}} 
),
PAT_ENC_HSP_2 as
(
    select * 
    from {{ref('pat_enc_hsp_2_base')}} 
),
BED_PLAN_HX as
(
    select * 
    from {{ref('bed_plan_hx_base')}} 
),
TXPORT_REQ_INFO as
(
    select * 
    from {{ref('txport_req_info_base')}} 
),
TXPORT_EVENTS as
(
    select * 
    from {{ref('txport_events_base')}} 
),
VALID_PATIENT as
(
    select * 
    from {{ref('valid_patient_base')}} 
),
DISCHARGE_DISPOSITION_MAP as
(
    select * 
    from {{ref('discharge_disposition_map_base')}} 
),
HL_REQ_STATUS_AUDIT as
(
    select * 
    from {{ref('hl_req_status_audit_base')}} 
),
HL_REQ_INFO as
(
    select * 
    from {{ref('hl_req_info_base')}} 
)
,
ClarityAdtSourceTable AS (
    SELECT CLARITY_ADT.PAT_ENC_CSN_ID,
                        CLARITY_ADT.EFFECTIVE_TIME,
                        CLARITY_ADT.TO_BASE_CLASS_C
                    FROM CLARITY_ADT
                    WHERE CLARITY_ADT.EVENT_TYPE_C IN ( 3, 5 )     --xfer in or pt update
                    AND CLARITY_ADT.EVENT_SUBTYPE_C <> 2         --not canceled
                    AND CLARITY_ADT.TO_BASE_CLASS_C IN ( 1, 4 )
),
ClarityAdtPivotTable AS(
    SELECT * from ClarityAdtSourceTable
          PIVOT( MIN( ClarityAdtSourceTable.EFFECTIVE_TIME )
            FOR ClarityAdtSourceTable.TO_BASE_CLASS_C IN ( 1, 4 ) )
),
IpEventSub AS(
    SELECT DISTINCT CLARITY_ADT.PAT_ENC_CSN_ID,CLARITY_ADT.EVENT_ID   --FIXED
      FROM CLARITY_ADT
          INNER JOIN ClarityAdtPivotTable on
              ClarityAdtPivotTable."1" = CLARITY_ADT.EFFECTIVE_TIME
              and
              ClarityAdtPivotTable.PAT_ENC_CSN_ID = CLARITY_ADT.PAT_ENC_CSN_ID        
      WHERE 
        CLARITY_ADT.EVENT_TYPE_C IN ( 3, 5 )     --xfer in or pt update
        AND CLARITY_ADT.EVENT_SUBTYPE_C <> 2         --not canceled
        AND CLARITY_ADT.TO_BASE_CLASS_C = 1  --IP
)
,
ObsEventSub AS(
    SELECT CLARITY_ADT.PAT_ENC_CSN_ID, CLARITY_ADT.EVENT_ID   --FIXED
      FROM CLARITY_ADT
          INNER JOIN ClarityAdtPivotTable on
                  ClarityAdtPivotTable."4" = CLARITY_ADT.EFFECTIVE_TIME 
                  and
                  ClarityAdtPivotTable.PAT_ENC_CSN_ID = CLARITY_ADT.PAT_ENC_CSN_ID    
      WHERE
        CLARITY_ADT.EVENT_TYPE_C IN ( 3, 5 )     --xfer in or pt update
        AND CLARITY_ADT.EVENT_SUBTYPE_C <> 2         --not canceled
        AND CLARITY_ADT.TO_BASE_CLASS_C = 4  --Obs
),
FirstIpObsEffectiveTimeCte AS
(

    SELECT 
        ClarityAdtPivotTable.PAT_ENC_CSN_ID,
        COALESCE( ClarityAdtPivotTable."1", ClarityAdtPivotTable."4" ) FirstIpObsEffectiveTime,
        COALESCE( IpEventSub.EVENT_ID, ObsEventSub.EVENT_ID ) EVENT_ID
    FROM ClarityAdtPivotTable
      LEFT JOIN  IpEventSub
          on IpEventSub.PAT_ENC_CSN_ID = ClarityAdtPivotTable.PAT_ENC_CSN_ID
      LEFT JOIN  ObsEventSub
          on ObsEventSub.PAT_ENC_CSN_ID = ClarityAdtPivotTable.PAT_ENC_CSN_ID
),
CompletedPndsCte AS  --bed request completed, was a xfer in ADT event that isn't canceled, from ED
(
  SELECT PEND_ACTION.PEND_ID
    FROM PEND_ACTION
      INNER JOIN CLARITY_ADT TxIn
        ON PEND_ACTION.LINKED_EVENT_ID = TxIn.EVENT_ID
      LEFT OUTER JOIN CLARITY_ADT TxOut
        ON TxIn.XFER_EVENT_ID = TxOut.EVENT_ID
      LEFT OUTER JOIN CLARITY_DEP OutDep
        ON TxOut.DEPARTMENT_ID = OutDep.DEPARTMENT_ID
    WHERE TxIn.EVENT_TYPE_C = 3
      AND TxIn.EVENT_SUBTYPE_C IN ( 1, 3 )
      AND OutDep.ADT_UNIT_TYPE_C = 1
      AND PEND_ACTION.COMPLETED_YN = 'Y'  
),
     CanceledPndsCte AS  --bed request canceled, is of type xfer, and pt was discharged from ED
(
  SELECT PEND_ACTION.PEND_ID
    FROM PEND_ACTION
	  INNER JOIN PAT_ENC_HSP
        ON PEND_ACTION.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
      INNER JOIN CLARITY_DEP
        ON PAT_ENC_HSP.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
          AND CLARITY_DEP.ADT_UNIT_TYPE_C = 1
    WHERE PEND_ACTION.DELETE_TIME IS NOT NULL
      AND COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) = 'N'
      AND PEND_ACTION.PEND_EVENT_TYPE_C = 3  
),
v_adt_ed_admits_cte as (
SELECT DISTINCT FirstIpObsEffectiveTimeCte.EVENT_ID,
       PEND_ACTION.PEND_ID,
       PEND_ACTION.PAT_ID,
       PEND_ACTION.PAT_ENC_CSN_ID,
       TransportSub.TRANSPORT_ID TNP_ID,
       CASE WHEN PEND_ACTION.AUTOCREATE_SOURCE_C = 6 THEN COALESCE ( BedPlanHxSub.OrderDttm, FirstIpObsEffectiveTimeCte.FirstIpObsEffectiveTime )
            WHEN BedPlanHxSub.RequestedDttm IS NULL THEN FirstIpObsEffectiveTimeCte.FirstIpObsEffectiveTime
            WHEN FirstIpObsEffectiveTimeCte.FirstIpObsEffectiveTime < BedPlanHxSub.RequestedDttm THEN FirstIpObsEffectiveTimeCte.FirstIpObsEffectiveTime
            ELSE BedPlanHxSub.RequestedDttm END BOARD_START_DTTM,
       CASE WHEN PEND_ACTION.COMPLETED_YN = 'Y' THEN COALESCE( HlrTxportEventsSub.InProgressDttm, TxportEventsSub.InProgressDttm, PEND_ACTION.DELETE_TIME )
            ELSE PAT_ENC_HSP.HOSP_DISCH_TIME END BOARD_END_DTTM,
       EdAdmissionSub.DEPARTMENT_ID,
       EdAdmissionSub.REV_LOC_ID,
       EdAdmissionSub.SERV_AREA_ID,
       FirstIpObsEffectiveTimeCte.FirstIpObsEffectiveTime TO_IP_CLASS_DTTM,
       COALESCE( PEND_ACTION.OFF_SERVICE_YN, 'N' ) OFF_SERVICE_YN,
       PEND_ACTION.LINKED_EVENT_ID XFER_IN_EVENT_ID,
       COALESCE( PEND_ACTION.COMPLETED_YN, 'N' ) COMPLETED_YN,
       CASE WHEN DischDispMap.RPT_DISCHARGE_DISPOSITION_C = '1000' THEN 'Y'
            ELSE 'N' END DISCH_READMIT_YN,
       HlrTransportSub.TXPORT_HLR_ID
  FROM PEND_ACTION
    INNER JOIN ( SELECT CompletedPndsCte.PEND_ID
                   FROM CompletedPndsCte
                 UNION ALL
                 SELECT CanceledPndsCte.PEND_ID
                   FROM CanceledPndsCte ) PndFilterSub
      ON PEND_ACTION.PEND_ID = PndFilterSub.PEND_ID
    INNER JOIN ( SELECT BED_PLAN_HX.PEND_ID,
                        MIN( CASE WHEN BED_PLAN_HX.UPDATE_TYPE_C = 2 THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) RequestedDttm,
                        MIN( CASE WHEN BED_PLAN_HX.ORD_ID IS NOT NULL THEN BED_PLAN_HX.UPDATE_INST_LOCAL_DTTM ELSE NULL END ) OrderDttm
                   FROM BED_PLAN_HX
                   GROUP BY BED_PLAN_HX.PEND_ID ) BedPlanHxSub  --This subquery gets the latest Requested and RTP status instants for PNDs
      ON PEND_ACTION.PEND_ID = BedPlanHxSub.PEND_ID
    LEFT OUTER JOIN FirstIpObsEffectiveTimeCte
      ON PEND_ACTION.PAT_ENC_CSN_ID = FirstIpObsEffectiveTimeCte.PAT_ENC_CSN_ID
    LEFT OUTER JOIN CLARITY_ADT TxIn
      ON PEND_ACTION.LINKED_EVENT_ID = TxIn.EVENT_ID
    LEFT OUTER JOIN CLARITY_DEP InDep
      ON PEND_ACTION.UNIT_ID = InDep.DEPARTMENT_ID
    LEFT OUTER JOIN CLARITY_ADT TxOut
      ON TxIn.XFER_EVENT_ID = TxOut.EVENT_ID
    INNER JOIN PAT_ENC_HSP
      ON PEND_ACTION.PAT_ENC_CSN_ID = PAT_ENC_HSP.PAT_ENC_CSN_ID
    INNER JOIN PAT_ENC_HSP_2
      ON PAT_ENC_HSP.PAT_ENC_CSN_ID = PAT_ENC_HSP_2.PAT_ENC_CSN_ID
        AND COALESCE( PAT_ENC_HSP_2.LEGACY_ADT_ENC_YN, 'N' ) <> 'Y'
    INNER JOIN ( SELECT CLARITY_ADT.EVENT_ID,
                        CLARITY_DEP.DEPARTMENT_ID,
                        CLARITY_DEP.REV_LOC_ID,
                        CLARITY_DEP.SERV_AREA_ID
                   FROM CLARITY_ADT
                     INNER JOIN CLARITY_DEP
                       ON CLARITY_ADT.DEPARTMENT_ID = CLARITY_DEP.DEPARTMENT_ID
                         AND CLARITY_DEP.ADT_UNIT_TYPE_C = 1 ) EdAdmissionSub  --This subquery finds admission encounters into an ED unit
      ON PAT_ENC_HSP_2.HSP_ADM_EVENT_ID = EdAdmissionSub.EVENT_ID
    LEFT OUTER JOIN ( SELECT MIN( TXPORT_REQ_INFO.TRANSPORT_ID ) TRANSPORT_ID,
                             TXPORT_REQ_INFO.TXPORT_XFER_PND_ID
                        FROM TXPORT_REQ_INFO
                        WHERE TXPORT_REQ_INFO.CURRENT_STATUS_C = 5  --completed
                          AND TXPORT_REQ_INFO.PARENT_REQUEST_ID IS NULL  --parent request only
                          AND TXPORT_REQ_INFO.TXPORT_TYPE_C = 1  --patient transport
                        GROUP BY TXPORT_REQ_INFO.TXPORT_XFER_PND_ID ) TransportSub
      ON PEND_ACTION.PEND_ID = TransportSub.TXPORT_XFER_PND_ID
    LEFT OUTER JOIN ( SELECT TXPORT_EVENTS.TXPORT_ID,
                             MAX( CASE WHEN TXPORT_EVENTS.ASGN_STATUS_C = 3 THEN TXPORT_EVENTS.EVENT_INSTANT_LOCAL_DTTM ELSE NULL END ) InProgressDttm
                        FROM TXPORT_EVENTS
                        GROUP BY TXPORT_EVENTS.TXPORT_ID ) TxportEventsSub
      ON TransportSub.TRANSPORT_ID = TxportEventsSub.TXPORT_ID
    LEFT OUTER JOIN ( SELECT MIN( HL_REQ_INFO.HLR_ID ) TXPORT_HLR_ID,
                             HL_REQ_INFO.REQ_PEND_ID
                        FROM HL_REQ_INFO
                        WHERE HL_REQ_INFO.REQ_STATUS_C = 35  --Completed
                          AND HL_REQ_INFO.HLR_TYPE_C = 1  --Request
                          AND HL_REQ_INFO.REQ_TASK_SUBTYPE_C = 1  --Patient Transport
                        GROUP BY HL_REQ_INFO.REQ_PEND_ID ) HlrTransportSub
      ON PEND_ACTION.PEND_ID = HlrTransportSub.REQ_PEND_ID
    LEFT OUTER JOIN ( SELECT HL_REQ_STATUS_AUDIT.HLR_ID,
                             MAX( CASE WHEN HL_REQ_STATUS_AUDIT.STATUS_C = 25 THEN HL_REQ_STATUS_AUDIT.EVENT_LOCAL_DTTM ELSE NULL END ) InProgressDttm
                        FROM HL_REQ_STATUS_AUDIT
                        GROUP BY HL_REQ_STATUS_AUDIT.HLR_ID ) HlrTxportEventsSub
      ON HlrTransportSub.TXPORT_HLR_ID = HlrTxportEventsSub.HLR_ID
    INNER JOIN VALID_PATIENT
      ON PEND_ACTION.PAT_ID = VALID_PATIENT.PAT_ID
        AND VALID_PATIENT.IS_VALID_PAT_YN = 'Y'
    LEFT OUTER JOIN DISCHARGE_DISPOSITION_MAP DischDispMap
      ON PAT_ENC_HSP.DISCH_DISP_C =  DischDispMap.ADT_DISCHARGE_DISPOSITION_C
      AND DischDispMap.FACILITY_ID = 1
      AND DischDispMap.RPT_DISCHARGE_DISPOSITION_C = '1000'
  WHERE FirstIpObsEffectiveTimeCte.FirstIpObsEffectiveTime IS NOT NULL
    AND InDep.OR_UNIT_TYPE_C IS NULL  --not OR
    AND COALESCE( InDep.IS_PERIOP_DEP_YN, 'N' ) = 'N'  --not OR
    AND COALESCE( InDep.ADT_UNIT_TYPE_C, 0 ) <> 1  --Not going to ED
)

SELECT * 
FROM v_adt_ed_admits_cte
