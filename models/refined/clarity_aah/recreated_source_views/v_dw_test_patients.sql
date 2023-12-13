with PATIENT as(
    select * 
    from {{ref('patient_base')}} 
),
PATIENT_3 as(
    select * 
    from {{ref('patient_3_base')}} 
)

select 
    PATIENT.PAT_ID
	,PATIENT.PAT_MRN_ID
	,PATIENT.PAT_NAME
	,PATIENT.PAT_LAST_NAME
	,PATIENT.PAT_FIRST_NAME
	,PATIENT.REC_CREATE_DATE
	,PATIENT_3.IS_TEST_PAT_YN													--002
from PATIENT
	left outer join PATIENT_3 on PATIENT.PAT_ID = PATIENT_3.PAT_ID				--002
where 
	PATIENT.PAT_NAME like 'zz%' or 
	PATIENT.PAT_NAME like 'ztest%' or 
	PATIENT.PAT_NAME like 'test,%' or 
	PATIENT.PAT_NAME like 'ime,%' or 
	PATIENT.PAT_NAME like '%research%' or
	PATIENT.PAT_NAME like 'meeting,%' or
	PATIENT.PAT_NAME like 'drug,%' or
	PATIENT.PAT_NAME like '%drugstudy%' or
	PATIENT.PAT_NAME like 'donotbook%' or
	PATIENT.PAT_NAME like 'DONOT,BOOK' or
	PATIENT.PAT_NAME like 'DONOT USE,CORPORATE BILLING' or
	PATIENT.PAT_NAME like '%do,not book%' or
	PATIENT.PAT_NAME like '%unavailable%' or
	PATIENT.PAT_NAME like '%do,not use%' or
	PATIENT.PAT_NAME like '*%do,not schedule%' or 
	PATIENT.PAT_NAME like '%staff,out%' or
	PATIENT.PAT_NAME like '%ztest%' or
	PATIENT.PAT_NAME like '%zz,test%' OR
	PATIENT.PAT_NAME like '%ON-CALL,%' OR
	PATIENT.PAT_NAME LIKE '%ON CALL,%' OR
	PATIENT.PAT_NAME LIKE 'BOOK,DON%' OR
	PATIENT.PAT_NAME LIKE 'DO NOT BOOK,%' OR
	PATIENT.PAT_NAME LIKE 'STAFF,MEETING%' OR
	PATIENT.PAT_NAME LIKE 'TRAVEL,TO' OR
	PATIENT.PAT_NAME LIKE 'IDN,%' OR
	PATIENT.PAT_NAME like 'test test%' 
	or																			--001 Added PAT_FIRST_NAME to the search
	PATIENT.PAT_FIRST_NAME like 'zz%' or
	PATIENT.PAT_FIRST_NAME like 'ztest%' or 
	PATIENT.PAT_FIRST_NAME like 'test,%' or 
	PATIENT.PAT_FIRST_NAME like 'ime,%' or 
	PATIENT.PAT_FIRST_NAME like '%research%' or
	PATIENT.PAT_FIRST_NAME like 'meeting,%' or
	PATIENT.PAT_FIRST_NAME like 'drug,%' or
	PATIENT.PAT_FIRST_NAME like '%drugstudy%' or
	PATIENT.PAT_FIRST_NAME like 'donotbook%' or
	PATIENT.PAT_FIRST_NAME like 'DONOT,BOOK' or
	PATIENT.PAT_FIRST_NAME like 'DONOT USE,CORPORATE BILLING' or
	PATIENT.PAT_FIRST_NAME like '%do,not book%' or
	PATIENT.PAT_FIRST_NAME like '%unavailable%' or
	PATIENT.PAT_FIRST_NAME like '%do,not use%' or
	PATIENT.PAT_FIRST_NAME like '*%do,not schedule%' or 
	PATIENT.PAT_FIRST_NAME like '%staff,out%' or
	PATIENT.PAT_FIRST_NAME like '%ztest%' or
	PATIENT.PAT_FIRST_NAME like '%zz,test%' OR
	PATIENT.PAT_FIRST_NAME like '%ON-CALL,%' OR
	PATIENT.PAT_FIRST_NAME LIKE '%ON CALL,%' OR
	PATIENT.PAT_FIRST_NAME LIKE 'BOOK,DON%' OR
	PATIENT.PAT_FIRST_NAME LIKE 'DO NOT BOOK,%' OR
	PATIENT.PAT_FIRST_NAME LIKE 'STAFF,MEETING%' OR
	PATIENT.PAT_FIRST_NAME LIKE 'TRAVEL,TO' OR
	PATIENT.PAT_FIRST_NAME LIKE 'IDN,%' OR
	PATIENT.PAT_FIRST_NAME like 'test test%'
	or 																			--001 Added PAT_LAST_NAME to the search
	PATIENT.PAT_LAST_NAME like 'zz%' or 
	PATIENT.PAT_LAST_NAME like 'ztest%' or 
	PATIENT.PAT_LAST_NAME like 'test,%' or 
	PATIENT.PAT_LAST_NAME like 'ime,%' or 
	PATIENT.PAT_LAST_NAME like '%research%' or
	PATIENT.PAT_LAST_NAME like 'meeting,%' or
	PATIENT.PAT_LAST_NAME like 'drug,%' or
	PATIENT.PAT_LAST_NAME like '%drugstudy%' or
	PATIENT.PAT_LAST_NAME like 'donotbook%' or
	PATIENT.PAT_LAST_NAME like 'DONOT,BOOK' or
	PATIENT.PAT_LAST_NAME like 'DONOT USE,CORPORATE BILLING' or
	PATIENT.PAT_LAST_NAME like '%do,not book%' or
	PATIENT.PAT_LAST_NAME like '%unavailable%' or
	PATIENT.PAT_LAST_NAME like '%do,not use%' or
	PATIENT.PAT_LAST_NAME like '*%do,not schedule%' or 
	PATIENT.PAT_LAST_NAME like '%staff,out%' or
	PATIENT.PAT_LAST_NAME like '%ztest%' or
	PATIENT.PAT_LAST_NAME like '%zz,test%' OR
	PATIENT.PAT_LAST_NAME like '%ON-CALL,%' OR
	PATIENT.PAT_LAST_NAME LIKE '%ON CALL,%' OR
	PATIENT.PAT_LAST_NAME LIKE 'BOOK,DON%' OR
	PATIENT.PAT_LAST_NAME LIKE 'DO NOT BOOK,%' OR
	PATIENT.PAT_LAST_NAME LIKE 'STAFF,MEETING%' OR
	PATIENT.PAT_LAST_NAME LIKE 'TRAVEL,TO' OR
	PATIENT.PAT_LAST_NAME LIKE 'IDN,%' OR
	PATIENT.PAT_LAST_NAME like 'test test%' 
	or
	(PATIENT.PAT_NAME like '%meeting%' AND (PATIENT.PAT_NAME like'%DR%' OR PATIENT.BIRTH_DATE = '1999-01-01 00:00:00.000' OR PATIENT.SSN in ('111-11-1111','000-00-0000','999-99-9999')))
	or
	PATIENT_3.IS_TEST_PAT_YN = 'Y'												--002
