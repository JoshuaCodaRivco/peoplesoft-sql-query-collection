
WITH 
-- Output1 isolates effdt by employee greater than or equal to 3 years.
output1 AS (
  SELECT
    *
  FROM
  (
    SELECT
      *
    FROM
    (
      SELECT 
        o1.*, 
        CASE  WHEN EXTRACT (MONTH FROM effdt)  BETWEEN 7 AND 12
                THEN EXTRACT(YEAR FROM effdt)                              
              WHEN EXTRACT (MONTH FROM effdt)  BETWEEN 1 AND 6
                THEN EXTRACT(YEAR FROM ADD_MONTHS(effdt,-6))
          END fin_yr,
        CASE  WHEN EXTRACT (MONTH FROM SYSDATE)  BETWEEN 7 AND 12
                THEN EXTRACT(YEAR FROM SYSDATE)                              
              WHEN EXTRACT (MONTH FROM SYSDATE)  BETWEEN 1 AND 6
                THEN EXTRACT(YEAR FROM ADD_MONTHS(SYSDATE,-6))
          END cur_fin_yr,   
        row_number () OVER (
                PARTITION BY emplid, effdt
                ORDER BY effseq DESC, effdt DESC
              ) rn
      FROM   sysadm.ps_job o1
    )
    -- REGEXP_LIKE Department of Public Social Services
    WHERE rn = 1 AND REGEXP_LIKE (deptid, '^((51001?[0-5])|985)') AND effdt >= ADD_MONTHS(TRUNC(SYSDATE), -54)
    ORDER BY effdt DESC
  )
  WHERE  fin_yr >= (cur_fin_yr - 3) AND action = 'PRO'
)

SELECT
    deptid,
    emplid,
    t4.first_name,
    t4.middle_name,
    t4.last_name,
    jobcode,
    t5.descr AS jobtitle,
    t2.event,
    t3.event_reason,
    effdt AS event_dt,
    fin_yr

FROM output1 t1
LEFT JOIN 
(
  SELECT
    *
  FROM
  (
    SELECT
        action,
        action_descr AS event,
        ROW_NUMBER() OVER ( PARTITION BY action ORDER BY effdt DESC) AS rn
    FROM sysadm.ps_action_tbl
  )
  WHERE rn=1
) t2 ON (t1.action = t2.action)
LEFT JOIN 
(
  SELECT 
    *
  FROM
  (
    SELECT
        action,
        action_reason,
        descr AS event_reason,
        ROW_NUMBER() OVER ( PARTITION BY action_reason, action ORDER BY effdt DESC) AS rn
    FROM sysadm.ps_actn_reason_tbl
  )
  WHERE rn=1

) t3 ON (t1.action = t3.action) AND (t1.action_reason = t3.action_reason)
LEFT JOIN 
(
    SELECT
      *
    FROM 
    (
      SELECT
        emplid AS emplid_,
        name_type,
        first_name,
        middle_name,
        last_name,
        ROW_NUMBER() OVER ( PARTITION BY emplid ORDER BY effdt DESC) AS rn
      FROM sysadm.ps_names
    )
    -- Mutliple rows selected primary name.
    WHERE rn=1
) t4 ON (t1.emplid = t4.emplid_)
LEFT JOIN
(
    SELECT
        jobcode AS jobcode_,
        descr,
        rn
    FROM (
        SELECT
            effdt,
            jobcode,
            descr,
            ROW_NUMBER() OVER ( PARTITION BY jobcode ORDER BY effdt DESC) AS rn
        FROM sysadm.ps_jobcode_tbl
        )
        WHERE RN=1        
) t5 ON (t1.jobcode = t5.jobcode_)