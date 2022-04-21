-- Employee histroy
WITH
-- Output1 return LEAVE OF ABSENSE employees.
output1 AS (
  SELECT
    *
  FROM
  (
    SELECT
      z.*,
      ROW_NUMBER() OVER ( PARTITION BY emplid ORDER BY effdt DESC) AS rn
    FROM
    (
      SELECT
        emplid,
        oa.effdt,
        oa.action,
        oa.action_descr
      FROM sysadm.ps_employees o1
      LEFT JOIN 
      (
        SELECT
          *
        FROM 
        (
          SELECT
            tt.*,
            ob.action_descr
          FROM
          (
            SELECT
              effdt,
              emplid AS emplid_,
              action
            FROM sysadm.ps_job
            -- Mutliple rows selected primary name.
            WHERE (action = 'PLA' OR action = 'PLV' OR action = 'RFL')
          ) tt
          LEFT JOIN 
          (
              SELECT
                *
              FROM 
              (
                SELECT
                  effdt,
                  eff_status,
                  action,
                  action_descr,
                  ROW_NUMBER() OVER ( PARTITION BY action ORDER BY effdt DESC) AS rn
                FROM sysadm.ps_action_tbl
                WHERE (eff_status = 'A' AND (action = 'PLA' OR action = 'PLV' OR action = 'RFL'))
              )
              -- Mutliple rows selected primary name.
              WHERE rn=1
          ) ob ON (tt.action = ob.action)    
        )
      ) oa ON (o1.emplid = oa.emplid_)    
    ) z
    WHERE effdt IS NOT NULL
    ORDER BY effdt DESC
  )
  WHERE rn=1 
),
-- Output1 isolates most recent review_fromt_dt to pull latest review period by employee.
output2 AS (
  SELECT 
    o2.*, 
    row_number () OVER (
           PARTITION BY emplid
           ORDER BY review_thru_dt DESC
         ) rn
  from   sysadm.ps_employee_review o2
  WHERE  (review_thru_dt IS NOT NULL) AND (review_thru_dt <= SYSDATE)
),
-- Output2 isolates most recent rvw_conduct_dt_rv to pull latest review conducted by employee.
output3 AS (
  SELECT o3.*, row_number () OVER (
           PARTITION BY emplid
           ORDER BY rvw_conduct_dt_rv DESC
         ) rn
  from   sysadm.ps_employee_review o3
  WHERE rvw_conduct_dt_rv IS NOT NULL
)

SELECT
    emplid,
    reg_temp,
    full_part_time,
    name AS employee_name,
    jobtitle AS job_title,
    job_entry_dt, 
    dept_entry_dt, 
    position_entry_dt, 
    -- Extract most recent hire or rehire date and if rehire is blank, pull hire date.
    NVL(GREATEST(rehire_dt, hire_dt), hire_dt) AS start_date,
    deptid,
    reports_to,
    t2.review_from_dt,
    t2.review_thru_dt, 
    t2.review_type,
    t3.supervisor_name,
    t4.last_review_dt,
    t5.event,
    t5.event_reason,
    t5.event_dt
FROM sysadm.ps_employees t1
LEFT JOIN 
(
    SELECT
        emplid AS empl_id,
        review_from_dt,
        review_thru_dt, 
        review_type
    FROM output2
    WHERE rn = 1
) t2 ON t1.emplid = t2.empl_id

LEFT JOIN
(
    SELECT 
        position_nbr,
        supervisor_name 
    FROM sysadm.ps_rv_suprvsrs_vw
) t3 ON t1.reports_to = t3.position_nbr

LEFT JOIN 
(
    SELECT
        emplid AS empl_id,
        rvw_conduct_dt_rv AS last_review_dt
    FROM output3
    WHERE rn = 1
) t4 ON t1.emplid = t4.empl_id
LEFT JOIN 
(
    SELECT
        emplid AS empl_id,
        action AS event,
        action_descr AS event_reason,
        effdt AS event_dt
    FROM output1
) t5 ON t1.emplid = t5.empl_id
--WHERE (eff_status = 'A' AND (action = 'LOA' OR action = 'PLA' OR action = 'PLV' OR action = 'RFL'))
