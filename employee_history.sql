-- Employee histroy
WITH    got_output_group    
AS
(
    SELECT
    *
    FROM 
    (
        SELECT
            j.*,
            event,
            event_reason,
            ROW_NUMBER() OVER ( PARTITION BY emplid ORDER BY effdt DESC) AS rn_
        FROM
        (
            SELECT
                h.*,
                action_,
                action_reason_,
                location_
            FROM
            (
                SELECT
                    g.*,
                    CASE 
                        WHEN end_date IS NULL THEN (SYSDATE - start_date)
                        ELSE (end_date - start_date)
                        END job_calc,
                    CASE 
                        WHEN end_date IS NULL THEN sysdate
                        ELSE end_date
                        END birth_calc
                FROM
                (
                    SELECT
                        f.*,
                        CASE 
                            WHEN (NVL(last_hire_dt, hire_dt) <= NVL(new_term_dt, start_date)) AND (((new_term_dt >= start_date) AND ((new_term_dt <= prv_dt)) OR ((prv_dt IS NULL) AND (new_term_dt IS NOT NULL)))) THEN new_term_dt
                            ELSE prv_dt
                            END end_date
                    FROM
                    (
                        SELECT
                            e.*,
                            CASE 
                                WHEN prv_calc = start_date THEN prv_calc 
                                ELSE prv_calc - 1 
                                END prv_dt
                        FROM
                        (
                            SELECT
                                d.*,
                                LAG(start_date, 1) OVER(PARTITION BY emplid ORDER BY start_date DESC, effdt DESC) AS prv_calc
                            FROM
                            (
                                SELECT
                                    c.*,
                                    birthdate,
                                    ethnicity,
                                    sex,
                                    mar_status,
                                    highest_educ_lvl,
                                    -- work_location,
                                    -- work_city,
                                    -- work_county,
                                    -- work_state,
                                    -- work_postal,
                                    -- home_city,
                                    -- home_county,
                                    -- home_state,
                                    -- home_postal,
                                    descr AS jobtitle,
                                    GREATEST(job_entry_dt, NVL(dept_entry_dt, job_entry_dt), NVL(position_entry_dt, job_entry_dt)) as start_date
                                FROM
                                (
                                    SELECT
                                        a.*,
                                        COALESCE(( 
                                            SELECT
                                                MAX(termination_dt) AS termination_dt
                                            FROM sysadm.ps_job aa
                                            WHERE aa.emplid = a.emplid
                                            AND aa.position_nbr = a.position_nbr
                                        ), NULL) AS new_term_dt
                                    FROM 
                                    (
                                        SELECT
                                            ax.*
                                        FROM sysadm.ps_job ax
                                        ORDER BY rank() OVER (PARTITION BY emplid, effdt ORDER BY effseq DESC, effdt DESC)
                                        FETCH FIRST 1 row with ties
                                    ) a
                                    ORDER BY rank() OVER (PARTITION BY emplid, jobcode, job_entry_dt, position_nbr, position_entry_dt, deptid, dept_entry_dt ORDER BY effseq DESC, effdt DESC)
                                    FETCH FIRST 1 row with ties
                                ) c
                                LEFT JOIN 
                                (
                                    SELECT
                                        emplid,
                                        ethnic_grp_cd AS ethnicity
                                    FROM sysadm.ps_divers_ethnic
                            
                                ) ca ON (c.emplid = ca.emplid)
                                LEFT JOIN 
                                (
                                    SELECT
                                        effdt,
                                        emplid,
                                        mar_status,
                                        highest_educ_lvl
                                    FROM sysadm.ps_pers_data_effdt
                            
                                ) cb ON (c.emplid = cb.emplid) AND (c.effdt >= cb.effdt)
                                LEFT JOIN
                                (
                                    SELECT
                                        jobcode,
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
                                ) cc ON (c.jobcode = cc.jobcode)

                                LEFT JOIN 
                                (
                                    SELECT
                                        location AS work_location,
                                        address1 AS work_address1,
                                        address2 AS work_address2,
                                        city AS work_city,
                                        county AS work_county,
                                        state AS work_state,
                                        postal AS work_postal
                                    FROM sysadm.ps_location_tbl
                                ) cd ON (c.location = cd.work_location)
                                
                                LEFT JOIN 
                                (
                                    SELECT
                                        emplid,
                                        asofdate,
                                        address_type,
                                        city AS home_city,
                                        county AS home_county,
                                        state AS home_state,
                                        postal AS home_postal
                                    FROM sysadm.ps_person_address
                                    WHERE address_type = 'HOME'
                                ) ce ON (c.emplid = ce.emplid) AND (c.effdt >= ce.asofdate)
                                LEFT JOIN 
                                (
                                    SELECT
                                        emplid,
                                        birthdate,
                                        sex
                                    FROM sysadm.ps_personnel
                            
                                ) cb ON (c.emplid = cb.emplid)
                                
                            ) d
                        ) e
                    ) f
                ) g
            ) h
            LEFT JOIN 
            (
                SELECT
                    emplid,
                    jobcode,
                    position_nbr,
                    deptid,
                    termination_dt,
                    action AS action_,
                    action_reason AS action_reason_,
                    location AS location_
                FROM
                (
                    SELECT
                        sep_a.*
                    FROM 
                    (
                        SELECT
                            sep_ax.*
                        FROM 
                        (
                            SELECT
                                sep_ay.*
                            FROM sysadm.ps_job sep_ay
                            WHERE (ACTION = 'TER') OR (ACTION = 'RET')
                        ) sep_ax
                        ORDER BY rank() OVER (PARTITION BY emplid, effdt ORDER BY effseq DESC, effdt DESC)
                        FETCH FIRST 1 row with ties
                    ) sep_a
                    ORDER BY rank() OVER (PARTITION BY emplid, jobcode, job_entry_dt, position_nbr, position_entry_dt, deptid, dept_entry_dt ORDER BY effseq DESC, effdt DESC)
                    FETCH FIRST 1 row with ties
                )
                
            ) i ON (h.emplid = i.emplid) AND (h.jobcode = i.jobcode) AND (h.position_nbr = i.position_nbr) AND (h.deptid = i.deptid) AND (h.end_date = i.termination_dt)
        ) j
        LEFT JOIN 
        (
            SELECT
                action,
                action_descr AS event
            FROM sysadm.ps_action_tbl

        ) ja ON (j.action_ = ja.action)
        LEFT JOIN 
        (
            SELECT
                action,
                action_reason,
                descr AS event_reason
            FROM sysadm.ps_actn_reason_tbl

        ) jb ON (j.action_ = jb.action) AND (j.action_reason_ = jb.action_reason)
    )
 
)

SELECT 
    effdt,
    effseq,
    emplid,
    t4.first_name,
    t4.middle_name,
    t4.last_name,
    deptid,
    jobcode,
    position_nbr,
    job_entry_dt,
    position_entry_dt,
    dept_entry_dt,
    hire_dt,
    last_hire_dt,
    termination_dt,
    new_term_dt,
    start_date,
    prv_dt,
    end_date,
    CASE 
        WHEN job_calc < 0 THEN 0 
        ELSE job_calc 
        END duration,
    jobtitle, 
    -- comprate,
    -- work_location,
    -- work_city,
    -- work_county,
    -- work_state,
    -- work_postal,
    -- home_city,
    -- home_county,
    -- home_state,
    -- home_postal,
    sex,
    ethnicity,
    mar_status,
    highest_educ_lvl,
    TRUNC(months_between(birth_calc, birthdate) / 12) AS age,
    event,
    event_reason,
    sysdate
FROM
    got_output_group t1

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