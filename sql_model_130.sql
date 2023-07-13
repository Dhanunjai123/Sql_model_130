/*
*  Model:   kennametal_130.sql
*
*  Purpose: Take data from typed tables in kennametal_STAGING to fully transformed facts and dimensions
*
*  Notes:   Tables should be dropped and created each time (to process incoming data only)
*/

--------------------------------------------------------------------------------------------

--Specify client nickname for matching to customer dimension and inclusion in generated key columns

SET global_nickname = 'Kennametal'
;

--------------------------------------------------------------------------------------------

-- Purpose: Creating application temp table to avoid duplicate applications.

CREATE OR REPLACE TEMPORARY TABLE KENNAMETAL_TRANSFORMATION.application_deduped_temp AS (

    --To remove duplicate applications

    WITH deduped_appplications AS (
        SELECT
            apt.*,
            asat.application_status_name,
            asat.application_step_name,
            asat.ats_application_status,
            asat.application_disposition_type,
            IFF(asat.application_disposition_type IS NOT NULL, NULL, application_status_rank) AS application_status_ranker
        FROM
            KENNAMETAL_STAGING.applicants_typed apt
        LEFT JOIN KENNAMETAL_STAGING.application_status_alignment_typed asat ON apt.application_current_status = asat.ats_application_status
        WHERE apt.application_date IS NOT NULL
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY apt.application_id
            ORDER BY application_status_ranker DESC NULLS LAST
        ) = 1
    )

    SELECT
        da.*
    FROM
        deduped_appplications da
    LEFT JOIN KENNAMETAL_STAGING.application_status_alignment_typed asat ON da.application_current_status = asat.ats_application_status
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY da.candidate_id, da.job_req_id
        ORDER BY da.application_status_ranker DESC NULLS LAST
    ) = 1
)

--------------------------------------------------------------------------------------------

-- Purpose: Creating jobs temp table to avoid duplicate jobs

CREATE OR REPLACE TEMPORARY TABLE KENNAMETAL_TRANSFORMATION.job_deduped_temp AS (

    SELECT
        *,
        job_req_status_manual_update AS job_req_status,---------------  pending
        requisition_put_on_hold_date AS date_job_hold,
        requisition_taken_off_hold_date AS date_job_removed_hold,
        approved_date AS date_job_opened,
        CONCAT_WS(' ', recruiter_first_name, recruiter_last_name) AS recruiter_name
    FROM KENNAMETAL_STAGING.jobs_typed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY job_req_id
        ORDER BY approved_date DESC NULLS LAST
    ) = 1
)
;

--------------------------------------------------------------------------------------------

-- Purpose : Creating dim_applicant table

DROP TABLE IF EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_applicant
;

CREATE TABLE IF NOT EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_applicant (
    applicant_id VARCHAR(250),
    applicant_key BINARY,
    crm_applicant_id VARCHAR(250),
    customer_key BINARY,
    applicant_name VARCHAR(250),
    applicant_email_address VARCHAR(250),
    applicant_phone VARCHAR(250),
    applicant_city VARCHAR(250),
    applicant_state VARCHAR(250),
    applicant_country VARCHAR(250),
    applicant_ethnicity VARCHAR(250),
    applicant_race VARCHAR(250),
    applicant_gender VARCHAR(250),
    applicant_veteran_status VARCHAR(250),
    applicant_disability_status VARCHAR(250)
)
;

INSERT INTO KENNAMETAL_TRANSFORMATION.tf_dim_applicant
SELECT
    apt.candidate_id AS applicant_id,
    MD5(CONCAT_WS('|', apt.candidate_id, $global_nickname)) AS applicant_key,
    NULL AS crm_applicant_id,
    MAX(dc.customer_key) AS customer_key,
    MAX(CONCAT_WS(' ', apt.first_name, apt.last_name)) AS applicant_name,
    MAX(apt.email_address) AS applicant_email_address,
    MAX(apt.mobile_phone) AS applicant_phone,
    MAX(apt.city) AS applicant_city,
    MAX(apt.state) AS applicant_state,
    MAX(apt.candidate_country_region) AS applicant_country,
    MAX(apt.ethnicity) AS applicant_ethnicity,
    MAX(apt.race) AS applicant_race,
    MAX(apt.gender) AS applicant_gender,
    MAX(apt.are_you_a_veteran) AS applicant_veteran_status,
    MAX(
        CASE
            WHEN disability_status = 'Yes' THEN 'Disabled'
            WHEN disability_status = 'No' THEN 'Not Disabled'
        END
    )  AS applicant_disability_status
FROM
    KENNAMETAL_STAGING.applicants_typed apt
LEFT JOIN UNIFIED_MART.dim_customer dc ON $global_nickname = dc.global_nickname
GROUP BY apt.candidate_id
;

--------------------------------------------------------------------------------------------

-- Purpose: Creating dim_application_status table

DROP TABLE IF EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_application_status
;

CREATE TABLE IF NOT EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_application_status (
    application_status_key BINARY,
    customer_key BINARY,
    application_step VARCHAR(250),
    application_status VARCHAR(250),
    application_status_group_name VARCHAR(250),
    application_status_sort_order NUMBER
)
;

INSERT INTO KENNAMETAL_TRANSFORMATION.tf_dim_application_status

SELECT
    MD5(
        CONCAT_WS('|', application_step_name, ats_application_status, $global_nickname)::VARCHAR
    ) AS application_status_key,
    dc.customer_key AS customer_key,
    application_step_name AS application_step,
    ats_application_status AS application_status,
    NULL AS application_status_group_name,
    application_status_rank AS application_status_sort_order
FROM
    KENNAMETAL_STAGING.application_status_alignment_typed
LEFT JOIN UNIFIED_MART.dim_customer dc ON $global_nickname = dc.global_nickname
;

--------------------------------------------------------------------------------------------

-- Purpose: Creating dim_job_event table

DROP TABLE IF EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_job_event
;

CREATE TABLE KENNAMETAL_TRANSFORMATION.tf_dim_job_event (
    event_key BINARY,
    event_name VARCHAR(250)
)
;

INSERT INTO KENNAMETAL_TRANSFORMATION.tf_dim_job_event
SELECT
    MD5(CONCAT_WS('|', $1, $global_nickname)),
    $1
FROM
    (VALUES
        ('Job Created'),
        ('Job Opened'),
        ('Job Closed'),
        ('Job Cancelled'),
        ('Job On Hold')
    )
;

--------------------------------------------------------------------------------------------

-- Purpose: Creating dim_job_requisition

DROP TABLE IF EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_job_requisition
;

CREATE TABLE IF NOT EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_job_requisition (
    job_req_key BINARY,
    job_req_id VARCHAR(250),
    crm_job_req_id VARCHAR(250),
    customer_key BINARY,
    job_req_status VARCHAR(250),
    job_req_status_raw VARCHAR(250),
    total_number_of_openings_requested NUMBER,
    total_number_of_openings_available NUMBER,
    job_profile_id VARCHAR(250),
    job_profile_name VARCHAR(250),
    job_posting_title VARCHAR(250),
    job_city_name VARCHAR(250),
    job_state_province_name VARCHAR(250),
    job_country_name VARCHAR(250),
    job_location_site_name VARCHAR(250),
    job_position_type VARCHAR(250),
    job_employment_type VARCHAR(250),
    job_experience_level VARCHAR(250),
    job_compensation_type VARCHAR(250),
    job_overtime_compensation_type VARCHAR(250),
    job_pay_grade_name VARCHAR(250),
    cielo_finance_invoice_cancel_tier VARCHAR(250),
    cielo_finance_invoice_forecast_hire_tier VARCHAR(250),
    cielo_finance_invoice_forecast_open_tier VARCHAR(250),
    cielo_finance_invoice_hire_tier VARCHAR(250),
    cielo_finance_invoice_open_tier VARCHAR(250),
    cielo_segmentation_job_category_major VARCHAR(250),
    cielo_segmentation_job_category_minor VARCHAR(250),
    cielo_segmentation_job_category_specific VARCHAR(250),
    cielo_segmentation_job_level VARCHAR(250),
    date_job_approved DATE,
    date_job_cancelled DATE,
    job_cancellation_reason VARCHAR(250),
    date_job_closed DATE,
    date_job_created DATE,
    date_job_hold DATE,
    job_hold_reason VARCHAR(250),
    date_job_opened DATE,
    date_job_posting_published DATE,
    date_job_posting_withdrawn DATE,
    date_job_removed_hold DATE,
    elapsed_days_job_in_hold_status NUMBER,
    elapsed_days_job_open NUMBER,
    elapsed_days_job_open_group VARCHAR(250),
    elapsed_days_job_open_less_hold NUMBER,
    elapsed_days_job_open_less_hold_group VARCHAR(250),
    business_unit_id VARCHAR(250),
    business_unit_name VARCHAR(250),
    cost_center_id VARCHAR(250),
    cost_center_name VARCHAR(250),
    department_id VARCHAR(250),
    department_name VARCHAR(250),
    function_id VARCHAR(250),
    function_name VARCHAR(250),
    industry_name VARCHAR(250),
    hiring_manager_id VARCHAR(250),
    hiring_manager_name VARCHAR(250),
    hiring_manager_email VARCHAR(250),
    job_family_id VARCHAR(250),
    job_family_name VARCHAR(250),
    job_family_group_name VARCHAR(250),
    job_grade_name VARCHAR(250),
    organizational_division_id VARCHAR(250),
    organizational_division_name VARCHAR(250),
    region_id VARCHAR(250),
    region_name VARCHAR(250),
    recruiters VARCHAR(250),
    org_leadership_hierarchy_1_level_up VARCHAR(250),
    org_leadership_hierarchy_2_levels_up VARCHAR(250),
    org_leadership_hierarchy_3_levels_up VARCHAR(250),
    org_leadership_hierarchy_4_levels_up VARCHAR(250),
    org_leadership_hierarchy_5_levels_up VARCHAR(250),
    org_leadership_hierarchy_6_levels_up VARCHAR(250),
    org_leadership_hierarchy_7_levels_up VARCHAR(250),
    org_leadership_hierarchy_8_levels_up VARCHAR(250),
    org_leadership_hierarchy_9_levels_up VARCHAR(250)
)
;

INSERT INTO KENNAMETAL_TRANSFORMATION.tf_dim_job_requisition
SELECT
    MD5(CONCAT_WS('|', jdt.job_req_id, $global_nickname)) AS job_req_key,
    jdt.job_req_id AS job_req_id,
    NULL AS crm_job_req_id,
    dc.customer_key AS customer_key,
    jdt.job_req_status AS job_req_status,---------------  pending
    jdt.job_req_status_manual_update AS job_req_status_raw,
    jdt.number_of_openings AS total_number_of_openings_requested,
    NULL AS total_number_of_openings_available,
    NULL AS job_profile_id,
    jdt.eeo_category AS job_profile_name,
    jdt.job_title AS job_posting_title,
    jdt.city AS job_city_name,
    CASE 
        WHEN jdt.state_province_other LIKE 'US%' OR jdt.state_province_other LIKE 'CA%' 
    THEN st.job_state_name  
    END AS job_state_province_name,
    c.job_country AS job_country_name,
    jdt.location AS job_location_site_name,
    CASE
        WHEN jdt.requisition_type IN ('Evergreen','Child of Evergreen') THEN 'Sourcing/Pipeline'
        ELSE jdt.requisition_type
    END AS job_position_type,
    jdt.job_posting_type AS job_employment_type,
    jdt.job_shift AS job_experience_level,
    NULL AS job_compensation_type,
    CASE 
        WHEN jdt.flsa_status_us_only = 'No FLSA Required' THEN 'Non-Exempt'
        ELSE jdt.flsa_status_us_only
    END AS job_overtime_compensation_type,
    jdt.pay_grade AS job_pay_grade_name,
    NULL AS cielo_finance_invoice_cancel_tier,
    NULL AS cielo_finance_invoice_forecast_hire_tier,
    NULL AS cielo_finance_invoice_forecast_open_tier,
    NULL AS cielo_finance_invoice_hire_tier,
    jdt.tier AS cielo_finance_invoice_open_tier,
    jsg.net_suite_job_category_major AS cielo_segmentation_job_category_major,
    jsg.net_suite_job_category_minor AS cielo_segmentation_job_category_minor,
    jsg.net_suite_job_category_specific AS cielo_segmentation_job_category_specific,
    jsg.net_suite_job_level AS cielo_segmentation_job_level,
    jdt.intake_completed_date AS date_job_approved,
    NULL AS date_job_cancelled,
    NULL AS job_cancellation_reason,
    jdt.closed_date AS date_job_closed,
    jdt.date_created AS date_job_created,
    jdt.requisition_put_on_hold_date AS date_job_hold,
    NULL AS job_hold_reason,
    jdt.approved_date AS date_job_opened,
    NULL AS date_job_posting_published,
    NULL AS date_job_posting_withdrawn,
    jdt.requisition_taken_off_hold_date AS date_job_removed_hold,
    DATEDIFF(DAYS, jdt.date_job_hold, jdt.date_job_removed_hold) AS elapsed_days_job_in_hold_status,
    CASE
        WHEN jdt.job_req_status = 'Open' THEN DATEDIFF(DAYS, jdt.date_job_opened, CURRENT_DATE())
    END AS elapsed_days_job_open,
    CASE
        WHEN elapsed_days_job_open <= 30 THEN '0 - 30 Days'
        WHEN elapsed_days_job_open <= 60 THEN '31 - 60 Days'
        WHEN elapsed_days_job_open <= 90 THEN '61 - 90 Days'
        WHEN elapsed_days_job_open <= 120 THEN '91 - 120 Days'
        WHEN elapsed_days_job_open > 120 THEN '121+ Days'
    END AS elapsed_days_job_open_group,
    elapsed_days_job_open - elapsed_days_job_in_hold_status AS elapsed_days_job_open_less_hold,
    CASE
        WHEN elapsed_days_job_open_less_hold <= 30 THEN '0 - 30 Days'
        WHEN elapsed_days_job_open_less_hold <= 60 THEN '31 - 60 Days'
        WHEN elapsed_days_job_open_less_hold <= 90 THEN '61 - 90 Days'
        WHEN elapsed_days_job_open_less_hold <= 120 THEN '91 - 120 Days'
        WHEN elapsed_days_job_open_less_hold > 120 THEN '121+ Days'
    END AS elapsed_days_job_open_less_hold_group,
    NULL AS business_unit_id,
    jdt.business_unit AS business_unit_name,
    NULL AS cost_center_id,
    jdt.cost_center AS cost_center_name,
    NULL AS department_id,
    jdt.department AS department_name,
    NULL AS function_id,
    jdt.job_posting_function AS function_name,
    jdt.company AS industry_name,
    jdt.hiring_manager_user_sys_id AS hiring_manager_id,
    CONCAT_WS(' ', jdt.hiring_manager_first_name, jdt.hiring_manager_last_name)AS hiring_manager_name,
    NULL AS hiring_manager_email,
    NULL AS job_family_id,
    jdt.owner AS job_family_name,
    COALESCE(jdt.job_function_type,jdt.job_function_type_1) AS job_family_group_name,
    jdt.confidential AS job_grade_name,
    NULL AS organizational_division_id,
    jdt.division AS organizational_division_name,
    NULL AS region_id,
    jdt.region AS region_name,
    NULL AS recruiters,
    NULL AS org_leadership_hierarchy_1_level_up,
    CONCAT_WS(' ', jdt.elt_member_professional_role_vp_operations_production_role_first_name, jdt.elt_member_professional_role_vp_operations_production_role_last_name) AS org_leadership_hierarchy_2_levels_up,
    CONCAT_WS(' ', jdt.l_3_manager_professional_role_director_operations_production_role_first_name, jdt.l_3_manager_professional_role_director_operations_production_role_last_name) AS org_leadership_hierarchy_3_levels_up,
    NULL AS org_leadership_hierarchy_4_levels_up,
    CONCAT_WS(' ', jdt.hr_business_partner_first_name, jdt.hr_business_partner_last_name) AS org_leadership_hierarchy_5_levels_up,
    NULL AS org_leadership_hierarchy_6_levels_up,
    NULL AS org_leadership_hierarchy_7_levels_up,
    NULL AS org_leadership_hierarchy_8_levels_up,
    NULL AS org_leadership_hierarchy_9_levels_up
FROM
    KENNAMETAL_TRANSFORMATION.job_deduped_temp jdt
LEFT JOIN KENNAMETAL_STAGING.job_segmentation_typed jsg ON jdt.job_title = jsg.job_title
LEFT JOIN KENNAMETAL_STAGING.state_typed st ON jdt.state_province_other = st.state_province_other
LEFT JOIN KENNAMETAL_STAGING.country_typed c ON jdt.job_location_country = c.job_location_country
LEFT JOIN UNIFIED_MART.dim_customer dc ON $global_nickname = dc.global_nickname
;

--------------------------------------------------------------------------------------------

-- Purpose: Creating dim_recruiter table

DROP TABLE IF EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_recruiter
;

CREATE TABLE IF NOT EXISTS KENNAMETAL_TRANSFORMATION.tf_dim_recruiter (
    recruiter_key BINARY,
    recruiter_id VARCHAR(250),
    customer_key BINARY,
    recruiter_name VARCHAR(250),
    recruiter_email VARCHAR(250),
    recruitment_team VARCHAR(250)
)
;

INSERT INTO KENNAMETAL_TRANSFORMATION.tf_dim_recruiter
SELECT
    MD5(CONCAT_WS('|', jdt.recruiter_name, $global_nickname)) AS recruiter_key,
    MD5(jdt.recruiter_name) AS recruiter_id,
    MAX(dc.customer_key) AS customer_key,
    jdt.recruiter_name,
    NULL AS recruiter_email,
    MAX(r.recruitment_team) AS recruitment_team
FROM
    KENNAMETAL_TRANSFORMATION.job_deduped_temp jdt
LEFT JOIN KENNAMETAL_STAGING.recruiters_typed r
LEFT JOIN UNIFIED_MART.dim_customer dc ON $global_nickname = dc.global_nickname
WHERE jdt.recruiter_name IS NOT NULL
GROUP BY jdt.recruiter_name
;

--------------------------------------------------------------------------------------------

-- Purpose : Creating fact_applications table

DROP TABLE IF EXISTS KENNAMETAL_TRANSFORMATION.tf_fact_applications
;

CREATE TABLE IF NOT EXISTS KENNAMETAL_TRANSFORMATION.tf_fact_applications(
    application_key BINARY,
    application_id VARCHAR(250),
    crm_application_id VARCHAR(250),
    customer_key BINARY,
    applicant_key BINARY,
    applicant_id VARCHAR(250),
    job_req_key BINARY,
    job_req_id VARCHAR(250),
    application_source_name VARCHAR(250),
    application_source_type VARCHAR(250),
    application_source_category VARCHAR(250),
    application_source_referral_name VARCHAR(250),
    application_source_referral_email VARCHAR(250),
    application_current_step VARCHAR(250),
    application_current_status VARCHAR(250),
    application_disposition_reason VARCHAR(250),
    is_application_dispositioned BOOLEAN,
    is_application_rejected BOOLEAN,
    is_application_rescinded BOOLEAN,
    is_application_withdrawn BOOLEAN,
    is_cielo_peak_application_reviewed BOOLEAN,
    cielo_peak_applications_pending_review_group VARCHAR(250),
    cielo_segmentation_application_source_type VARCHAR(250),
    cielo_segmentation_application_source_category VARCHAR(250),
    cielo_segmentation_application_source_specific_name VARCHAR(250),
    date_application_created TIMESTAMP,
    date_application_current_status TIMESTAMP,
    date_application_dispositioned TIMESTAMP,
    date_application_lead TIMESTAMP,
    date_application_new TIMESTAMP,
    date_application_rejected TIMESTAMP,
    date_application_review TIMESTAMP,
    date_application_screen TIMESTAMP,
    date_application_screen_pending TIMESTAMP,
    date_application_screen_successful TIMESTAMP,
    date_application_withdrawn TIMESTAMP,
    date_hire TIMESTAMP,
    date_hm_interview TIMESTAMP,
    date_hm_interview_pending TIMESTAMP,
    date_hm_interview_successful TIMESTAMP,
    date_pre_employment_complete TIMESTAMP,
    date_pre_employment_start TIMESTAMP,
    date_ready_for_start TIMESTAMP,
    date_start TIMESTAMP,
    date_submit_offer_request TIMESTAMP,
    date_submit_to_hm TIMESTAMP,
    date_verbal_offer_accept TIMESTAMP,
    date_verbal_offer_extend TIMESTAMP,
    date_written_offer_accept TIMESTAMP,
    date_written_offer_extend TIMESTAMP,
    elapsed_days_to_hire NUMBER,
    elapsed_days_to_hm_interview NUMBER,
    elapsed_days_to_offer_accept NUMBER,
    elapsed_days_to_start NUMBER
)
;

INSERT INTO KENNAMETAL_TRANSFORMATION.tf_fact_applications (
    SELECT
        MD5(CONCAT_WS('|', apt.application_id, $global_nickname)) AS application_key,
        apt.application_id AS application_id,
        NULL AS crm_application_id,
        MAX(dc.customer_key) AS customer_key,
        MD5(CONCAT_WS('|', MAX(apt.candidate_id), $global_nickname)) AS applicant_key,
        MAX(apt.candidate_id) AS applicant_id,
        MD5(CONCAT_WS('|', MAX(apt.job_req_id), $global_nickname)) AS job_req_key,
        MAX(apt.job_req_id) AS job_req_id,
        MAX(apt.source_details) AS application_source_name,
        MAX(
            CASE
                WHEN apt.is_internal = 'Yes' THEN 'Internal'
                WHEN apt.is_internal = 'No' THEN 'External'
            END
        ) AS application_source_type,
        NULL AS application_source_category,
        MAX(apt.if_employee_referral_provide_name) AS application_source_referral_name,
        NULL AS application_source_referral_email,
        MAX(adt.application_step_name) AS application_current_step,
        MAX(apt.application_current_status) AS application_current_status,
        MAX(
            CASE
                WHEN adt.application_step_name IN ('Rejected', 'Withdrawn', 'Rescinded') THEN apt.application_status
            END
        ) AS application_disposition_reason,
        MAX(
            CASE
                WHEN adt.application_step_name IN ('Rejected', 'Withdrawn', 'Rescinded') THEN 'Yes'
                ELSE 'No'
            END
        ) AS is_application_dispositioned,
        MAX(
            CASE
                WHEN adt.application_step_name = 'Rejected' THEN 'Yes'
                ELSE 'No'
            END
        ) AS is_application_rejected,
        MAX(
            CASE
                WHEN adt.application_step_name = 'Rescinded' THEN 'Yes'
                ELSE 'No'
            END
        ) AS is_application_rescinded,
        MAX(
            CASE
                WHEN adt.application_step_name = 'Withdrawn' THEN 'Yes'
                ELSE 'No'
            END
        ) AS is_application_withdrawn,
        NULL AS is_cielo_peak_application_reviewed,
        NULL AS cielo_peak_applications_pending_review_group,
        NULL AS cielo_segmentation_application_source_type,
        MAX(ss.net_suite_broad_source_category) AS cielo_segmentation_application_source_category,
        MAX(ss.net_suite_specific_source) AS cielo_segmentation_application_source_specific_name,
        MAX(apt.application_date) AS date_application_created,
        MAX(
            CASE
                WHEN apt.application_current_status = apt.application_status THEN apt.created_date
            END
        ) AS date_application_current_status,
        MAX(
            CASE
                WHEN adt.application_step_name IN ('Rejected', 'Withdrawn', 'Rescinded') THEN apt.created_date
            END
        ) AS date_application_dispositioned,
        NULL AS date_application_lead,
        NULL AS date_application_new,
        MAX(
            CASE
                WHEN adt.application_step_name = 'Rejected' THEN apt.created_date
            END
        ) AS date_application_rejected,
        MAX(
            CASE
                WHEN apt.application_status = 'Under Recruiter Review' THEN apt.created_date
            END
        ) AS date_application_review,
        MAX(
            CASE
                WHEN apt.application_status = 'Recruiter Phone Screen Completed' THEN apt.created_date
            END
        ) AS date_application_screen,
        MAX(
            CASE
                WHEN apt.application_status = 'Recruiter Phone Screen Completed' THEN apt.created_date
            END
        ) AS date_application_screen_pending,
        NULL AS date_application_screen_successful,
        MAX(
            CASE
                WHEN adt.application_step_name = 'Withdrawn' THEN apt.created_date
            END
        ) AS date_application_withdrawn,
        MAX(apt.hired_on) AS date_hire,
        MAX(
            CASE
                WHEN apt.application_status = 'Interview Completed' THEN apt.created_date
            END
        ) AS date_hm_interview,
        MAX(
            CASE
                WHEN apt.application_status = 'Interview Scheduled' THEN apt.created_date
            END
        ) AS date_hm_interview_pending,
        NULL AS date_hm_interview_successful,
        MAX(
            CASE
                WHEN apt.application_status = 'Post-Offer Contingencies Completed' THEN apt.created_date
            END
        ) AS date_pre_employment_complete,
        MAX(
            CASE
                WHEN apt.application_status = 'Post-Offer Contingencies Initiated' THEN apt.created_date
            END
        ) AS date_pre_employment_start,
        MAX(
            CASE
                WHEN apt.application_status = 'Move to Onboarding' THEN apt.created_date
            END
        ) AS date_ready_for_start,
        MAX(apt.app_job_start_date) AS date_start,
        MAX(
            CASE
                WHEN apt.application_status = 'Offer Approved' THEN apt.created_date
            END
        ) AS date_submit_offer_request,
        MAX(
            CASE
                WHEN apt.application_status = 'Under HM Review' THEN apt.created_date
            END
        ) AS date_submit_to_hm,
        MAX(
            CASE
                WHEN apt.application_status = 'Offer Accepted' THEN apt.created_date
            END
        ) AS date_verbal_offer_accept,
        MAX(
            CASE
                WHEN apt.application_status = 'Verbal Offer Extended' THEN apt.created_date
            END
        ) AS date_verbal_offer_extend,
        MAX(
            CASE
                WHEN apt.application_status = 'Written Offer Accepted' THEN apt.created_date
            END
        ) AS date_written_offer_accept,
        MAX(
            CASE
                WHEN apt.application_status = 'Written Offer Extended' THEN apt.created_date
            END
        ) AS date_written_offer_extend,
        DATEDIFF(DAY, MAX(jdt.date_job_opened), date_hire) AS elapsed_days_to_hire,
        DATEDIFF(DAY, MAX(jdt.date_job_opened), date_hm_interview) AS elapsed_days_to_hm_interview,
        DATEDIFF(DAY, MAX(jdt.date_job_opened), date_verbal_offer_accept) AS elapsed_days_to_offer_accept,
        NULL AS elapsed_days_to_start
    FROM
        KENNAMETAL_STAGING.applicants_typed apt
    INNER JOIN KENNAMETAL_TRANSFORMATION.application_deduped_temp adt ON apt.application_id = adt.application_id AND apt.application_date = adt.application_date
    LEFT JOIN KENNAMETAL_TRANSFORMATION.job_deduped_temp jdt ON apt.job_req_id = jdt.job_req_id
    LEFT JOIN KENNAMETAL_STAGING.source_segmentation_typed ss ON apt.source_details = ss.ats_source_details
    LEFT JOIN UNIFIED_MART.dim_customer dc ON $global_nickname = dc.global_nickname
    GROUP BY apt.application_id,
             apt.application_date
)
;

------------------------------------------------------------------------------------------------------
