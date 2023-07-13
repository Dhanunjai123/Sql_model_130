/*
* Model: Kennametal_120
* Purpose: Rename columns and cast to the correct data types
*
* Notes: This model performs casting and renaming from the untyped tables
* Tables should be dropped and created each time to process incoming data only
*/

----------------------------------------------------------------------------

SET global_nickname = 'Kennametal'
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.applicants_typed
;

CREATE TABLE KENNAMETAL_STAGING.applicants_typed (
    gender VARCHAR(32767) NULL,
    are_you_liable_for_full_time_or_part_time_national_service VARCHAR(32767) NULL,
    mobile_phone VARCHAR(32767) NULL,
    disability_status VARCHAR(32767) NULL,
    created_date DATE NULL,
    salutation VARCHAR(32767) NULL,
    candidate_country_region VARCHAR(32767) NULL,
    application_status VARCHAR(32767) NULL,
    application_status_category VARCHAR(32767) NULL,
    is_current_status VARCHAR(32767) NULL,
    ethnicity VARCHAR(32767) NULL,
    is_internal VARCHAR(32767) NULL,
    candidate_id NUMBER NULL,
    app_job_start_date DATE NULL,
    first_name VARCHAR(32767) NULL,
    are_you_a_veteran VARCHAR(32767) NULL,
    how_did_you_hear_about_this_position VARCHAR(32767) NULL,
    application_date DATE NULL,
    last_name VARCHAR(32767) NULL,
    application_id NUMBER NULL,
    if_employee_referral_provide_name VARCHAR(32767) NULL,
    application_current_status VARCHAR(32767) NULL,
    job_req_id NUMBER NULL,
    email_address VARCHAR(32767) NULL,
    state VARCHAR(32767) NULL,
    source_details VARCHAR(32767) NULL,
    race VARCHAR(32767) NULL,
    hired_on DATE NULL,
    city VARCHAR(32767) NULL,
    highest_degree_completed VARCHAR(32767) NULL,
    source VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.applicants_typed
SELECT
    "Gender:",
    "Are you liable for full-time or part-time National Service",
    "Mobile Phone",
    "Disability Status",
    "Created Date",
    "Salutation:",
    "Candidate Country/Region",
    "Application Status",
    "Application Status Category",
    "Is Current Status",
    "Ethnicity",
    "Is Internal",
    "Candidate ID",
    "App_Job Start Date",
    "First Name",
    "Are you a Veteran",
    "How did you hear about this position",
    "Application Date",
    "Last Name",
    "Application ID",
    "If Employee Referral Provide Name:",
    "Application Current Status",
    "Job Req ID",
    "Email Address",
    "State",
    "Source Details",
    "Race",
    "Hired On",
    "City",
    "Highest Degree Completed:",
    "Source",
    $global_nickname
FROM
    KENNAMETAL_STAGING.applicants_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.application_status_alignment_typed
;

CREATE TABLE KENNAMETAL_STAGING.application_status_alignment_typed (
    include_for_submit_to_hm_to_include_for_hm_interview VARCHAR(32767) NULL,
    include_for_application_complete_to_application_review VARCHAR(32767) NULL,
    application_step_name VARCHAR(32767) NULL,
    include_for_application_review_to_application_screen VARCHAR(32767) NULL,
    display_order NUMBER NULL,
    application_status_name VARCHAR(32767) NULL,
    application_status_rank NUMBER NULL,
    include_for_hm_interview_to_application_verbal_offer_accept VARCHAR(32767) NULL,
    include_for_application_screen_to_application_submit_to_hm VARCHAR(32767) NULL,
    include_for_verbal_offer_accept_to_application_hire VARCHAR(32767) NULL,
    ats_application_status VARCHAR(32767) NULL,
    application_disposition_type VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.application_status_alignment_typed
SELECT
    "INCLUDE_FOR_SUBMIT_TO_HM_TO_INCLUDE_FOR_HM_INTERVIEW",
    "INCLUDE_FOR_APPLICATION_COMPLETE_TO_APPLICATION_REVIEW",
    "Application Step Name",
    "INCLUDE_FOR_APPLICATION_REVIEW_TO_APPLICATION_SCREEN",
    "Display Order",
    "Application Status Name",
    "Application Status Rank",
    "INCLUDE_FOR_HM_INTERVIEW_TO_APPLICATION_VERBAL_OFFER_ACCEPT",
    "INCLUDE_FOR_APPLICATION_SCREEN_TO_APPLICATION_SUBMIT_TO_HM",
    "INCLUDE_FOR_VERBAL_OFFER_ACCEPT_TO_APPLICATION_HIRE",
    "ATS Application Status",
    "Application Disposition Type",
    $global_nickname
FROM
    KENNAMETAL_STAGING.application_status_alignment_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.country_typed
;

CREATE TABLE KENNAMETAL_STAGING.country_typed (
    job_location_country VARCHAR(32767) NULL,
    job_country VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.country_typed
SELECT
    "Job Location Country",
    "Job Country",
    $global_nickname
FROM
    KENNAMETAL_STAGING.country_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.jobs_typed
;

CREATE TABLE KENNAMETAL_STAGING.jobs_typed (
    job_shift VARCHAR(32767) NULL,
    job_posting_type VARCHAR(32767) NULL,
    employee_class VARCHAR(32767) NULL,
    owner VARCHAR(32767) NULL,
    eeo_category VARCHAR(32767) NULL,
    job_posting_function VARCHAR(32767) NULL,
    elt_member_professional_role_vp_operations_production_role_first_name VARCHAR(32767) NULL,
    job_req_id NUMBER NULL,
    l_3_manager_professional_role_director_operations_production_role_last_name VARCHAR(32767) NULL,
    target_start_date DATE NULL,
    recruiter_last_name VARCHAR(32767) NULL,
    currently_with_approver_first_name VARCHAR(32767) NULL,
    flsa_status_us_only VARCHAR(32767) NULL,
    closed_date DATE NULL,
    hr_business_partner_last_name VARCHAR(32767) NULL,
    requisition_taken_off_hold_date DATE NULL,
    job_posting_city VARCHAR(32767) NULL,
    job_req_status_manual_update VARCHAR(32767) NULL,
    state_province_other VARCHAR(32767) NULL,
    hiring_manager_user_sys_id NUMBER NULL,
    headcount_type VARCHAR(32767) NULL,
    hr_business_partner_user_sys_id NUMBER NULL,
    job_title VARCHAR(32767) NULL,
    city VARCHAR(32767) NULL,
    elt_member_professional_role_vp_operations_production_role_last_name VARCHAR(32767) NULL,
    tier VARCHAR(32767) NULL,
    intake_completed_date DATE NULL,
    company VARCHAR(32767) NULL,
    requisition_status VARCHAR(32767) NULL,
    date_created DATE NULL,
    requisition_type VARCHAR(32767) NULL,
    recruiting_team_comments VARCHAR(32767) NULL,
    business_unit VARCHAR(32767) NULL,
    job_function_type_1 VARCHAR(32767) NULL,
    job_function_type VARCHAR(32767) NULL,
    currently_with_approver_last_name VARCHAR(32767) NULL,
    job_location_country VARCHAR(32767) NULL,
    department VARCHAR(32767) NULL,
    location VARCHAR(32767) NULL,
    cost_center VARCHAR(32767) NULL,
    employee_type VARCHAR(32767) NULL,
    pay_grade VARCHAR(32767) NULL,
    hiring_manager_last_name VARCHAR(32767) NULL,
    region VARCHAR(32767) NULL,
    regular_temporary VARCHAR(32767) NULL,
    job_posting_state_province_other VARCHAR(32767) NULL,
    l_3_manager_professional_role_director_operations_production_role_first_name VARCHAR(32767) NULL,
    recruiter_first_name VARCHAR(32767) NULL,
    job_posting_country VARCHAR(32767) NULL,
    replacement_or_redeployment_for_please_add_employee_name VARCHAR(32767) NULL,
    division VARCHAR(32767) NULL,
    approved_date TIMESTAMP NULL,
    requisition_put_on_hold_date DATE NULL,
    hr_business_partner_first_name VARCHAR(32767) NULL,
    confidential VARCHAR(32767) NULL,
    number_of_openings NUMBER NULL,
    hiring_manager_first_name VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.jobs_typed
SELECT
    "Job Shift",
    "Job Posting Type ",
    "Employee Class",
    "Owner",
    "EEO Category",
    "Job Posting Function ",
    "ELT Member (Professional Role) / VP -Operations (Production Role) First name",
    "Job Req ID",
    "L-3 Manager (Professional Role) / Director Operations (Production Role) Last Name",
    "Target Start Date",
    "Recruiter Last Name",
    "Currently With Approver First Name",
    "FLSA Status - US Only",
    "Closed Date",
    "HR Business Partner Last Name",
    "Requisition Taken off Hold Date",
    "Job Posting City",
    "Job Req Status- Manual Update!",
    "State/Province/Other",
    "Hiring Manager User Sys ID",
    "Headcount Type",
    "HR Business Partner User Sys ID",
    "Job Title",
    "City",
    "ELT Member (Professional Role) / VP -Operations (Production Role) Last Name",
    "Tier",
    "Intake Completed Date",
    "Company",
    "Requisition Status",
    "Date Created",
    "Requisition Type",
    "Recruiting Team Comments",
    "Business Unit",
    "Job Function Type_1",
    "Job Function Type",
    "Currently With Approver Last Name",
    "Job Location Country",
    "Department",
    "Location",
    "Cost Center",
    "Employee Type",
    "Pay Grade",
    "Hiring Manager Last Name",
    "Region",
    "Regular/Temporary",
    "Job Posting State/Province/Other",
    "L-3 Manager (Professional Role) / Director Operations (Production Role) First name",
    "Recruiter First Name",
    "Job Posting Country",
    "Replacement or Redeployment for (Please add employee name)",
    "Division",
    COALESCE(TRY_TO_TIMESTAMP("Approved Date"), TO_TIMESTAMP("Approved Date", 'mm/dd/yyyy HH12:MI:SS PM')),
    "Requisition Put on Hold Date",
    "HR Business Partner First name",
    "Confidential",
    "Number of Openings",
    "Hiring Manager First Name",
    $global_nickname
FROM
    KENNAMETAL_STAGING.jobs_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.recruiters_typed
;

CREATE TABLE KENNAMETAL_STAGING.recruiters_typed (
    recruitment_team VARCHAR(32767) NULL,
    recruiter_name VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.recruiters_typed
SELECT
    "Recruitment Team",
    "Recruiter Name",
    $global_nickname
FROM
    KENNAMETAL_STAGING.recruiters_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.source_segmentation_typed
;

CREATE TABLE KENNAMETAL_STAGING.source_segmentation_typed (
    net_suite_specific_source VARCHAR(32767) NULL,
    net_suite_broad_source_category VARCHAR(32767) NULL,
    ats_source_details VARCHAR(32767) NULL,
    net_suite_internal_external VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.source_segmentation_typed
SELECT
    "NetSuite Specific Source",
    "NetSuite Broad Source Category",
    "ATS Source Details",
    "NetSuite Internal/External",
    $global_nickname
FROM
    KENNAMETAL_STAGING.source_segmentation_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.state_typed
;

CREATE TABLE KENNAMETAL_STAGING.state_typed (
    state_province_other VARCHAR(32767) NULL,
    job_state_name VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.state_typed
SELECT
    "State/Province/Other",
    "Job State Name",
    $global_nickname
FROM
    KENNAMETAL_STAGING.state_untyped
;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS KENNAMETAL_STAGING.job_segmentation_typed
;

CREATE TABLE KENNAMETAL_STAGING.job_segmentation_typed (
    net_suite_job_category_minor VARCHAR(32767) NULL,
    net_suite_job_level VARCHAR(32767) NULL,
    net_suite_job_category_major VARCHAR(32767) NULL,
    net_suite_job_category_specific VARCHAR(32767) NULL,
    job_title VARCHAR(32767) NULL,
    global_nickname VARCHAR(255) NULL )
;

INSERT INTO KENNAMETAL_STAGING.job_segmentation_typed
SELECT
    "NetSuite Job Category Minor",
    "NetSuite Job Level",
    "NetSuite Job Category Major",
    "NetSuite Job Category Specific",
    "Job Title",
    $global_nickname
FROM
    KENNAMETAL_STAGING.job_segmentation_untyped
;
