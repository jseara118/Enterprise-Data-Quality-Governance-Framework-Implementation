-- ============================================================
-- G. DATA REMEDIATION / NORMALIZATION LOGIC
-- FieldOps Pro — Raw → Staging Transformations
-- Shows what gets fixed, quarantined, standardized, rejected
-- ============================================================

USE FieldOpsDQ;
GO

-- ============================================================
-- STEP 1: STANDARDIZE AND LOAD customers INTO STAGING
-- Remediation logic applied inline before insert
-- ============================================================

TRUNCATE TABLE stg.customers;

INSERT INTO stg.customers (
    customer_id, customer_name, customer_type, account_status,
    credit_limit, primary_contact, email_address, phone_number,
    billing_city, billing_state, billing_zip, region_code,
    created_date, last_modified,
    is_quarantined, quarantine_reason, dq_score, source_system,
    stg_batch_id, is_duplicate, duplicate_of_key
)
WITH deduped AS (
    -- Deduplication: keep the most recently modified record per email
    -- Flag lower-priority duplicates for quarantine
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(LTRIM(RTRIM(email_address)))
            ORDER BY TRY_CAST(last_modified AS DATETIME2) DESC
        ) AS rn_email_dedup,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _extract_timestamp DESC
        ) AS rn_id_dedup
    FROM raw.customers
),
scored AS (
    SELECT
        d.*,
        -- ── Quarantine Flags ──────────────────────────────
        CASE
            -- CRITICAL: NULL customer_name
            WHEN customer_name IS NULL OR LTRIM(RTRIM(customer_name)) = ''
                THEN 1
            -- CRITICAL: Invalid account_status
            WHEN UPPER(LTRIM(RTRIM(account_status))) NOT IN ('ACTIVE','INACTIVE','SUSPENDED','PENDING')
                 AND account_status IS NOT NULL
                THEN 1
            -- HIGH: Unparseable credit_limit
            WHEN TRY_CAST(credit_limit AS DECIMAL(12,2)) IS NULL AND credit_limit IS NOT NULL
                THEN 1
            -- HIGH: Negative credit_limit
            WHEN TRY_CAST(credit_limit AS DECIMAL(12,2)) < 0
                THEN 1
            ELSE 0
        END AS should_quarantine,

        -- ── Quarantine Reason (most critical first) ───────
        CASE
            WHEN customer_name IS NULL OR LTRIM(RTRIM(customer_name)) = ''
                THEN 'CRITICAL: customer_name is NULL'
            WHEN UPPER(LTRIM(RTRIM(account_status))) NOT IN ('ACTIVE','INACTIVE','SUSPENDED','PENDING')
                 AND account_status IS NOT NULL
                THEN 'CRITICAL: invalid account_status [' + account_status + ']'
            WHEN TRY_CAST(credit_limit AS DECIMAL(12,2)) IS NULL AND credit_limit IS NOT NULL
                THEN 'HIGH: credit_limit cannot be parsed [' + credit_limit + ']'
            WHEN TRY_CAST(credit_limit AS DECIMAL(12,2)) < 0
                THEN 'HIGH: negative credit_limit'
            ELSE NULL
        END AS q_reason,

        -- ── Standardization ───────────────────────────────
        -- Normalize account_status
        CASE
            WHEN UPPER(LTRIM(RTRIM(account_status))) IN ('ACTIVE', 'ACTIV')
                THEN 'ACTIVE'
            WHEN UPPER(LTRIM(RTRIM(account_status))) IN ('INACTIVE', 'INACT')
                THEN 'INACTIVE'
            WHEN UPPER(LTRIM(RTRIM(account_status))) IN ('SUSPENDED', 'TEMP SUSPEND', 'SUSPENDED')
                THEN 'SUSPENDED'
            WHEN UPPER(LTRIM(RTRIM(account_status))) = 'PENDING'
                THEN 'PENDING'
            ELSE account_status  -- pass through for quarantine review
        END AS account_status_std,

        -- Normalize customer_type
        CASE
            WHEN UPPER(LTRIM(RTRIM(customer_type))) IN ('ENTERPRISE', 'ENTERP')
                THEN 'ENTERPRISE'
            WHEN UPPER(LTRIM(RTRIM(customer_type))) IN ('GOVERNMENT', 'GOVT', 'GOV')
                THEN 'GOVERNMENT'
            WHEN UPPER(LTRIM(RTRIM(customer_type))) = 'SMB'
                THEN 'SMB'
            ELSE 'UNKNOWN'  -- non-standard types mapped to UNKNOWN
        END AS customer_type_std,

        -- Standardize billing_state (trim whitespace, uppercase)
        UPPER(LTRIM(RTRIM(billing_state))) AS billing_state_std,

        -- Standardize billing_zip (trim, take first 5 for US)
        LEFT(LTRIM(RTRIM(billing_zip)), 5) AS billing_zip_std,

        -- Normalize phone_number — strip to digits for comparison
        -- (store standardized format: (NXX) NXX-XXXX)
        phone_number AS phone_std,  -- simplified; full normalization in production would use regex

        -- DQ Score calculation (0-100, weighted)
        100
            - (CASE WHEN customer_name IS NULL THEN 30 ELSE 0 END)
            - (CASE WHEN account_status IS NULL THEN 20 ELSE 0 END)
            - (CASE WHEN TRY_CAST(credit_limit AS DECIMAL(12,2)) IS NULL AND credit_limit IS NOT NULL THEN 15 ELSE 0 END)
            - (CASE WHEN email_address IS NULL THEN 10 ELSE 0 END)
            - (CASE WHEN email_address NOT LIKE '%@%.%' AND email_address IS NOT NULL THEN 10 ELSE 0 END)
            - (CASE WHEN primary_contact IS NULL THEN 5 ELSE 0 END)
            - (CASE WHEN TRY_CAST(created_date AS DATE) > CAST(GETDATE() AS DATE) THEN 10 ELSE 0 END)
            AS dq_score_calc,

        -- Duplicate detection flag
        CASE WHEN rn_email_dedup > 1 OR rn_id_dedup > 1 THEN 1 ELSE 0 END AS is_dup_flag,
        CASE
            WHEN rn_id_dedup > 1 THEN 'Duplicate customer_id — lower priority retained'
            WHEN rn_email_dedup > 1 THEN 'Duplicate email address — older record'
            ELSE NULL
        END AS dup_reason
    FROM deduped
)
SELECT
    customer_id,
    LTRIM(RTRIM(customer_name))                                     AS customer_name,
    customer_type_std,
    account_status_std,
    TRY_CAST(credit_limit AS DECIMAL(12,2))                         AS credit_limit,
    LTRIM(RTRIM(primary_contact))                                   AS primary_contact,
    LOWER(LTRIM(RTRIM(email_address)))                              AS email_address,
    phone_std,
    LTRIM(RTRIM(billing_city))                                      AS billing_city,
    billing_state_std,
    billing_zip_std,
    LTRIM(RTRIM(region_code))                                       AS region_code,
    TRY_CAST(created_date AS DATE)                                  AS created_date,
    TRY_CAST(last_modified AS DATETIME2)                            AS last_modified,
    -- Quarantine: set if critical/high failure OR is duplicate
    CASE WHEN should_quarantine = 1 OR is_dup_flag = 1 THEN 1 ELSE 0 END AS is_quarantined,
    COALESCE(q_reason, dup_reason)                                  AS quarantine_reason,
    CAST(CASE WHEN dq_score_calc < 0 THEN 0 ELSE dq_score_calc END AS DECIMAL(5,2)) AS dq_score,
    source_system,
    _batch_id,
    is_dup_flag,
    CASE WHEN is_dup_flag = 1 THEN
        (SELECT TOP 1 s2.customer_id
         FROM raw.customers s2
         WHERE LOWER(s2.email_address) = LOWER(scored.email_address)
           AND s2.customer_id <> scored.customer_id)
    END                                                             AS duplicate_of_key
FROM scored;

-- ============================================================
-- STEP 2: LOAD QUARANTINE LOG FROM STAGING
-- Captures failed records for governance review
-- ============================================================

INSERT INTO dq.quarantine_log (
    run_id, source_table, source_key, rule_code, severity, quarantine_reason, quarantine_date
)
SELECT
    'RUN-' + FORMAT(GETDATE(), 'yyyyMMdd-HHmmss'),
    'raw.customers',
    customer_id,
    CASE
        WHEN quarantine_reason LIKE 'CRITICAL%' THEN 'C-001'
        WHEN quarantine_reason LIKE 'HIGH%' THEN 'V-003'
        ELSE 'U-002'
    END,
    CASE
        WHEN quarantine_reason LIKE 'CRITICAL%' THEN 'CRITICAL'
        ELSE 'HIGH'
    END,
    quarantine_reason,
    SYSDATETIME()
FROM stg.customers
WHERE is_quarantined = 1;

-- ============================================================
-- STEP 3: STANDARDIZE WORK ORDERS INTO STAGING
-- ============================================================

TRUNCATE TABLE stg.work_orders;

INSERT INTO stg.work_orders (
    work_order_id, work_order_number, site_id, customer_id,
    work_order_type, priority, status, description,
    scheduled_date, actual_start, actual_end,
    estimated_hours, actual_hours, labor_cost, parts_cost, total_cost,
    sla_due_date, sla_met, resolution_code, created_date, last_modified,
    is_quarantined, quarantine_reason, dq_score, ref_integrity_ok, stg_batch_id
)
WITH deduped_wo AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY work_order_id ORDER BY _extract_timestamp DESC) AS rn
    FROM raw.work_orders
),
enriched AS (
    SELECT
        d.*,
        -- Referential integrity flags
        CASE WHEN c.customer_id IS NOT NULL THEN 1 ELSE 0 END AS cust_ref_ok,

        -- Status normalization
        CASE
            WHEN UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) = 'SCHEDULED'     THEN 'SCHEDULED'
            WHEN UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) = 'IN_PROGRESS'   THEN 'IN_PROGRESS'
            WHEN UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) IN ('COMPLETED', 'COMPLETE') THEN 'COMPLETED'
            WHEN UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) = 'CANCELLED'     THEN 'CANCELLED'
            WHEN UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) = 'ON_HOLD'       THEN 'ON_HOLD'
            ELSE 'UNKNOWN'
        END AS status_std,

        -- SLA met normalization
        CASE
            WHEN UPPER(LTRIM(RTRIM(sla_met))) IN ('Y', 'YES', '1', 'TRUE') THEN 1
            WHEN UPPER(LTRIM(RTRIM(sla_met))) IN ('N', 'NO', '0', 'FALSE') THEN 0
            ELSE NULL
        END AS sla_met_std,

        -- Quarantine logic (CRITICAL violations)
        CASE
            WHEN customer_id IS NULL THEN 1
            WHEN c.customer_id IS NULL AND customer_id IS NOT NULL THEN 1  -- orphan
            WHEN TRY_CAST(actual_end AS DATETIME2) IS NOT NULL
                 AND TRY_CAST(actual_start AS DATETIME2) IS NOT NULL
                 AND TRY_CAST(actual_end AS DATETIME2) < TRY_CAST(actual_start AS DATETIME2) THEN 1
            WHEN TRY_CAST(labor_cost AS DECIMAL(12,2)) < 0 THEN 1
            WHEN TRY_CAST(parts_cost AS DECIMAL(12,2)) < 0 THEN 1
            ELSE 0
        END AS should_quarantine,

        CASE
            WHEN customer_id IS NULL THEN 'CRITICAL: customer_id is NULL'
            WHEN c.customer_id IS NULL AND customer_id IS NOT NULL THEN 'CRITICAL: orphaned customer_id [' + customer_id + ']'
            WHEN TRY_CAST(actual_end AS DATETIME2) < TRY_CAST(actual_start AS DATETIME2) THEN 'CRITICAL: actual_end before actual_start'
            WHEN TRY_CAST(labor_cost AS DECIMAL(12,2)) < 0 OR TRY_CAST(parts_cost AS DECIMAL(12,2)) < 0 THEN 'HIGH: negative cost values'
            ELSE NULL
        END AS q_reason_wo,

        -- DQ score
        100
            - (CASE WHEN customer_id IS NULL THEN 30 ELSE 0 END)
            - (CASE WHEN c.customer_id IS NULL AND customer_id IS NOT NULL THEN 25 ELSE 0 END)
            - (CASE WHEN site_id IS NULL THEN 15 ELSE 0 END)
            - (CASE WHEN TRY_CAST(actual_end AS DATETIME2) < TRY_CAST(actual_start AS DATETIME2) THEN 20 ELSE 0 END)
            - (CASE WHEN TRY_CAST(labor_cost AS DECIMAL(12,2)) < 0 THEN 15 ELSE 0 END)
            - (CASE WHEN description IS NULL THEN 5 ELSE 0 END)
            - (CASE WHEN priority IS NULL THEN 5 ELSE 0 END)
        AS wo_dq_score

    FROM deduped_wo d
    LEFT JOIN raw.customers c ON d.customer_id = c.customer_id
    WHERE d.rn = 1  -- keep only the latest version of each WO
)
SELECT
    work_order_id,
    work_order_number,
    site_id,
    customer_id,
    -- Normalize work_order_type
    CASE
        WHEN UPPER(work_order_type) IN ('PREVENTIVE_MAINTENANCE', 'CORRECTIVE_MAINTENANCE',
                                         'INSPECTION', 'EMERGENCY_REPAIR', 'INSTALLATION')
            THEN UPPER(work_order_type)
        ELSE 'OTHER'
    END                                                     AS work_order_type,
    -- Normalize priority
    CASE
        WHEN UPPER(LTRIM(RTRIM(priority))) IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')
            THEN UPPER(LTRIM(RTRIM(priority)))
        ELSE NULL
    END                                                     AS priority,
    status_std                                              AS status,
    LTRIM(RTRIM(description))                               AS description,
    TRY_CAST(scheduled_date AS DATE)                        AS scheduled_date,
    TRY_CAST(actual_start AS DATETIME2)                     AS actual_start,
    -- Fix inverted actual_end: if end < start, null it out (quarantined above)
    CASE
        WHEN TRY_CAST(actual_end AS DATETIME2) < TRY_CAST(actual_start AS DATETIME2) THEN NULL
        ELSE TRY_CAST(actual_end AS DATETIME2)
    END                                                     AS actual_end,
    TRY_CAST(estimated_hours AS DECIMAL(6,2))               AS estimated_hours,
    -- Cap impossible actual_hours values at NULL (flag for review)
    CASE
        WHEN TRY_CAST(actual_hours AS DECIMAL(6,2)) > 24 THEN NULL
        WHEN TRY_CAST(actual_hours AS DECIMAL(6,2)) < 0 THEN NULL
        ELSE TRY_CAST(actual_hours AS DECIMAL(6,2))
    END                                                     AS actual_hours,
    -- Replace negative costs with NULL
    CASE WHEN TRY_CAST(labor_cost AS DECIMAL(12,2)) < 0 THEN NULL ELSE TRY_CAST(labor_cost AS DECIMAL(12,2)) END AS labor_cost,
    CASE WHEN TRY_CAST(parts_cost AS DECIMAL(12,2)) < 0 THEN NULL ELSE TRY_CAST(parts_cost AS DECIMAL(12,2)) END AS parts_cost,
    CASE WHEN TRY_CAST(total_cost AS DECIMAL(12,2)) < 0 THEN NULL ELSE TRY_CAST(total_cost AS DECIMAL(12,2)) END AS total_cost,
    TRY_CAST(sla_due_date AS DATETIME2)                     AS sla_due_date,
    sla_met_std                                             AS sla_met,
    LTRIM(RTRIM(resolution_code))                           AS resolution_code,
    TRY_CAST(created_date AS DATE)                          AS created_date,
    TRY_CAST(last_modified AS DATETIME2)                    AS last_modified,
    should_quarantine                                       AS is_quarantined,
    q_reason_wo                                             AS quarantine_reason,
    CAST(CASE WHEN wo_dq_score < 0 THEN 0 ELSE wo_dq_score END AS DECIMAL(5,2)) AS dq_score,
    cust_ref_ok                                             AS ref_integrity_ok,
    _batch_id
FROM enriched;

-- ============================================================
-- STEP 4: BUILD CURATED DIMENSION — dim_customer
-- Only non-quarantined, high-quality customer records
-- ============================================================

-- Populate dim_date (simplified — use a date generator in production)
-- Insert a range of dates: 2015-01-01 to 2026-12-31
WITH date_series AS (
    SELECT DATEADD(DAY, n, '2015-01-01') AS dt
    FROM (
        SELECT TOP 4018 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.columns c1 CROSS JOIN sys.columns c2
    ) x
    WHERE DATEADD(DAY, n, '2015-01-01') <= '2026-12-31'
)
INSERT INTO curated.dim_date (
    date_sk, full_date, year_num, quarter_num, month_num, month_name,
    week_num, day_of_week, day_name, is_weekend
)
SELECT
    CAST(FORMAT(dt, 'yyyyMMdd') AS INT)  AS date_sk,
    dt                                   AS full_date,
    YEAR(dt)                             AS year_num,
    DATEPART(QUARTER, dt)                AS quarter_num,
    MONTH(dt)                            AS month_num,
    FORMAT(dt, 'MMMM')                  AS month_name,
    DATEPART(WEEK, dt)                   AS week_num,
    DATEPART(WEEKDAY, dt)               AS day_of_week,
    FORMAT(dt, 'dddd')                  AS day_name,
    CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
FROM date_series
WHERE NOT EXISTS (SELECT 1 FROM curated.dim_date dd WHERE dd.date_sk = CAST(FORMAT(dt, 'yyyyMMdd') AS INT));

-- Populate dim_customer (only clean, non-quarantined records)
INSERT INTO curated.dim_customer (
    customer_id, customer_name, customer_type, account_status,
    region_code, credit_limit, effective_from, effective_to, is_current, record_source
)
SELECT
    customer_id,
    customer_name,
    customer_type,
    account_status,
    region_code,
    credit_limit,
    ISNULL(created_date, CAST('2000-01-01' AS DATE)) AS effective_from,
    NULL                                              AS effective_to,
    1                                                 AS is_current,
    stg_batch_id                                      AS record_source
FROM stg.customers
WHERE is_quarantined = 0
  AND is_duplicate = 0;

-- Populate fact_work_orders (join to curated dimensions)
INSERT INTO curated.fact_work_orders (
    work_order_id, work_order_number, customer_sk, scheduled_date_sk, completed_date_sk,
    work_order_type, priority, status, estimated_hours, actual_hours,
    labor_cost, parts_cost, total_cost, sla_met, load_date, record_source
)
SELECT
    wo.work_order_id,
    wo.work_order_number,
    dc.customer_sk,
    CAST(FORMAT(wo.scheduled_date, 'yyyyMMdd') AS INT)                  AS scheduled_date_sk,
    CAST(FORMAT(CAST(wo.actual_end AS DATE), 'yyyyMMdd') AS INT)        AS completed_date_sk,
    wo.work_order_type,
    wo.priority,
    wo.status,
    wo.estimated_hours,
    wo.actual_hours,
    wo.labor_cost,
    wo.parts_cost,
    -- Recalculate total_cost where inconsistent
    ISNULL(wo.labor_cost, 0) + ISNULL(wo.parts_cost, 0)                AS total_cost,
    wo.sla_met,
    SYSDATETIME(),
    wo.stg_batch_id
FROM stg.work_orders wo
LEFT JOIN curated.dim_customer dc
    ON wo.customer_id = dc.customer_id AND dc.is_current = 1
WHERE wo.is_quarantined = 0;

PRINT 'Staging and Curated load complete.';
PRINT 'Review dq.quarantine_log for rejected records.';
GO

-- ============================================================
-- BEFORE / AFTER COMPARISON QUERY
-- Key metric for demo presentation
-- ============================================================

SELECT
    'RAW (Dirty)'       AS layer,
    COUNT(*)            AS total_customers,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_names,
    SUM(CASE WHEN UPPER(LTRIM(RTRIM(account_status))) NOT IN ('ACTIVE','INACTIVE','SUSPENDED','PENDING') OR account_status IS NULL THEN 1 ELSE 0 END) AS invalid_status,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_ids
FROM raw.customers

UNION ALL

SELECT
    'STAGING (Cleansed)' AS layer,
    COUNT(*)             AS total_customers,
    SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_names,
    SUM(CASE WHEN account_status NOT IN ('ACTIVE','INACTIVE','SUSPENDED','PENDING') OR account_status IS NULL THEN 1 ELSE 0 END) AS invalid_status,
    COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_ids
FROM stg.customers
WHERE is_quarantined = 0

UNION ALL

SELECT
    'CURATED (Analytics-Ready)' AS layer,
    COUNT(*)                    AS total_customers,
    0                           AS null_names,
    0                           AS invalid_status,
    0                           AS duplicate_ids
FROM curated.dim_customer
WHERE is_current = 1;
GO