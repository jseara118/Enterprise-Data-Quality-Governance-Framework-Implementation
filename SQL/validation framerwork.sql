-- ============================================================
-- F. SQL VALIDATION FRAMEWORK
-- FieldOps Pro — Enterprise Data Quality Checks
-- Organized by DQ Dimension | Annotated with severity
-- ============================================================

USE FieldOpsDQ;
GO

-- ============================================================
-- DIMENSION 1: COMPLETENESS
-- Checks for NULLs and blanks in required fields
-- ============================================================

-- ── CHECK C-001 ──────────────────────────────────────────────
-- What:     customer_name is NULL (required field)
-- Severity: CRITICAL
-- Expected: 0 failures
-- ─────────────────────────────────────────────────────────────
SELECT
    'C-001'             AS rule_code,
    'CRITICAL'          AS severity,
    'Completeness'      AS dq_dimension,
    'raw.customers'     AS target_table,
    'customer_name'     AS target_column,
    COUNT(*)            AS total_records,
    SUM(CASE WHEN customer_name IS NULL OR LTRIM(RTRIM(customer_name)) = '' THEN 1 ELSE 0 END) AS failed_records,
    CAST(
        100.0 * (1 - CAST(SUM(CASE WHEN customer_name IS NULL OR LTRIM(RTRIM(customer_name)) = '' THEN 1 ELSE 0 END) AS FLOAT) / NULLIF(COUNT(*), 0))
        AS DECIMAL(5,2)
    )                   AS pass_rate_pct
FROM raw.customers;

-- ── CHECK C-002 ──────────────────────────────────────────────
-- What:     account_status is NULL on any customer
-- Severity: CRITICAL
-- Expected: 0 failures
-- ─────────────────────────────────────────────────────────────
SELECT
    'C-002'             AS rule_code,
    'CRITICAL'          AS severity,
    'Completeness'      AS dq_dimension,
    customer_id,
    customer_name,
    account_status
FROM raw.customers
WHERE account_status IS NULL;

-- ── CHECK C-003 ──────────────────────────────────────────────
-- What:     Work orders with NULL site_id or NULL customer_id
-- Severity: CRITICAL
-- Expected: 0 failures
-- ─────────────────────────────────────────────────────────────
SELECT
    'C-003'             AS rule_code,
    'CRITICAL'          AS severity,
    work_order_id,
    work_order_number,
    CASE WHEN site_id IS NULL THEN 'site_id is NULL' ELSE '' END     AS site_issue,
    CASE WHEN customer_id IS NULL THEN 'customer_id is NULL' ELSE '' END AS cust_issue
FROM raw.work_orders
WHERE site_id IS NULL OR customer_id IS NULL;

-- ── CHECK C-004 ──────────────────────────────────────────────
-- What:     Completed WOs missing actual_start or actual_end
-- Severity: HIGH
-- Expected: 0 failures on completed WOs
-- ─────────────────────────────────────────────────────────────
SELECT
    'C-004'             AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    status,
    actual_start,
    actual_end
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(status))) IN ('COMPLETED', 'COMPLETE')
  AND (actual_start IS NULL OR actual_end IS NULL);

-- ── CHECK C-005 ──────────────────────────────────────────────
-- What:     Technicians with NULL employee_number
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'C-005'             AS rule_code,
    'HIGH'              AS severity,
    technician_id,
    first_name,
    last_name,
    employee_number,
    employment_status
FROM raw.technicians
WHERE employee_number IS NULL;

-- ── CHECK C-006 ──────────────────────────────────────────────
-- What:     Invoices missing customer contact info (join to customers)
--           Checks PRIMARY CONTACT missing at customer level for invoiced accounts
-- Severity: MEDIUM
-- ─────────────────────────────────────────────────────────────
SELECT
    'C-006'             AS rule_code,
    'MEDIUM'            AS severity,
    i.invoice_id,
    i.invoice_number,
    i.customer_id,
    c.customer_name,
    c.primary_contact,
    c.email_address,
    c.phone_number
FROM raw.invoices i
JOIN raw.customers c ON i.customer_id = c.customer_id
WHERE c.primary_contact IS NULL
   OR c.email_address IS NULL
   OR c.phone_number IS NULL;


-- ============================================================
-- DIMENSION 2: UNIQUENESS
-- Checks for duplicate records across key tables
-- ============================================================

-- ── CHECK U-001 ──────────────────────────────────────────────
-- What:     Duplicate customer_id in raw.customers
-- Severity: CRITICAL
-- Expected: Each customer_id appears exactly once
-- ─────────────────────────────────────────────────────────────
SELECT
    'U-001'             AS rule_code,
    'CRITICAL'          AS severity,
    customer_id,
    COUNT(*)            AS occurrence_count
FROM raw.customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- ── CHECK U-002 ──────────────────────────────────────────────
-- What:     Fuzzy duplicate customers (same email — different ID)
--           Detects cross-system duplicates from legacy migration
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'U-002'             AS rule_code,
    'HIGH'              AS severity,
    a.customer_id       AS customer_id_1,
    b.customer_id       AS customer_id_2,
    a.customer_name     AS name_1,
    b.customer_name     AS name_2,
    a.email_address     AS shared_email,
    a.source_system     AS source_1,
    b.source_system     AS source_2
FROM raw.customers a
JOIN raw.customers b
    ON LOWER(a.email_address) = LOWER(b.email_address)
   AND a.customer_id < b.customer_id  -- avoid self-join and reverse duplicates
WHERE a.email_address IS NOT NULL;

-- ── CHECK U-003 ──────────────────────────────────────────────
-- What:     Duplicate work_order_id in raw.work_orders
-- Severity: CRITICAL
-- ─────────────────────────────────────────────────────────────
SELECT
    'U-003'             AS rule_code,
    'CRITICAL'          AS severity,
    work_order_id,
    COUNT(*)            AS occurrence_count,
    MIN(_extract_timestamp) AS first_seen,
    MAX(_extract_timestamp) AS last_seen
FROM raw.work_orders
GROUP BY work_order_id
HAVING COUNT(*) > 1;

-- ── CHECK U-004 ──────────────────────────────────────────────
-- What:     Multiple invoices for the same work order
--           (One WO should produce at most one invoice)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'U-004'             AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    COUNT(*)            AS invoice_count,
    STRING_AGG(invoice_id, ', ') AS invoice_ids
FROM raw.invoices
GROUP BY work_order_id
HAVING COUNT(*) > 1;

-- ── CHECK U-005 ──────────────────────────────────────────────
-- What:     Technician assigned to same work order more than once
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'U-005'             AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    technician_id,
    COUNT(*)            AS assignment_count,
    STRING_AGG(assignment_id, ', ') AS assignment_ids
FROM raw.work_order_assignments
GROUP BY work_order_id, technician_id
HAVING COUNT(*) > 1;

-- ── CHECK U-006 ──────────────────────────────────────────────
-- What:     Duplicate employee_number across technicians
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'U-006'             AS rule_code,
    'HIGH'              AS severity,
    employee_number,
    COUNT(*)            AS occurrence_count,
    STRING_AGG(technician_id, ', ') AS technician_ids
FROM raw.technicians
WHERE employee_number IS NOT NULL
GROUP BY employee_number
HAVING COUNT(*) > 1;


-- ============================================================
-- DIMENSION 3: VALIDITY
-- Checks for invalid values against allowed domains and formats
-- ============================================================

-- ── CHECK V-001 ──────────────────────────────────────────────
-- What:     customer account_status not in allowed domain
--           Valid values: ACTIVE, INACTIVE, SUSPENDED, PENDING
-- Severity: CRITICAL
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-001'             AS rule_code,
    'CRITICAL'          AS severity,
    customer_id,
    customer_name,
    account_status,
    'Not in allowed domain: ACTIVE, INACTIVE, SUSPENDED, PENDING' AS violation_detail
FROM raw.customers
WHERE UPPER(LTRIM(RTRIM(account_status))) NOT IN ('ACTIVE', 'INACTIVE', 'SUSPENDED', 'PENDING')
   OR account_status IS NULL;

-- ── CHECK V-002 ──────────────────────────────────────────────
-- What:     Work order status not in allowed domain
--           Valid values: SCHEDULED, IN_PROGRESS, COMPLETED, CANCELLED, ON_HOLD
-- Severity: CRITICAL
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-002'             AS rule_code,
    'CRITICAL'          AS severity,
    work_order_id,
    work_order_number,
    status,
    'Invalid status value' AS violation_detail
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) NOT IN
      ('SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'CANCELLED', 'ON_HOLD');

-- ── CHECK V-003 ──────────────────────────────────────────────
-- What:     Negative credit_limit (must be >= 0)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-003'             AS rule_code,
    'HIGH'              AS severity,
    customer_id,
    customer_name,
    credit_limit        AS raw_credit_limit
FROM raw.customers
WHERE TRY_CAST(credit_limit AS DECIMAL(12,2)) < 0;

-- ── CHECK V-004 ──────────────────────────────────────────────
-- What:     credit_limit is not a valid numeric value (unparseable)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-004'             AS rule_code,
    'HIGH'              AS severity,
    customer_id,
    customer_name,
    credit_limit        AS raw_credit_limit,
    'Cannot convert to decimal' AS violation_detail
FROM raw.customers
WHERE TRY_CAST(credit_limit AS DECIMAL(12,2)) IS NULL
  AND credit_limit IS NOT NULL;

-- ── CHECK V-005 ──────────────────────────────────────────────
-- What:     Invalid email format in customers
-- Severity: MEDIUM
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-005'             AS rule_code,
    'MEDIUM'            AS severity,
    customer_id,
    customer_name,
    email_address,
    'Invalid email format' AS violation_detail
FROM raw.customers
WHERE email_address IS NOT NULL
  AND (
       email_address NOT LIKE '%@%.%'   -- must contain @ and a dot after @
    OR email_address LIKE '% %'          -- no spaces
    OR LEN(email_address) < 6
  );

-- ── CHECK V-006 ──────────────────────────────────────────────
-- What:     Negative labor_cost, parts_cost, or total_cost on work orders
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-006'             AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    labor_cost,
    parts_cost,
    total_cost,
    'Negative monetary amount' AS violation_detail
FROM raw.work_orders
WHERE TRY_CAST(labor_cost AS DECIMAL(12,2)) < 0
   OR TRY_CAST(parts_cost AS DECIMAL(12,2)) < 0
   OR TRY_CAST(total_cost AS DECIMAL(12,2)) < 0;

-- ── CHECK V-007 ──────────────────────────────────────────────
-- What:     Negative payment amounts or invoice totals
-- Severity: CRITICAL (financial data)
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-007'             AS rule_code,
    'CRITICAL'          AS severity,
    invoice_id,
    invoice_number,
    subtotal,
    total_amount,
    payment_received,
    'Negative financial amount' AS violation_detail
FROM raw.invoices
WHERE TRY_CAST(subtotal AS DECIMAL(12,2)) < 0
   OR TRY_CAST(total_amount AS DECIMAL(12,2)) < 0
   OR TRY_CAST(payment_received AS DECIMAL(12,2)) < 0;

-- ── CHECK V-008 ──────────────────────────────────────────────
-- What:     Work order type not in allowed domain
--           Valid: PREVENTIVE_MAINTENANCE, CORRECTIVE_MAINTENANCE,
--                  INSPECTION, EMERGENCY_REPAIR, INSTALLATION
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-008'             AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    work_order_type,
    'WO type not in allowed domain' AS violation_detail
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(work_order_type))) NOT IN (
    'PREVENTIVE_MAINTENANCE', 'CORRECTIVE_MAINTENANCE',
    'INSPECTION', 'EMERGENCY_REPAIR', 'INSTALLATION'
);

-- ── CHECK V-009 ──────────────────────────────────────────────
-- What:     Technician employment_status inconsistent casing / invalid
--           Valid: ACTIVE, TERMINATED, ON_LEAVE, SUSPENDED
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-009'             AS rule_code,
    'HIGH'              AS severity,
    technician_id,
    first_name,
    last_name,
    employment_status,
    UPPER(LTRIM(RTRIM(REPLACE(employment_status, ' ', '_')))) AS normalized_attempt
FROM raw.technicians
WHERE UPPER(LTRIM(RTRIM(REPLACE(employment_status, ' ', '_')))) NOT IN (
    'ACTIVE', 'TERMINATED', 'ON_LEAVE', 'SUSPENDED'
);

-- ── CHECK V-010 ──────────────────────────────────────────────
-- What:     Negative hourly_rate on technicians
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'V-010'             AS rule_code,
    'HIGH'              AS severity,
    technician_id,
    full_name,
    hourly_rate,
    'Negative hourly rate' AS violation_detail
FROM raw.technicians
WHERE TRY_CAST(hourly_rate AS DECIMAL(8,2)) < 0;


-- ============================================================
-- DIMENSION 4: REFERENTIAL INTEGRITY
-- Checks that foreign keys resolve to valid parent records
-- ============================================================

-- ── CHECK R-001 ──────────────────────────────────────────────
-- What:     Work orders referencing customer_id not in customers table
-- Severity: CRITICAL
-- ─────────────────────────────────────────────────────────────
SELECT
    'R-001'             AS rule_code,
    'CRITICAL'          AS severity,
    wo.work_order_id,
    wo.work_order_number,
    wo.customer_id      AS orphaned_customer_id,
    'customer_id not found in raw.customers' AS violation_detail
FROM raw.work_orders wo
LEFT JOIN raw.customers c ON wo.customer_id = c.customer_id
WHERE c.customer_id IS NULL
  AND wo.customer_id IS NOT NULL;

-- ── CHECK R-002 ──────────────────────────────────────────────
-- What:     Invoices referencing work_order_id not in work_orders
-- Severity: CRITICAL (financial integrity)
-- ─────────────────────────────────────────────────────────────
SELECT
    'R-002'             AS rule_code,
    'CRITICAL'          AS severity,
    i.invoice_id,
    i.invoice_number,
    i.work_order_id     AS orphaned_wo_id,
    '$' + ISNULL(i.total_amount, '0.00') AS at_risk_amount,
    'work_order_id not found in raw.work_orders' AS violation_detail
FROM raw.invoices i
LEFT JOIN raw.work_orders wo ON i.work_order_id = wo.work_order_id
WHERE wo.work_order_id IS NULL
  AND i.work_order_id IS NOT NULL;

-- ── CHECK R-003 ──────────────────────────────────────────────
-- What:     Assignments referencing work_order_id not in work_orders
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'R-003'             AS rule_code,
    'HIGH'              AS severity,
    a.assignment_id,
    a.work_order_id     AS orphaned_wo_id,
    a.technician_id,
    'work_order_id not found in raw.work_orders' AS violation_detail
FROM raw.work_order_assignments a
LEFT JOIN raw.work_orders wo ON a.work_order_id = wo.work_order_id
WHERE wo.work_order_id IS NULL
  AND a.work_order_id IS NOT NULL;

-- ── CHECK R-004 ──────────────────────────────────────────────
-- What:     Assignments referencing technician_id not in technicians
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'R-004'             AS rule_code,
    'HIGH'              AS severity,
    a.assignment_id,
    a.work_order_id,
    a.technician_id     AS orphaned_tech_id,
    'technician_id not found in raw.technicians' AS violation_detail
FROM raw.work_order_assignments a
LEFT JOIN raw.technicians t ON a.technician_id = t.technician_id
WHERE t.technician_id IS NULL
  AND a.technician_id IS NOT NULL;


-- ============================================================
-- DIMENSION 5: CONSISTENCY
-- Cross-field and cross-table business logic checks
-- ============================================================

-- ── CHECK CS-001 ─────────────────────────────────────────────
-- What:     actual_end is before actual_start (impossible time range)
-- Severity: CRITICAL
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-001'            AS rule_code,
    'CRITICAL'          AS severity,
    work_order_id,
    work_order_number,
    actual_start,
    actual_end,
    DATEDIFF(MINUTE, TRY_CAST(actual_end AS DATETIME2), TRY_CAST(actual_start AS DATETIME2)) AS minutes_inverted
FROM raw.work_orders
WHERE TRY_CAST(actual_start AS DATETIME2) IS NOT NULL
  AND TRY_CAST(actual_end AS DATETIME2) IS NOT NULL
  AND TRY_CAST(actual_end AS DATETIME2) < TRY_CAST(actual_start AS DATETIME2);

-- ── CHECK CS-002 ─────────────────────────────────────────────
-- What:     hire_date is after termination_date (logically impossible)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-002'            AS rule_code,
    'HIGH'              AS severity,
    technician_id,
    full_name,
    hire_date,
    termination_date,
    'hire_date is after termination_date' AS violation_detail
FROM raw.technicians
WHERE TRY_CAST(hire_date AS DATE) IS NOT NULL
  AND TRY_CAST(termination_date AS DATE) IS NOT NULL
  AND TRY_CAST(hire_date AS DATE) > TRY_CAST(termination_date AS DATE);

-- ── CHECK CS-003 ─────────────────────────────────────────────
-- What:     WO is COMPLETED but actual_start or actual_end is NULL
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-003'            AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    status,
    actual_start,
    actual_end,
    'Completed WO missing time data' AS violation_detail
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(status))) IN ('COMPLETED', 'COMPLETE')
  AND (actual_start IS NULL OR actual_end IS NULL);

-- ── CHECK CS-004 ─────────────────────────────────────────────
-- What:     total_cost does not equal labor_cost + parts_cost
--           Tolerance: $0.01 (rounding)
-- Severity: HIGH (financial)
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-004'            AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    TRY_CAST(labor_cost AS DECIMAL(12,2))   AS labor_cost,
    TRY_CAST(parts_cost AS DECIMAL(12,2))   AS parts_cost,
    TRY_CAST(labor_cost AS DECIMAL(12,2)) + TRY_CAST(parts_cost AS DECIMAL(12,2)) AS expected_total,
    TRY_CAST(total_cost AS DECIMAL(12,2))   AS actual_total,
    ABS(
        (TRY_CAST(labor_cost AS DECIMAL(12,2)) + TRY_CAST(parts_cost AS DECIMAL(12,2)))
        - TRY_CAST(total_cost AS DECIMAL(12,2))
    )                   AS discrepancy_amount
FROM raw.work_orders
WHERE TRY_CAST(labor_cost AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(parts_cost AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(total_cost AS DECIMAL(12,2)) IS NOT NULL
  AND ABS(
        (TRY_CAST(labor_cost AS DECIMAL(12,2)) + TRY_CAST(parts_cost AS DECIMAL(12,2)))
        - TRY_CAST(total_cost AS DECIMAL(12,2))
      ) > 0.01;

-- ── CHECK CS-005 ─────────────────────────────────────────────
-- What:     Invoice subtotal + tax - discount does not equal total_amount
-- Severity: CRITICAL (financial)
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-005'            AS rule_code,
    'CRITICAL'          AS severity,
    invoice_id,
    invoice_number,
    TRY_CAST(subtotal AS DECIMAL(12,2))         AS subtotal,
    TRY_CAST(tax_amount AS DECIMAL(12,2))        AS tax_amount,
    TRY_CAST(discount_amount AS DECIMAL(12,2))   AS discount_amount,
    TRY_CAST(subtotal AS DECIMAL(12,2)) + TRY_CAST(tax_amount AS DECIMAL(12,2))
        - TRY_CAST(discount_amount AS DECIMAL(12,2)) AS expected_total,
    TRY_CAST(total_amount AS DECIMAL(12,2))      AS recorded_total,
    ABS(
        (TRY_CAST(subtotal AS DECIMAL(12,2)) + TRY_CAST(tax_amount AS DECIMAL(12,2))
        - TRY_CAST(discount_amount AS DECIMAL(12,2)))
        - TRY_CAST(total_amount AS DECIMAL(12,2))
    )                   AS discrepancy
FROM raw.invoices
WHERE TRY_CAST(subtotal AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(tax_amount AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(discount_amount AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(total_amount AS DECIMAL(12,2)) IS NOT NULL
  AND ABS(
        (TRY_CAST(subtotal AS DECIMAL(12,2)) + TRY_CAST(tax_amount AS DECIMAL(12,2))
         - TRY_CAST(discount_amount AS DECIMAL(12,2)))
        - TRY_CAST(total_amount AS DECIMAL(12,2))
      ) > 0.01;

-- ── CHECK CS-006 ─────────────────────────────────────────────
-- What:     SLA_MET = 'Y' but actual completion was after SLA due date
--           (contradictory values — data entry or sync error)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-006'            AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    sla_due_date,
    actual_end,
    sla_met,
    'SLA flagged as MET but completed after due date' AS violation_detail
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(sla_met))) = 'Y'
  AND TRY_CAST(actual_end AS DATETIME2) IS NOT NULL
  AND TRY_CAST(sla_due_date AS DATETIME2) IS NOT NULL
  AND TRY_CAST(actual_end AS DATETIME2) > TRY_CAST(sla_due_date AS DATETIME2);

-- ── CHECK CS-007 ─────────────────────────────────────────────
-- What:     Terminated technician assigned to a non-completed work order
-- Severity: HIGH (business rule violation)
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-007'            AS rule_code,
    'HIGH'              AS severity,
    a.assignment_id,
    t.technician_id,
    t.full_name,
    t.employment_status,
    a.work_order_id,
    wo.status           AS wo_status,
    'Terminated tech assigned to open WO' AS violation_detail
FROM raw.work_order_assignments a
JOIN raw.technicians t ON a.technician_id = t.technician_id
JOIN raw.work_orders wo ON a.work_order_id = wo.work_order_id
WHERE UPPER(t.employment_status) = 'TERMINATED'
  AND UPPER(LTRIM(RTRIM(wo.status))) NOT IN ('COMPLETED', 'COMPLETE', 'CANCELLED');

-- ── CHECK CS-008 ─────────────────────────────────────────────
-- What:     Payment received exceeds invoice total (overpayment not flagged)
-- Severity: MEDIUM
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-008'            AS rule_code,
    'MEDIUM'            AS severity,
    invoice_id,
    invoice_number,
    TRY_CAST(total_amount AS DECIMAL(12,2))     AS invoice_total,
    TRY_CAST(payment_received AS DECIMAL(12,2)) AS payment_received,
    TRY_CAST(payment_received AS DECIMAL(12,2)) - TRY_CAST(total_amount AS DECIMAL(12,2)) AS overpayment
FROM raw.invoices
WHERE TRY_CAST(payment_received AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(total_amount AS DECIMAL(12,2)) IS NOT NULL
  AND TRY_CAST(payment_received AS DECIMAL(12,2)) > TRY_CAST(total_amount AS DECIMAL(12,2));

-- ── CHECK CS-009 ─────────────────────────────────────────────
-- What:     Invoice due_date is before invoice_date (impossible)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-009'            AS rule_code,
    'HIGH'              AS severity,
    invoice_id,
    invoice_number,
    invoice_date,
    due_date,
    'due_date is before invoice_date' AS violation_detail
FROM raw.invoices
WHERE TRY_CAST(invoice_date AS DATE) IS NOT NULL
  AND TRY_CAST(due_date AS DATE) IS NOT NULL
  AND TRY_CAST(due_date AS DATE) < TRY_CAST(invoice_date AS DATE);

-- ── CHECK CS-010 ─────────────────────────────────────────────
-- What:     actual_hours > 24 for a single-day work order
--           (logically impossible for one technician on one calendar day)
-- Severity: MEDIUM
-- ─────────────────────────────────────────────────────────────
SELECT
    'CS-010'            AS rule_code,
    'MEDIUM'            AS severity,
    work_order_id,
    work_order_number,
    actual_start,
    actual_end,
    actual_hours,
    'actual_hours exceeds maximum for single-day WO' AS violation_detail
FROM raw.work_orders
WHERE TRY_CAST(actual_hours AS DECIMAL(6,2)) > 24
  AND TRY_CAST(actual_start AS DATETIME2) IS NOT NULL
  AND TRY_CAST(actual_end AS DATETIME2) IS NOT NULL
  AND DATEDIFF(DAY, TRY_CAST(actual_start AS DATETIME2), TRY_CAST(actual_end AS DATETIME2)) = 0;


-- ============================================================
-- DIMENSION 6: TIMELINESS / FRESHNESS
-- Checks for stale data, future dates, and extraction lag
-- ============================================================

-- ── CHECK T-001 ──────────────────────────────────────────────
-- What:     Customer created_date is in the future
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'T-001'             AS rule_code,
    'HIGH'              AS severity,
    customer_id,
    customer_name,
    created_date,
    'created_date is in the future' AS violation_detail
FROM raw.customers
WHERE TRY_CAST(created_date AS DATE) IS NOT NULL
  AND TRY_CAST(created_date AS DATE) > CAST(GETDATE() AS DATE);

-- ── CHECK T-002 ──────────────────────────────────────────────
-- What:     Technician hire_date is in the future
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'T-002'             AS rule_code,
    'HIGH'              AS severity,
    technician_id,
    full_name,
    hire_date,
    'hire_date is in the future' AS violation_detail
FROM raw.technicians
WHERE TRY_CAST(hire_date AS DATE) IS NOT NULL
  AND TRY_CAST(hire_date AS DATE) > CAST(GETDATE() AS DATE);

-- ── CHECK T-003 ──────────────────────────────────────────────
-- What:     Work orders with COMPLETED status and future scheduled_date
-- Severity: CRITICAL (impossible business state)
-- ─────────────────────────────────────────────────────────────
SELECT
    'T-003'             AS rule_code,
    'CRITICAL'          AS severity,
    work_order_id,
    work_order_number,
    status,
    scheduled_date,
    actual_end,
    'Completed WO has future scheduled date' AS violation_detail
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(status))) IN ('COMPLETED', 'COMPLETE')
  AND TRY_CAST(scheduled_date AS DATE) IS NOT NULL
  AND TRY_CAST(scheduled_date AS DATE) > CAST(GETDATE() AS DATE);

-- ── CHECK T-004 ──────────────────────────────────────────────
-- What:     Customers with last_modified more than 2 years ago
--           (possible stale / abandoned records)
-- Severity: LOW (data freshness concern)
-- ─────────────────────────────────────────────────────────────
SELECT
    'T-004'             AS rule_code,
    'LOW'               AS severity,
    customer_id,
    customer_name,
    account_status,
    last_modified,
    DATEDIFF(DAY, TRY_CAST(last_modified AS DATETIME2), GETDATE()) AS days_since_modified,
    'Customer record not updated in 2+ years' AS violation_detail
FROM raw.customers
WHERE TRY_CAST(last_modified AS DATETIME2) IS NOT NULL
  AND DATEDIFF(DAY, TRY_CAST(last_modified AS DATETIME2), GETDATE()) > 730
  AND UPPER(LTRIM(RTRIM(account_status))) = 'ACTIVE';

-- ── CHECK T-005 ──────────────────────────────────────────────
-- What:     Work orders with IN_PROGRESS status and last_modified 
--           more than 90 days ago (stale open WO — possible zombie records)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'T-005'             AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    status,
    scheduled_date,
    last_modified,
    DATEDIFF(DAY, TRY_CAST(last_modified AS DATETIME2), GETDATE()) AS days_open,
    'IN_PROGRESS WO stale for 90+ days' AS violation_detail
FROM raw.work_orders
WHERE UPPER(LTRIM(RTRIM(REPLACE(status, ' ', '_')))) = 'IN_PROGRESS'
  AND TRY_CAST(last_modified AS DATETIME2) IS NOT NULL
  AND DATEDIFF(DAY, TRY_CAST(last_modified AS DATETIME2), GETDATE()) > 90;

-- ── CHECK T-006 ──────────────────────────────────────────────
-- What:     created_date value is unparseable (e.g. 'UNKNOWN', 'N/A')
-- Severity: MEDIUM
-- ─────────────────────────────────────────────────────────────
SELECT
    'T-006'             AS rule_code,
    'MEDIUM'            AS severity,
    customer_id,
    customer_name,
    created_date,
    'created_date cannot be parsed as a date' AS violation_detail
FROM raw.customers
WHERE created_date IS NOT NULL
  AND TRY_CAST(created_date AS DATE) IS NULL;


-- ============================================================
-- DIMENSION 7: BUSINESS RULE VALIDATION
-- Complex cross-entity checks unique to FieldOps domain
-- ============================================================

-- ── CHECK BR-001 ─────────────────────────────────────────────
-- What:     COMPLETED work orders with no assignment record
--           (Every completed WO must have at least one technician)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'BR-001'            AS rule_code,
    'HIGH'              AS severity,
    wo.work_order_id,
    wo.work_order_number,
    wo.status,
    wo.customer_id,
    'Completed WO has no technician assignment' AS violation_detail
FROM raw.work_orders wo
LEFT JOIN raw.work_order_assignments a ON wo.work_order_id = a.work_order_id
WHERE UPPER(LTRIM(RTRIM(wo.status))) IN ('COMPLETED', 'COMPLETE')
  AND a.assignment_id IS NULL;

-- ── CHECK BR-002 ─────────────────────────────────────────────
-- What:     COMPLETED work orders with no corresponding invoice
--           (Revenue leakage risk)
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'BR-002'            AS rule_code,
    'HIGH'              AS severity,
    wo.work_order_id,
    wo.work_order_number,
    wo.status,
    wo.customer_id,
    wo.total_cost,
    'Completed WO has no invoice — possible revenue leakage' AS violation_detail
FROM raw.work_orders wo
LEFT JOIN raw.invoices i ON wo.work_order_id = i.work_order_id
WHERE UPPER(LTRIM(RTRIM(wo.status))) IN ('COMPLETED', 'COMPLETE')
  AND i.invoice_id IS NULL
  AND TRY_CAST(wo.total_cost AS DECIMAL(12,2)) > 0;

-- ── CHECK BR-003 ─────────────────────────────────────────────
-- What:     SLA compliance verification
--           Where actual completion is available, validate sla_met flag
-- Severity: HIGH
-- ─────────────────────────────────────────────────────────────
SELECT
    'BR-003'            AS rule_code,
    'HIGH'              AS severity,
    work_order_id,
    work_order_number,
    priority,
    sla_due_date,
    actual_end,
    sla_met             AS recorded_sla_flag,
    CASE
        WHEN TRY_CAST(actual_end AS DATETIME2) <= TRY_CAST(sla_due_date AS DATETIME2) THEN 'Y'
        ELSE 'N'
    END                 AS calculated_sla_flag,
    CASE
        WHEN UPPER(LTRIM(RTRIM(sla_met))) <>
             CASE WHEN TRY_CAST(actual_end AS DATETIME2) <= TRY_CAST(sla_due_date AS DATETIME2) THEN 'Y' ELSE 'N' END
        THEN 'MISMATCH'
        ELSE 'OK'
    END                 AS sla_flag_status
FROM raw.work_orders
WHERE TRY_CAST(actual_end AS DATETIME2) IS NOT NULL
  AND TRY_CAST(sla_due_date AS DATETIME2) IS NOT NULL
  AND sla_met IS NOT NULL;


-- ============================================================
-- AGGREGATE DQ SCORE SUMMARY
-- Produces an overall quality score per table
-- ============================================================

WITH rule_results AS (
    -- Completeness checks (counts against total)
    SELECT 'raw.customers' AS tbl, 'COMPLETENESS' AS dim, 'CRITICAL' AS sev,
           COUNT(*) AS total,
           SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS failed
    FROM raw.customers
    UNION ALL
    SELECT 'raw.customers', 'COMPLETENESS', 'HIGH',
           COUNT(*),
           SUM(CASE WHEN account_status IS NULL THEN 1 ELSE 0 END)
    FROM raw.customers
    UNION ALL
    -- Uniqueness checks
    SELECT 'raw.customers', 'UNIQUENESS', 'CRITICAL',
           COUNT(*),
           COUNT(*) - COUNT(DISTINCT customer_id)
    FROM raw.customers
    UNION ALL
    SELECT 'raw.work_orders', 'UNIQUENESS', 'CRITICAL',
           COUNT(*),
           COUNT(*) - COUNT(DISTINCT work_order_id)
    FROM raw.work_orders
    UNION ALL
    -- Validity checks
    SELECT 'raw.customers', 'VALIDITY', 'CRITICAL',
           COUNT(*),
           SUM(CASE WHEN UPPER(LTRIM(RTRIM(account_status))) NOT IN ('ACTIVE','INACTIVE','SUSPENDED','PENDING') OR account_status IS NULL THEN 1 ELSE 0 END)
    FROM raw.customers
    UNION ALL
    SELECT 'raw.work_orders', 'VALIDITY', 'HIGH',
           COUNT(*),
           SUM(CASE WHEN TRY_CAST(labor_cost AS DECIMAL(12,2)) < 0 OR TRY_CAST(parts_cost AS DECIMAL(12,2)) < 0 THEN 1 ELSE 0 END)
    FROM raw.work_orders
)
SELECT
    tbl                 AS target_table,
    dim                 AS dq_dimension,
    sev                 AS severity,
    SUM(total)          AS total_records_checked,
    SUM(failed)         AS total_failures,
    CAST(100.0 * (1 - CAST(SUM(failed) AS FLOAT) / NULLIF(SUM(total), 0)) AS DECIMAL(5,2)) AS pass_rate_pct
FROM rule_results
GROUP BY tbl, dim, sev
ORDER BY tbl, dim,
    CASE sev WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END;
GO