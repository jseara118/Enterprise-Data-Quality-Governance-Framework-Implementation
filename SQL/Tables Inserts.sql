-- ============================================================
-- DATABASE SETUP
-- FieldOps Pro Data Quality Portfolio Project
-- ============================================================

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'FieldOpsDQ')
BEGIN
    CREATE DATABASE FieldOpsDQ;
END
GO

USE FieldOpsDQ;
GO

-- Create schemas
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
    EXEC('CREATE SCHEMA raw');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'curated')
    EXEC('CREATE SCHEMA curated');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dq')
    EXEC('CREATE SCHEMA dq');
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
    EXEC('CREATE SCHEMA audit');
GO

-- ============================================================
-- RAW LAYER — Exact source extract, no constraints enforced
-- Note: Intentionally loose typing to accept dirty data
-- ============================================================

DROP TABLE IF EXISTS raw.work_order_parts;
DROP TABLE IF EXISTS raw.invoice_line_items;
DROP TABLE IF EXISTS raw.invoices;
DROP TABLE IF EXISTS raw.work_order_assignments;
DROP TABLE IF EXISTS raw.work_orders;
DROP TABLE IF EXISTS raw.technician_certifications;
DROP TABLE IF EXISTS raw.technicians;
DROP TABLE IF EXISTS raw.sites;
DROP TABLE IF EXISTS raw.customers;
GO

-- ------------------------------
-- raw.customers
-- ------------------------------
CREATE TABLE raw.customers (
    customer_id         VARCHAR(50)     NULL,
    customer_name       NVARCHAR(500)   NULL,
    customer_type       VARCHAR(50)     NULL,
    account_status      VARCHAR(50)     NULL,
    credit_limit        VARCHAR(50)     NULL,   -- stored as varchar to capture dirty data
    primary_contact     NVARCHAR(200)   NULL,
    email_address       NVARCHAR(300)   NULL,
    phone_number        VARCHAR(50)     NULL,
    billing_address     NVARCHAR(500)   NULL,
    billing_city        NVARCHAR(100)   NULL,
    billing_state       VARCHAR(10)     NULL,
    billing_zip         VARCHAR(20)     NULL,
    region_code         VARCHAR(20)     NULL,
    created_date        VARCHAR(50)     NULL,   -- varchar to capture malformed dates
    last_modified       VARCHAR(50)     NULL,
    created_by          VARCHAR(100)    NULL,
    modified_by         VARCHAR(100)    NULL,
    source_system       VARCHAR(50)     NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.sites
-- ------------------------------
CREATE TABLE raw.sites (
    site_id             VARCHAR(50)     NULL,
    customer_id         VARCHAR(50)     NULL,
    site_name           NVARCHAR(300)   NULL,
    site_type           VARCHAR(50)     NULL,
    service_address     NVARCHAR(500)   NULL,
    city                NVARCHAR(100)   NULL,
    state               VARCHAR(10)     NULL,
    zip_code            VARCHAR(20)     NULL,
    latitude            VARCHAR(30)     NULL,
    longitude           VARCHAR(30)     NULL,
    site_status         VARCHAR(50)     NULL,
    priority_tier       VARCHAR(20)     NULL,
    territory_code      VARCHAR(20)     NULL,
    created_date        VARCHAR(50)     NULL,
    last_modified       VARCHAR(50)     NULL,
    created_by          VARCHAR(100)    NULL,
    modified_by         VARCHAR(100)    NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.technicians
-- ------------------------------
CREATE TABLE raw.technicians (
    technician_id       VARCHAR(50)     NULL,
    employee_number     VARCHAR(50)     NULL,
    first_name          NVARCHAR(100)   NULL,
    last_name           NVARCHAR(100)   NULL,
    full_name           NVARCHAR(200)   NULL,
    employment_status   VARCHAR(50)     NULL,
    skill_level         VARCHAR(50)     NULL,
    home_region         VARCHAR(20)     NULL,
    hire_date           VARCHAR(50)     NULL,
    termination_date    VARCHAR(50)     NULL,
    hourly_rate         VARCHAR(50)     NULL,
    manager_id          VARCHAR(50)     NULL,
    email               NVARCHAR(300)   NULL,
    mobile_phone        VARCHAR(50)     NULL,
    created_date        VARCHAR(50)     NULL,
    last_modified       VARCHAR(50)     NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.technician_certifications
-- ------------------------------
CREATE TABLE raw.technician_certifications (
    cert_id             VARCHAR(50)     NULL,
    technician_id       VARCHAR(50)     NULL,
    certification_code  VARCHAR(50)     NULL,
    certification_name  NVARCHAR(200)   NULL,
    issued_date         VARCHAR(50)     NULL,
    expiry_date         VARCHAR(50)     NULL,
    issuing_body        NVARCHAR(200)   NULL,
    cert_status         VARCHAR(50)     NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.work_orders
-- ------------------------------
CREATE TABLE raw.work_orders (
    work_order_id       VARCHAR(50)     NULL,
    work_order_number   VARCHAR(50)     NULL,
    site_id             VARCHAR(50)     NULL,
    customer_id         VARCHAR(50)     NULL,
    work_order_type     VARCHAR(50)     NULL,
    priority            VARCHAR(20)     NULL,
    status              VARCHAR(50)     NULL,
    description         NVARCHAR(1000)  NULL,
    scheduled_date      VARCHAR(50)     NULL,
    scheduled_start     VARCHAR(50)     NULL,
    scheduled_end       VARCHAR(50)     NULL,
    actual_start        VARCHAR(50)     NULL,
    actual_end          VARCHAR(50)     NULL,
    estimated_hours     VARCHAR(20)     NULL,
    actual_hours        VARCHAR(20)     NULL,
    labor_cost          VARCHAR(30)     NULL,
    parts_cost          VARCHAR(30)     NULL,
    total_cost          VARCHAR(30)     NULL,
    sla_due_date        VARCHAR(50)     NULL,
    sla_met             VARCHAR(10)     NULL,
    resolution_code     VARCHAR(50)     NULL,
    notes               NVARCHAR(2000)  NULL,
    created_date        VARCHAR(50)     NULL,
    last_modified       VARCHAR(50)     NULL,
    created_by          VARCHAR(100)    NULL,
    modified_by         VARCHAR(100)    NULL,
    source_system       VARCHAR(50)     NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.work_order_assignments
-- ------------------------------
CREATE TABLE raw.work_order_assignments (
    assignment_id       VARCHAR(50)     NULL,
    work_order_id       VARCHAR(50)     NULL,
    technician_id       VARCHAR(50)     NULL,
    assignment_role     VARCHAR(50)     NULL,
    assigned_date       VARCHAR(50)     NULL,
    start_time          VARCHAR(50)     NULL,
    end_time            VARCHAR(50)     NULL,
    hours_logged        VARCHAR(20)     NULL,
    assignment_status   VARCHAR(50)     NULL,
    notes               NVARCHAR(500)   NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.work_order_parts
-- ------------------------------
CREATE TABLE raw.work_order_parts (
    part_usage_id       VARCHAR(50)     NULL,
    work_order_id       VARCHAR(50)     NULL,
    part_number         VARCHAR(50)     NULL,
    part_description    NVARCHAR(300)   NULL,
    quantity_used       VARCHAR(20)     NULL,
    unit_cost           VARCHAR(30)     NULL,
    total_cost          VARCHAR(30)     NULL,
    warehouse_code      VARCHAR(20)     NULL,
    recorded_by         VARCHAR(100)    NULL,
    recorded_date       VARCHAR(50)     NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.invoices
-- ------------------------------
CREATE TABLE raw.invoices (
    invoice_id          VARCHAR(50)     NULL,
    invoice_number      VARCHAR(50)     NULL,
    work_order_id       VARCHAR(50)     NULL,
    customer_id         VARCHAR(50)     NULL,
    invoice_date        VARCHAR(50)     NULL,
    due_date            VARCHAR(50)     NULL,
    invoice_status      VARCHAR(50)     NULL,
    subtotal            VARCHAR(30)     NULL,
    tax_amount          VARCHAR(30)     NULL,
    discount_amount     VARCHAR(30)     NULL,
    total_amount        VARCHAR(30)     NULL,
    payment_terms       VARCHAR(50)     NULL,
    payment_received    VARCHAR(30)     NULL,
    payment_date        VARCHAR(50)     NULL,
    created_date        VARCHAR(50)     NULL,
    last_modified       VARCHAR(50)     NULL,
    created_by          VARCHAR(100)    NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ------------------------------
-- raw.invoice_line_items
-- ------------------------------
CREATE TABLE raw.invoice_line_items (
    line_item_id        VARCHAR(50)     NULL,
    invoice_id          VARCHAR(50)     NULL,
    line_type           VARCHAR(50)     NULL,
    description         NVARCHAR(300)   NULL,
    quantity            VARCHAR(20)     NULL,
    unit_price          VARCHAR(30)     NULL,
    line_total          VARCHAR(30)     NULL,
    tax_code            VARCHAR(20)     NULL,
    _extract_timestamp  DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    _batch_id           VARCHAR(50)     NULL
);

-- ============================================================
-- DQ SCHEMA — Data Quality support tables
-- ============================================================

-- DQ Rule Registry
CREATE TABLE dq.dq_rules (
    rule_id             INT IDENTITY(1,1) PRIMARY KEY,
    rule_code           VARCHAR(50)     NOT NULL UNIQUE,
    rule_name           VARCHAR(200)    NOT NULL,
    dq_dimension        VARCHAR(50)     NOT NULL,   -- Completeness, Uniqueness, Validity, etc.
    target_table        VARCHAR(100)    NOT NULL,
    target_column       VARCHAR(100)    NULL,
    severity            VARCHAR(20)     NOT NULL,   -- CRITICAL, HIGH, MEDIUM, LOW
    rule_description    NVARCHAR(500)   NULL,
    sql_check           NVARCHAR(MAX)   NULL,
    is_active           BIT             NOT NULL DEFAULT 1,
    created_date        DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    created_by          VARCHAR(100)    NULL
);

-- DQ Rule Execution Results
CREATE TABLE dq.dq_results (
    result_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id              VARCHAR(50)     NOT NULL,
    rule_id             INT             NOT NULL,
    rule_code           VARCHAR(50)     NOT NULL,
    execution_time      DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    total_records       INT             NULL,
    failed_records      INT             NULL,
    pass_rate           DECIMAL(5,2)    NULL,
    status              VARCHAR(20)     NULL,   -- PASS, FAIL, WARNING
    details             NVARCHAR(1000)  NULL
);

-- DQ Quarantine Log
CREATE TABLE dq.quarantine_log (
    quarantine_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id              VARCHAR(50)     NOT NULL,
    source_table        VARCHAR(100)    NOT NULL,
    source_key          VARCHAR(100)    NULL,
    rule_code           VARCHAR(50)     NOT NULL,
    severity            VARCHAR(20)     NOT NULL,
    quarantine_reason   NVARCHAR(500)   NULL,
    raw_data            NVARCHAR(MAX)   NULL,
    quarantine_date     DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    resolved_flag       BIT             NOT NULL DEFAULT 0,
    resolved_date       DATETIME2       NULL,
    resolved_by         VARCHAR(100)    NULL
);

-- DQ Score Summary (per table, per run)
CREATE TABLE dq.dq_score_summary (
    score_id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id              VARCHAR(50)     NOT NULL,
    score_date          DATE            NOT NULL,
    target_table        VARCHAR(100)    NOT NULL,
    total_rules_run     INT             NULL,
    rules_passed        INT             NULL,
    rules_failed        INT             NULL,
    critical_failures   INT             NULL,
    high_failures       INT             NULL,
    overall_score       DECIMAL(5,2)    NULL,   -- 0-100
    completeness_score  DECIMAL(5,2)    NULL,
    uniqueness_score    DECIMAL(5,2)    NULL,
    validity_score      DECIMAL(5,2)    NULL,
    consistency_score   DECIMAL(5,2)    NULL,
    timeliness_score    DECIMAL(5,2)    NULL
);

-- Pipeline Run Log
CREATE TABLE audit.pipeline_runs (
    run_id              VARCHAR(50)     NOT NULL PRIMARY KEY,
    pipeline_name       VARCHAR(100)    NOT NULL,
    run_start           DATETIME2       NOT NULL,
    run_end             DATETIME2       NULL,
    status              VARCHAR(20)     NULL,   -- RUNNING, COMPLETED, FAILED
    records_extracted   INT             NULL,
    records_staged      INT             NULL,
    records_quarantined INT             NULL,
    records_curated     INT             NULL,
    error_message       NVARCHAR(MAX)   NULL,
    triggered_by        VARCHAR(100)    NULL
);
GO
GO


-- ============================================================
-- STAGING LAYER — Validated, conformed, quarantine-flagged
-- ============================================================

CREATE TABLE stg.customers (
    stg_customer_sk     BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id         VARCHAR(50)     NOT NULL,
    customer_name       NVARCHAR(300)   NOT NULL,
    customer_type       VARCHAR(50)     NULL,
    account_status      VARCHAR(20)     NULL,
    credit_limit        DECIMAL(12,2)   NULL,
    primary_contact     NVARCHAR(200)   NULL,
    email_address       NVARCHAR(300)   NULL,
    phone_number        VARCHAR(20)     NULL,
    billing_city        NVARCHAR(100)   NULL,
    billing_state       CHAR(2)         NULL,
    billing_zip         VARCHAR(10)     NULL,
    region_code         VARCHAR(20)     NULL,
    created_date        DATE            NULL,
    last_modified       DATETIME2       NULL,
    -- Governance columns
    is_quarantined      BIT             NOT NULL DEFAULT 0,
    quarantine_reason   NVARCHAR(500)   NULL,
    dq_score            DECIMAL(5,2)    NULL,
    source_system       VARCHAR(50)     NULL,
    stg_load_date       DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    stg_batch_id        VARCHAR(50)     NULL,
    is_duplicate        BIT             NOT NULL DEFAULT 0,
    duplicate_of_key    VARCHAR(50)     NULL
);

CREATE TABLE stg.work_orders (
    stg_wo_sk           BIGINT IDENTITY(1,1) PRIMARY KEY,
    work_order_id       VARCHAR(50)     NOT NULL,
    work_order_number   VARCHAR(50)     NULL,
    site_id             VARCHAR(50)     NULL,
    customer_id         VARCHAR(50)     NULL,
    work_order_type     VARCHAR(50)     NULL,
    priority            VARCHAR(20)     NULL,
    status              VARCHAR(30)     NULL,
    description         NVARCHAR(1000)  NULL,
    scheduled_date      DATE            NULL,
    actual_start        DATETIME2       NULL,
    actual_end          DATETIME2       NULL,
    estimated_hours     DECIMAL(6,2)    NULL,
    actual_hours        DECIMAL(6,2)    NULL,
    labor_cost          DECIMAL(12,2)   NULL,
    parts_cost          DECIMAL(12,2)   NULL,
    total_cost          DECIMAL(12,2)   NULL,
    sla_due_date        DATETIME2       NULL,
    sla_met             BIT             NULL,
    resolution_code     VARCHAR(50)     NULL,
    created_date        DATE            NULL,
    last_modified       DATETIME2       NULL,
    -- Governance columns
    is_quarantined      BIT             NOT NULL DEFAULT 0,
    quarantine_reason   NVARCHAR(500)   NULL,
    dq_score            DECIMAL(5,2)    NULL,
    ref_integrity_ok    BIT             NULL,
    stg_load_date       DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    stg_batch_id        VARCHAR(50)     NULL
);
GO


-- ============================================================
-- CURATED LAYER — Analytics-ready dimensions and facts
-- ============================================================

-- Dimension: Date
CREATE TABLE curated.dim_date (
    date_sk             INT             NOT NULL PRIMARY KEY,
    full_date           DATE            NOT NULL,
    year_num            SMALLINT        NOT NULL,
    quarter_num         TINYINT         NOT NULL,
    month_num           TINYINT         NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    week_num            TINYINT         NOT NULL,
    day_of_week         TINYINT         NOT NULL,
    day_name            VARCHAR(20)     NOT NULL,
    is_weekend          BIT             NOT NULL,
    is_holiday          BIT             NOT NULL DEFAULT 0,
    fiscal_year         SMALLINT        NULL,
    fiscal_quarter      TINYINT         NULL
);

-- Dimension: Customer
CREATE TABLE curated.dim_customer (
    customer_sk         BIGINT IDENTITY(1,1) PRIMARY KEY,
    customer_id         VARCHAR(50)     NOT NULL,
    customer_name       NVARCHAR(300)   NOT NULL,
    customer_type       VARCHAR(50)     NULL,
    account_status      VARCHAR(20)     NULL,
    region_code         VARCHAR(20)     NULL,
    credit_limit        DECIMAL(12,2)   NULL,
    -- SCD Type 2 columns
    effective_from      DATE            NOT NULL,
    effective_to        DATE            NULL,
    is_current          BIT             NOT NULL DEFAULT 1,
    -- Governance
    record_source       VARCHAR(50)     NULL,
    load_date           DATETIME2       NOT NULL DEFAULT SYSDATETIME()
);

-- Dimension: Technician
CREATE TABLE curated.dim_technician (
    technician_sk       BIGINT IDENTITY(1,1) PRIMARY KEY,
    technician_id       VARCHAR(50)     NOT NULL,
    employee_number     VARCHAR(50)     NULL,
    full_name           NVARCHAR(200)   NOT NULL,
    employment_status   VARCHAR(30)     NULL,
    skill_level         VARCHAR(30)     NULL,
    home_region         VARCHAR(20)     NULL,
    hire_date           DATE            NULL,
    effective_from      DATE            NOT NULL,
    effective_to        DATE            NULL,
    is_current          BIT             NOT NULL DEFAULT 1,
    load_date           DATETIME2       NOT NULL DEFAULT SYSDATETIME()
);

-- Fact: Work Orders
CREATE TABLE curated.fact_work_orders (
    fact_wo_sk          BIGINT IDENTITY(1,1) PRIMARY KEY,
    work_order_id       VARCHAR(50)     NOT NULL,
    work_order_number   VARCHAR(50)     NULL,
    customer_sk         BIGINT          NULL,
    scheduled_date_sk   INT             NULL,
    completed_date_sk   INT             NULL,
    work_order_type     VARCHAR(50)     NULL,
    priority            VARCHAR(20)     NULL,
    status              VARCHAR(30)     NULL,
    estimated_hours     DECIMAL(6,2)    NULL,
    actual_hours        DECIMAL(6,2)    NULL,
    labor_cost          DECIMAL(12,2)   NULL,
    parts_cost          DECIMAL(12,2)   NULL,
    total_cost          DECIMAL(12,2)   NULL,
    sla_met             BIT             NULL,
    hours_variance      AS (actual_hours - estimated_hours),  -- computed
    -- Governance
    load_date           DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    record_source       VARCHAR(50)     NULL,
    CONSTRAINT fk_fwo_customer  FOREIGN KEY (customer_sk)        REFERENCES curated.dim_customer(customer_sk),
    CONSTRAINT fk_fwo_schd_date FOREIGN KEY (scheduled_date_sk)  REFERENCES curated.dim_date(date_sk),
    CONSTRAINT fk_fwo_comp_date FOREIGN KEY (completed_date_sk)  REFERENCES curated.dim_date(date_sk)
);

-- Fact: Invoices (financial grain)
CREATE TABLE curated.fact_invoices (
    fact_inv_sk         BIGINT IDENTITY(1,1) PRIMARY KEY,
    invoice_id          VARCHAR(50)     NOT NULL,
    invoice_number      VARCHAR(50)     NULL,
    work_order_id       VARCHAR(50)     NULL,
    customer_sk         BIGINT          NULL,
    invoice_date_sk     INT             NULL,
    due_date_sk         INT             NULL,
    invoice_status      VARCHAR(30)     NULL,
    subtotal            DECIMAL(12,2)   NULL,
    tax_amount          DECIMAL(12,2)   NULL,
    discount_amount     DECIMAL(12,2)   NULL,
    total_amount        DECIMAL(12,2)   NULL,
    payment_received    DECIMAL(12,2)   NULL,
    outstanding_amount  AS (total_amount - ISNULL(payment_received,0)),
    load_date           DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT fk_finv_customer  FOREIGN KEY (customer_sk)    REFERENCES curated.dim_customer(customer_sk),
    CONSTRAINT fk_finv_inv_date  FOREIGN KEY (invoice_date_sk) REFERENCES curated.dim_date(date_sk)
);
GO