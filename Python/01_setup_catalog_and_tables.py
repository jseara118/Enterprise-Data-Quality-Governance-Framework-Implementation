# Databricks Notebook — 01_SETUP_CATALOG_AND_TABLES
# FieldOps Pro | Data Quality & Governance Portfolio
# ─────────────────────────────────────────────────────────────
# Run this notebook first.
# Works on: Databricks Community Edition (free) OR paid trial.
# Community Edition note: replace catalog references with just
#   the schema name — Community Edition has no Unity Catalog.
#   Use the toggle COMMUNITY_EDITION = True below.
# ─────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Configuration

# COMMAND ----------

COMMUNITY_EDITION = False   # Set True if using free Community Edition

CATALOG  = "fieldops_dq"    # Unity Catalog name (paid only)
RAW      = "raw"
STAGING  = "stg"
CURATED  = "curated"
DQ       = "dq"
AUDIT    = "audit"

def full(schema, table=""):
    """Return fully-qualified table reference."""
    if COMMUNITY_EDITION:
        ref = f"{schema}"
    else:
        ref = f"{CATALOG}.{schema}"
    return ref if not table else f"{ref}.{table}"

print(f"Mode          : {'Community Edition' if COMMUNITY_EDITION else 'Unity Catalog'}")
print(f"Catalog       : {CATALOG if not COMMUNITY_EDITION else '(not used)'}")
print(f"Raw schema    : {full(RAW)}")
print(f"Staging schema: {full(STAGING)}")
print(f"Curated schema: {full(CURATED)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Create Catalog and Schemas

# COMMAND ----------

if not COMMUNITY_EDITION:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"USE CATALOG {CATALOG}")

for schema in [RAW, STAGING, CURATED, DQ, AUDIT]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full(schema)}")
    print(f"Schema ready: {full(schema)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — RAW Layer Tables
# MAGIC Exact source extract. All columns STRING to absorb dirty data without rejection.
# MAGIC Append-only. No constraints enforced.

# COMMAND ----------

# ── raw.customers ─────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'customers')} (
    customer_id         STRING,
    customer_name       STRING,
    customer_type       STRING,
    account_status      STRING,
    credit_limit        STRING,
    primary_contact     STRING,
    email_address       STRING,
    phone_number        STRING,
    billing_city        STRING,
    billing_state       STRING,
    billing_zip         STRING,
    region_code         STRING,
    created_date        STRING,
    last_modified       STRING,
    created_by          STRING,
    source_system       STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
COMMENT 'RAW customer records — exact source extract, no transformations'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality' = 'raw'
)
""")

# ── raw.sites ─────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'sites')} (
    site_id             STRING,
    customer_id         STRING,
    site_name           STRING,
    site_type           STRING,
    service_address     STRING,
    city                STRING,
    state               STRING,
    zip_code            STRING,
    latitude            STRING,
    longitude           STRING,
    site_status         STRING,
    priority_tier       STRING,
    territory_code      STRING,
    created_date        STRING,
    last_modified       STRING,
    created_by          STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
COMMENT 'RAW site/location records'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

# ── raw.technicians ───────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'technicians')} (
    technician_id       STRING,
    employee_number     STRING,
    first_name          STRING,
    last_name           STRING,
    full_name           STRING,
    employment_status   STRING,
    skill_level         STRING,
    home_region         STRING,
    hire_date           STRING,
    termination_date    STRING,
    hourly_rate         STRING,
    manager_id          STRING,
    email               STRING,
    mobile_phone        STRING,
    created_date        STRING,
    last_modified       STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
COMMENT 'RAW technician records'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

# ── raw.technician_certifications ─────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'technician_certifications')} (
    cert_id             STRING,
    technician_id       STRING,
    certification_code  STRING,
    certification_name  STRING,
    issued_date         STRING,
    expiry_date         STRING,
    issuing_body        STRING,
    cert_status         STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

# ── raw.work_orders ───────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'work_orders')} (
    work_order_id       STRING,
    work_order_number   STRING,
    site_id             STRING,
    customer_id         STRING,
    work_order_type     STRING,
    priority            STRING,
    status              STRING,
    description         STRING,
    scheduled_date      STRING,
    scheduled_start     STRING,
    scheduled_end       STRING,
    actual_start        STRING,
    actual_end          STRING,
    estimated_hours     STRING,
    actual_hours        STRING,
    labor_cost          STRING,
    parts_cost          STRING,
    total_cost          STRING,
    sla_due_date        STRING,
    sla_met             STRING,
    resolution_code     STRING,
    notes               STRING,
    created_date        STRING,
    last_modified       STRING,
    created_by          STRING,
    source_system       STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
COMMENT 'RAW work order records'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

# ── raw.work_order_assignments ────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'work_order_assignments')} (
    assignment_id       STRING,
    work_order_id       STRING,
    technician_id       STRING,
    assignment_role     STRING,
    assigned_date       STRING,
    start_time          STRING,
    end_time            STRING,
    hours_logged        STRING,
    assignment_status   STRING,
    notes               STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

# ── raw.work_order_parts ──────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'work_order_parts')} (
    part_usage_id       STRING,
    work_order_id       STRING,
    part_number         STRING,
    part_description    STRING,
    quantity_used       STRING,
    unit_cost           STRING,
    total_cost          STRING,
    warehouse_code      STRING,
    recorded_by         STRING,
    recorded_date       STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

# ── raw.invoices ──────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(RAW, 'invoices')} (
    invoice_id          STRING,
    invoice_number      STRING,
    work_order_id       STRING,
    customer_id         STRING,
    invoice_date        STRING,
    due_date            STRING,
    invoice_status      STRING,
    subtotal            STRING,
    tax_amount          STRING,
    discount_amount     STRING,
    total_amount        STRING,
    payment_terms       STRING,
    payment_received    STRING,
    payment_date        STRING,
    created_date        STRING,
    last_modified       STRING,
    created_by          STRING,
    _extract_timestamp  TIMESTAMP,
    _batch_id           STRING
)
USING DELTA
COMMENT 'RAW invoice records'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'quality' = 'raw')
""")

print("All RAW tables created.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — DQ Governance Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(DQ, 'dq_rules')} (
    rule_id             INT,
    rule_code           STRING NOT NULL,
    rule_name           STRING NOT NULL,
    dq_dimension        STRING NOT NULL,
    target_table        STRING NOT NULL,
    target_column       STRING,
    severity            STRING NOT NULL,
    rule_description    STRING,
    is_active           BOOLEAN,
    created_date        TIMESTAMP,
    created_by          STRING
)
USING DELTA
COMMENT 'Registry of all DQ rules — governance ownership model'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(DQ, 'dq_results')} (
    result_id           BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id              STRING NOT NULL,
    rule_code           STRING NOT NULL,
    dq_dimension        STRING,
    target_table        STRING,
    severity            STRING,
    execution_time      TIMESTAMP,
    total_records       BIGINT,
    failed_records      BIGINT,
    pass_rate           DOUBLE,
    status              STRING,
    details             STRING
)
USING DELTA
COMMENT 'DQ rule execution results per pipeline run'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(DQ, 'quarantine_log')} (
    quarantine_id       BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id              STRING NOT NULL,
    source_table        STRING NOT NULL,
    source_key          STRING,
    rule_code           STRING NOT NULL,
    severity            STRING NOT NULL,
    quarantine_reason   STRING,
    raw_record          STRING,
    quarantine_date     TIMESTAMP,
    resolved_flag       BOOLEAN,
    resolved_date       TIMESTAMP,
    resolved_by         STRING
)
USING DELTA
COMMENT 'Quarantine log — rejected records pending governance review'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(DQ, 'dq_score_summary')} (
    score_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id              STRING NOT NULL,
    score_date          DATE NOT NULL,
    target_table        STRING NOT NULL,
    total_rules_run     INT,
    rules_passed        INT,
    rules_failed        INT,
    critical_failures   INT,
    high_failures       INT,
    overall_score       DOUBLE,
    completeness_score  DOUBLE,
    uniqueness_score    DOUBLE,
    validity_score      DOUBLE,
    consistency_score   DOUBLE,
    timeliness_score    DOUBLE,
    ref_integrity_score DOUBLE
)
USING DELTA
COMMENT 'DQ score summary per table per run — feeds governance dashboard'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(AUDIT, 'pipeline_runs')} (
    run_id              STRING NOT NULL,
    pipeline_name       STRING NOT NULL,
    run_start           TIMESTAMP NOT NULL,
    run_end             TIMESTAMP,
    status              STRING,
    records_extracted   BIGINT,
    records_staged      BIGINT,
    records_quarantined BIGINT,
    records_curated     BIGINT,
    dq_score            DOUBLE,
    error_message       STRING,
    triggered_by        STRING,
    notebook_url        STRING
)
USING DELTA
COMMENT 'Pipeline run audit log'
""")

print("DQ and Audit tables created.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 — Staging Layer Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(STAGING, 'customers')} (
    customer_id         STRING NOT NULL,
    customer_name       STRING,
    customer_type       STRING,
    account_status      STRING,
    credit_limit        DOUBLE,
    primary_contact     STRING,
    email_address       STRING,
    phone_number        STRING,
    billing_city        STRING,
    billing_state       STRING,
    billing_zip         STRING,
    region_code         STRING,
    created_date        DATE,
    last_modified       TIMESTAMP,
    source_system       STRING,
    is_quarantined      BOOLEAN,
    quarantine_reason   STRING,
    dq_score            DOUBLE,
    is_duplicate        BOOLEAN,
    duplicate_of_key    STRING,
    stg_load_date       TIMESTAMP,
    stg_batch_id        STRING
)
USING DELTA
COMMENT 'Staged customers — validated, standardized, quarantine-flagged'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(STAGING, 'work_orders')} (
    work_order_id       STRING NOT NULL,
    work_order_number   STRING,
    site_id             STRING,
    customer_id         STRING,
    work_order_type     STRING,
    priority            STRING,
    status              STRING,
    description         STRING,
    scheduled_date      DATE,
    actual_start        TIMESTAMP,
    actual_end          TIMESTAMP,
    estimated_hours     DOUBLE,
    actual_hours        DOUBLE,
    labor_cost          DOUBLE,
    parts_cost          DOUBLE,
    total_cost          DOUBLE,
    sla_due_date        TIMESTAMP,
    sla_met             BOOLEAN,
    resolution_code     STRING,
    created_date        DATE,
    last_modified       TIMESTAMP,
    is_quarantined      BOOLEAN,
    quarantine_reason   STRING,
    dq_score            DOUBLE,
    ref_integrity_ok    BOOLEAN,
    stg_load_date       TIMESTAMP,
    stg_batch_id        STRING
)
USING DELTA
COMMENT 'Staged work orders — validated and conformed'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(STAGING, 'technicians')} (
    technician_id       STRING NOT NULL,
    employee_number     STRING,
    first_name          STRING,
    last_name           STRING,
    full_name           STRING,
    employment_status   STRING,
    skill_level         STRING,
    home_region         STRING,
    hire_date           DATE,
    termination_date    DATE,
    hourly_rate         DOUBLE,
    manager_id          STRING,
    email               STRING,
    mobile_phone        STRING,
    is_quarantined      BOOLEAN,
    quarantine_reason   STRING,
    dq_score            DOUBLE,
    stg_load_date       TIMESTAMP,
    stg_batch_id        STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(STAGING, 'invoices')} (
    invoice_id          STRING NOT NULL,
    invoice_number      STRING,
    work_order_id       STRING,
    customer_id         STRING,
    invoice_date        DATE,
    due_date            DATE,
    invoice_status      STRING,
    subtotal            DOUBLE,
    tax_amount          DOUBLE,
    discount_amount     DOUBLE,
    total_amount        DOUBLE,
    payment_terms       STRING,
    payment_received    DOUBLE,
    payment_date        DATE,
    is_quarantined      BOOLEAN,
    quarantine_reason   STRING,
    dq_score            DOUBLE,
    stg_load_date       TIMESTAMP,
    stg_batch_id        STRING
)
USING DELTA
""")

print("Staging tables created.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 — Curated Layer Tables (Star Schema)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(CURATED, 'dim_date')} (
    date_sk             INT NOT NULL,
    full_date           DATE NOT NULL,
    year_num            INT,
    quarter_num         INT,
    month_num           INT,
    month_name          STRING,
    week_num            INT,
    day_of_week         INT,
    day_name            STRING,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN,
    fiscal_year         INT,
    fiscal_quarter      INT
)
USING DELTA
COMMENT 'Date dimension — covers 2015-2027'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(CURATED, 'dim_customer')} (
    customer_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id         STRING NOT NULL,
    customer_name       STRING NOT NULL,
    customer_type       STRING,
    account_status      STRING,
    region_code         STRING,
    credit_limit        DOUBLE,
    effective_from      DATE NOT NULL,
    effective_to        DATE,
    is_current          BOOLEAN,
    record_source       STRING,
    load_date           TIMESTAMP
)
USING DELTA
COMMENT 'Customer dimension — SCD Type 2'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(CURATED, 'dim_technician')} (
    technician_sk       BIGINT GENERATED ALWAYS AS IDENTITY,
    technician_id       STRING NOT NULL,
    employee_number     STRING,
    full_name           STRING NOT NULL,
    employment_status   STRING,
    skill_level         STRING,
    home_region         STRING,
    hire_date           DATE,
    effective_from      DATE NOT NULL,
    effective_to        DATE,
    is_current          BOOLEAN,
    load_date           TIMESTAMP
)
USING DELTA
COMMENT 'Technician dimension — SCD Type 2'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(CURATED, 'fact_work_orders')} (
    fact_wo_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
    work_order_id       STRING NOT NULL,
    work_order_number   STRING,
    customer_sk         BIGINT,
    scheduled_date_sk   INT,
    completed_date_sk   INT,
    work_order_type     STRING,
    priority            STRING,
    status              STRING,
    estimated_hours     DOUBLE,
    actual_hours        DOUBLE,
    labor_cost          DOUBLE,
    parts_cost          DOUBLE,
    total_cost          DOUBLE,
    sla_met             BOOLEAN,
    hours_variance      DOUBLE,
    load_date           TIMESTAMP,
    record_source       STRING
)
USING DELTA
COMMENT 'Work order fact table — analytics grain: one row per completed WO'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {full(CURATED, 'fact_invoices')} (
    fact_inv_sk         BIGINT GENERATED ALWAYS AS IDENTITY,
    invoice_id          STRING NOT NULL,
    invoice_number      STRING,
    work_order_id       STRING,
    customer_sk         BIGINT,
    invoice_date_sk     INT,
    due_date_sk         INT,
    invoice_status      STRING,
    subtotal            DOUBLE,
    tax_amount          DOUBLE,
    discount_amount     DOUBLE,
    total_amount        DOUBLE,
    payment_received    DOUBLE,
    outstanding_amount  DOUBLE,
    load_date           TIMESTAMP
)
USING DELTA
COMMENT 'Invoice fact table — financial grain'
""")

print("Curated tables created.")
print("\nAll schemas and tables ready. Run notebook 02 next.")
