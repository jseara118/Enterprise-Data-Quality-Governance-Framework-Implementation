# Databricks Notebook — 03_VALIDATION_FRAMEWORK
# FieldOps Pro | DQ Rules — all 7 dimensions in PySpark + Spark SQL
# ─────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

COMMUNITY_EDITION = False
CATALOG  = "fieldops_dq"
RAW      = "raw"
DQ       = "dq"

def r(table):
    return f"{CATALOG}.{RAW}.{table}" if not COMMUNITY_EDITION else f"{RAW}.{table}"

def dq(table):
    return f"{CATALOG}.{DQ}.{table}" if not COMMUNITY_EDITION else f"{DQ}.{table}"

import uuid
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RUN_ID   = f"RUN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:6].upper()}"
RUN_DATE = date.today()
results  = []   # collect all rule results in memory, bulk-insert at end

def check(rule_code, dq_dimension, target_table, severity, description, total, failed):
    """Record a DQ result and print inline."""
    passed = total - failed
    rate   = round(100 * passed / total, 2) if total > 0 else 100.0
    status = "PASS" if failed == 0 else ("FAIL" if severity in ("CRITICAL","HIGH") else "WARNING")
    icon   = "✓" if status == "PASS" else "✗"
    print(f"  [{icon}] {rule_code:<10} {severity:<10} {status:<8} {failed:>5}/{total} failed  — {description}")
    results.append({
        "run_id": RUN_ID, "rule_code": rule_code, "dq_dimension": dq_dimension,
        "target_table": target_table, "severity": severity,
        "execution_time": datetime.utcnow(), "total_records": int(total),
        "failed_records": int(failed), "pass_rate": rate, "status": status,
        "details": description
    })
    return {"rule_code": rule_code, "status": status, "failed": failed, "total": total, "severity": severity}

print(f"Run ID: {RUN_ID}")

# COMMAND ----------
# MAGIC %md ## Load raw tables into dataframes (cached for performance)

# COMMAND ----------

cust  = spark.table(r("customers")).cache()
tech  = spark.table(r("technicians")).cache()
wo    = spark.table(r("work_orders")).cache()
inv   = spark.table(r("invoices")).cache()
asgn  = spark.table(r("work_order_assignments")).cache()

# Convenience counts
n_cust = cust.count()
n_tech = tech.count()
n_wo   = wo.count()
n_inv  = inv.count()
n_asgn = asgn.count()

print(f"  customers   {n_cust}")
print(f"  technicians {n_tech}")
print(f"  work_orders {n_wo}")
print(f"  invoices    {n_inv}")
print(f"  assignments {n_asgn}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 1 — Completeness

# COMMAND ----------

print("\n── COMPLETENESS ──────────────────────────────────────")

# C-001: NULL or blank customer_name (CRITICAL)
failed = cust.filter(F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == "")).count()
check("C-001","Completeness","raw.customers","CRITICAL","customer_name is NULL or blank",n_cust,failed)

# C-002: NULL account_status (CRITICAL)
failed = cust.filter(F.col("account_status").isNull()).count()
check("C-002","Completeness","raw.customers","CRITICAL","account_status is NULL",n_cust,failed)

# C-003: WO missing site_id or customer_id (CRITICAL)
failed = wo.filter(F.col("site_id").isNull() | F.col("customer_id").isNull()).count()
check("C-003","Completeness","raw.work_orders","CRITICAL","WO missing site_id or customer_id",n_wo,failed)

# C-004: COMPLETED WO missing actual_start or actual_end (HIGH)
completed_wo = wo.filter(F.upper(F.trim(F.col("status"))).isin("COMPLETED","COMPLETE"))
failed = completed_wo.filter(F.col("actual_start").isNull() | F.col("actual_end").isNull()).count()
total  = completed_wo.count()
check("C-004","Completeness","raw.work_orders","HIGH","Completed WO missing actual times",total,failed)

# C-005: Technician missing employee_number (HIGH)
failed = tech.filter(F.col("employee_number").isNull()).count()
check("C-005","Completeness","raw.technicians","HIGH","Technician missing employee_number",n_tech,failed)

# C-006: Technician missing full_name (MEDIUM)
failed = tech.filter(F.col("full_name").isNull() | (F.trim(F.col("full_name")) == "")).count()
check("C-006","Completeness","raw.technicians","MEDIUM","Technician missing full_name",n_tech,failed)

# C-007: Customer missing all three contact fields (MEDIUM)
failed = cust.filter(
    F.col("primary_contact").isNull() &
    F.col("email_address").isNull() &
    F.col("phone_number").isNull()
).count()
check("C-007","Completeness","raw.customers","MEDIUM","Customer missing all contact fields",n_cust,failed)

# C-008: Invoice missing customer_id (CRITICAL)
failed = inv.filter(F.col("customer_id").isNull()).count()
check("C-008","Completeness","raw.invoices","CRITICAL","Invoice missing customer_id",n_inv,failed)

# C-009: TERMINATED technician missing termination_date (HIGH)
term = tech.filter(F.upper(F.trim(F.col("employment_status"))) == "TERMINATED")
failed = term.filter(F.col("termination_date").isNull()).count()
total  = term.count()
check("C-009","Completeness","raw.technicians","HIGH","Terminated tech missing termination_date",total,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 2 — Uniqueness

# COMMAND ----------

print("\n── UNIQUENESS ────────────────────────────────────────")

# U-001: Duplicate customer_id (CRITICAL)
dup_cust_id = cust.groupBy("customer_id").count().filter(F.col("count") > 1)
failed = dup_cust_id.count()
check("U-001","Uniqueness","raw.customers","CRITICAL","Duplicate customer_id",n_cust, n_cust - cust.select("customer_id").distinct().count())

# U-002: Duplicate email across customers (different IDs) (HIGH)
dup_email = (cust.filter(F.col("email_address").isNotNull())
               .withColumn("email_lower", F.lower(F.trim(F.col("email_address"))))
               .groupBy("email_lower").count()
               .filter(F.col("count") > 1))
failed = dup_email.agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
check("U-002","Uniqueness","raw.customers","HIGH","Duplicate email address across customers",n_cust,int(failed))

# U-003: Duplicate work_order_id (CRITICAL)
failed = n_wo - wo.select("work_order_id").distinct().count()
check("U-003","Uniqueness","raw.work_orders","CRITICAL","Duplicate work_order_id",n_wo,failed)

# U-004: Multiple invoices for same work order (HIGH)
dup_wo_inv = inv.groupBy("work_order_id").count().filter(F.col("count") > 1)
failed = dup_wo_inv.agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
check("U-004","Uniqueness","raw.invoices","HIGH","Multiple invoices per work order",n_inv,int(failed))

# U-005: Same technician assigned twice to same WO (HIGH)
dup_asgn = asgn.groupBy("work_order_id","technician_id").count().filter(F.col("count") > 1)
failed = dup_asgn.agg(F.sum(F.col("count") - 1)).collect()[0][0] or 0
check("U-005","Uniqueness","raw.work_order_assignments","HIGH","Same tech assigned twice to same WO",n_asgn,int(failed))

# U-006: Duplicate employee_number across technicians (HIGH)
dup_emp = (tech.filter(F.col("employee_number").isNotNull())
               .groupBy("employee_number").count()
               .filter(F.col("count") > 1))
failed = dup_emp.count()
check("U-006","Uniqueness","raw.technicians","HIGH","Duplicate employee_number",n_tech,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 3 — Validity

# COMMAND ----------

print("\n── VALIDITY ──────────────────────────────────────────")

VALID_STATUS   = ["ACTIVE","INACTIVE","SUSPENDED","PENDING"]
VALID_WO_TYPE  = ["PREVENTIVE_MAINTENANCE","CORRECTIVE_MAINTENANCE",
                   "INSPECTION","EMERGENCY_REPAIR","INSTALLATION"]
VALID_WO_STATUS= ["SCHEDULED","IN_PROGRESS","COMPLETED","CANCELLED","ON_HOLD"]
VALID_TECH_STATUS = ["ACTIVE","TERMINATED","ON_LEAVE","SUSPENDED"]
VALID_INV_STATUS  = ["DRAFT","SENT","PAID","OVERDUE","CANCELLED","VOID"]
VALID_PRIORITY    = ["CRITICAL","HIGH","MEDIUM","LOW"]
VALID_ASGN_ROLE   = ["LEAD","SUPPORT","SOLO"]
VALID_ASGN_STATUS = ["SCHEDULED","ACTIVE","COMPLETED","CANCELLED"]

# V-001: account_status invalid domain (CRITICAL)
failed = cust.filter(
    ~F.upper(F.trim(F.col("account_status"))).isin(VALID_STATUS) | F.col("account_status").isNull()
).count()
check("V-001","Validity","raw.customers","CRITICAL","account_status not in allowed domain",n_cust,failed)

# V-002: WO status invalid domain (CRITICAL)
failed = wo.filter(
    ~F.upper(F.trim(F.regexp_replace(F.col("status")," ","_"))).isin(VALID_WO_STATUS)
).count()
check("V-002","Validity","raw.work_orders","CRITICAL","WO status not in allowed domain",n_wo,failed)

# V-003: Negative credit_limit (HIGH)
failed = cust.filter(
    F.col("credit_limit").isNotNull() &
    F.col("credit_limit").cast("double").isNotNull() &
    (F.col("credit_limit").cast("double") < 0)
).count()
check("V-003","Validity","raw.customers","HIGH","Negative credit_limit",n_cust,failed)

# V-004: Unparseable credit_limit (HIGH)
failed = cust.filter(
    F.col("credit_limit").isNotNull() &
    F.col("credit_limit").cast("double").isNull()
).count()
check("V-004","Validity","raw.customers","HIGH","credit_limit cannot be parsed as numeric",n_cust,failed)

# V-005: Invalid email format (MEDIUM)
failed = cust.filter(
    F.col("email_address").isNotNull() & (
        ~F.col("email_address").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    )
).count()
check("V-005","Validity","raw.customers","MEDIUM","Invalid email format",n_cust,failed)

# V-006: Negative labor/parts/total cost on WOs (HIGH)
failed = wo.filter(
    (F.col("labor_cost").cast("double").isNotNull() & (F.col("labor_cost").cast("double") < 0)) |
    (F.col("parts_cost").cast("double").isNotNull() & (F.col("parts_cost").cast("double") < 0)) |
    (F.col("total_cost").cast("double").isNotNull() & (F.col("total_cost").cast("double") < 0))
).count()
check("V-006","Validity","raw.work_orders","HIGH","Negative monetary value on WO",n_wo,failed)

# V-007: Negative invoice amounts (CRITICAL — financial)
failed = inv.filter(
    (F.col("subtotal").cast("double").isNotNull()  & (F.col("subtotal").cast("double") < 0)) |
    (F.col("total_amount").cast("double").isNotNull() & (F.col("total_amount").cast("double") < 0)) |
    (F.col("payment_received").cast("double").isNotNull() & (F.col("payment_received").cast("double") < 0))
).count()
check("V-007","Validity","raw.invoices","CRITICAL","Negative financial amounts on invoice",n_inv,failed)

# V-008: WO type invalid domain (HIGH)
failed = wo.filter(
    ~F.upper(F.trim(F.col("work_order_type"))).isin(VALID_WO_TYPE)
).count()
check("V-008","Validity","raw.work_orders","HIGH","WO type not in allowed domain",n_wo,failed)

# V-009: Technician employment_status invalid / inconsistent (HIGH)
failed = tech.filter(
    ~F.upper(F.trim(F.regexp_replace(F.col("employment_status")," ","_"))).isin(VALID_TECH_STATUS)
).count()
check("V-009","Validity","raw.technicians","HIGH","employment_status invalid domain/casing",n_tech,failed)

# V-010: Negative hourly_rate (HIGH)
failed = tech.filter(
    F.col("hourly_rate").cast("double").isNotNull() &
    (F.col("hourly_rate").cast("double") < 0)
).count()
check("V-010","Validity","raw.technicians","HIGH","Negative hourly_rate",n_tech,failed)

# V-011: Unrealistic hourly_rate > $500 (MEDIUM)
failed = tech.filter(
    F.col("hourly_rate").cast("double").isNotNull() &
    (F.col("hourly_rate").cast("double") > 500)
).count()
check("V-011","Validity","raw.technicians","MEDIUM","hourly_rate > $500 (suspect value)",n_tech,failed)

# V-012: WO priority not in domain (MEDIUM)
failed = wo.filter(
    F.col("priority").isNotNull() &
    ~F.upper(F.trim(F.col("priority"))).isin(VALID_PRIORITY)
).count()
check("V-012","Validity","raw.work_orders","MEDIUM","WO priority not in allowed domain",n_wo,failed)

# V-013: Invoice status invalid domain (HIGH)
failed = inv.filter(
    ~F.upper(F.trim(F.col("invoice_status"))).isin(VALID_INV_STATUS)
).count()
check("V-013","Validity","raw.invoices","HIGH","invoice_status not in allowed domain",n_inv,failed)

# V-014: Assignment role not in domain (LOW)
failed = asgn.filter(
    ~F.upper(F.trim(F.col("assignment_role"))).isin(VALID_ASGN_ROLE)
).count()
check("V-014","Validity","raw.work_order_assignments","LOW","assignment_role not in allowed domain",n_asgn,failed)

# V-015: Assignment status not in domain (MEDIUM)
failed = asgn.filter(
    ~F.upper(F.trim(F.col("assignment_status"))).isin(VALID_ASGN_STATUS)
).count()
check("V-015","Validity","raw.work_order_assignments","MEDIUM","assignment_status not in allowed domain",n_asgn,failed)

# V-016: Phone number contains non-numeric non-format characters (LOW)
failed = cust.filter(
    F.col("phone_number").isNotNull() &
    F.col("phone_number").rlike(r"[a-zA-Z]")
).count()
check("V-016","Validity","raw.customers","LOW","phone_number contains alpha characters",n_cust,failed)

# V-017: customer_type not in allowed domain (HIGH)
failed = cust.filter(
    ~F.upper(F.trim(F.col("customer_type"))).isin(["ENTERPRISE","GOVERNMENT","SMB"])
).count()
check("V-017","Validity","raw.customers","HIGH","customer_type not in allowed domain",n_cust,failed)

# V-018: discount_amount > subtotal on invoices (CRITICAL — financial)
inv_numeric = inv.withColumn("sub_d", F.col("subtotal").cast("double")) \
                 .withColumn("dis_d", F.col("discount_amount").cast("double"))
failed = inv_numeric.filter(
    F.col("sub_d").isNotNull() & F.col("dis_d").isNotNull() &
    (F.col("dis_d") > F.col("sub_d"))
).count()
check("V-018","Validity","raw.invoices","CRITICAL","discount_amount exceeds subtotal",n_inv,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 4 — Referential Integrity

# COMMAND ----------

print("\n── REFERENTIAL INTEGRITY ─────────────────────────────")

cust_ids = cust.select("customer_id").distinct()
tech_ids  = tech.select("technician_id").distinct()
wo_ids    = wo.select("work_order_id").distinct()

# R-001: WO customer_id orphaned (CRITICAL)
failed = (wo.filter(F.col("customer_id").isNotNull())
            .join(cust_ids, "customer_id", "left_anti")
            .count())
check("R-001","Referential Integrity","raw.work_orders","CRITICAL","WO has orphaned customer_id",n_wo,failed)

# R-002: Invoice work_order_id orphaned (CRITICAL)
failed = (inv.filter(F.col("work_order_id").isNotNull())
             .join(wo_ids, "work_order_id", "left_anti")
             .count())
check("R-002","Referential Integrity","raw.invoices","CRITICAL","Invoice has orphaned work_order_id",n_inv,failed)

# R-003: Assignment work_order_id orphaned (HIGH)
failed = (asgn.filter(F.col("work_order_id").isNotNull())
              .join(wo_ids, "work_order_id", "left_anti")
              .count())
check("R-003","Referential Integrity","raw.work_order_assignments","HIGH","Assignment has orphaned work_order_id",n_asgn,failed)

# R-004: Assignment technician_id orphaned (HIGH)
failed = (asgn.filter(F.col("technician_id").isNotNull())
              .join(tech_ids, "technician_id", "left_anti")
              .count())
check("R-004","Referential Integrity","raw.work_order_assignments","HIGH","Assignment has orphaned technician_id",n_asgn,failed)

# R-005: Invoice customer_id orphaned (HIGH)
failed = (inv.filter(F.col("customer_id").isNotNull())
             .join(cust_ids, "customer_id", "left_anti")
             .count())
check("R-005","Referential Integrity","raw.invoices","HIGH","Invoice has orphaned customer_id",n_inv,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 5 — Consistency

# COMMAND ----------

print("\n── CONSISTENCY ───────────────────────────────────────")

# CS-001: actual_end before actual_start (CRITICAL)
wo_times = wo.withColumn("start_ts", F.to_timestamp("actual_start")) \
             .withColumn("end_ts",   F.to_timestamp("actual_end"))
failed = wo_times.filter(
    F.col("start_ts").isNotNull() & F.col("end_ts").isNotNull() &
    (F.col("end_ts") < F.col("start_ts"))
).count()
check("CS-001","Consistency","raw.work_orders","CRITICAL","actual_end before actual_start",n_wo,failed)

# CS-002: hire_date after termination_date (HIGH)
tech_dates = tech.withColumn("hire_d", F.to_date("hire_date")) \
                 .withColumn("term_d", F.to_date("termination_date"))
failed = tech_dates.filter(
    F.col("hire_d").isNotNull() & F.col("term_d").isNotNull() &
    (F.col("hire_d") > F.col("term_d"))
).count()
check("CS-002","Consistency","raw.technicians","HIGH","hire_date after termination_date",n_tech,failed)

# CS-003: COMPLETED WO missing time data (HIGH)
failed = wo.filter(
    F.upper(F.trim(F.col("status"))).isin("COMPLETED","COMPLETE") &
    (F.col("actual_start").isNull() | F.col("actual_end").isNull())
).count()
check("CS-003","Consistency","raw.work_orders","HIGH","Completed WO missing time data",n_wo,failed)

# CS-004: total_cost ≠ labor + parts (HIGH)
wo_costs = wo.withColumn("lc", F.col("labor_cost").cast("double")) \
             .withColumn("pc", F.col("parts_cost").cast("double")) \
             .withColumn("tc", F.col("total_cost").cast("double"))
failed = wo_costs.filter(
    F.col("lc").isNotNull() & F.col("pc").isNotNull() & F.col("tc").isNotNull() &
    (F.abs((F.col("lc") + F.col("pc")) - F.col("tc")) > 0.01)
).count()
check("CS-004","Consistency","raw.work_orders","HIGH","total_cost does not equal labor+parts",n_wo,failed)

# CS-005: Invoice total does not balance (CRITICAL)
inv_n = inv.withColumn("sub", F.col("subtotal").cast("double")) \
           .withColumn("tax", F.col("tax_amount").cast("double")) \
           .withColumn("dis", F.col("discount_amount").cast("double")) \
           .withColumn("tot", F.col("total_amount").cast("double"))
failed = inv_n.filter(
    F.col("sub").isNotNull() & F.col("tax").isNotNull() &
    F.col("dis").isNotNull() & F.col("tot").isNotNull() &
    (F.abs((F.col("sub") + F.col("tax") - F.col("dis")) - F.col("tot")) > 0.01)
).count()
check("CS-005","Consistency","raw.invoices","CRITICAL","Invoice total does not balance",n_inv,failed)

# CS-006: SLA_MET='Y' but completed after SLA due date (HIGH)
wo_sla = wo.withColumn("end_ts",     F.to_timestamp("actual_end")) \
           .withColumn("sla_ts",     F.to_timestamp("sla_due_date")) \
           .withColumn("sla_met_up", F.upper(F.trim(F.col("sla_met"))))
failed = wo_sla.filter(
    F.col("sla_met_up").isin("Y","YES") &
    F.col("end_ts").isNotNull() &
    F.col("sla_ts").isNotNull() &
    (F.col("end_ts") > F.col("sla_ts"))
).count()
check("CS-006","Consistency","raw.work_orders","HIGH","SLA flagged met but completed after due date",n_wo,failed)

# CS-007: Terminated tech on open (non-completed) WO (HIGH)
term_tech_ids = tech.filter(
    F.upper(F.trim(F.regexp_replace(F.col("employment_status")," ","_"))).isin("TERMINATED")
).select("technician_id")

open_asgn = asgn.join(
    wo.filter(~F.upper(F.trim(F.regexp_replace(F.col("status")," ","_"))).isin("COMPLETED","COMPLETE","CANCELLED")),
    "work_order_id"
)
failed = open_asgn.join(term_tech_ids, "technician_id").count()
check("CS-007","Consistency","raw.work_order_assignments","HIGH","Terminated tech on open WO",n_asgn,failed)

# CS-008: Payment received > invoice total (MEDIUM)
failed = inv_n.filter(
    F.col("payment_received").cast("double").isNotNull() &
    F.col("tot").isNotNull() &
    (F.col("payment_received").cast("double") > F.col("tot"))
).count()
check("CS-008","Consistency","raw.invoices","MEDIUM","Payment received exceeds invoice total",n_inv,failed)

# CS-009: Invoice due_date before invoice_date (HIGH)
inv_dates = inv.withColumn("inv_d", F.to_date("invoice_date")) \
               .withColumn("due_d", F.to_date("due_date"))
failed = inv_dates.filter(
    F.col("inv_d").isNotNull() & F.col("due_d").isNotNull() &
    (F.col("due_d") < F.col("inv_d"))
).count()
check("CS-009","Consistency","raw.invoices","HIGH","due_date before invoice_date",n_inv,failed)

# CS-010: actual_hours > 24 on same-day WO (MEDIUM)
failed = wo_times.filter(
    F.col("actual_hours").cast("double").isNotNull() &
    (F.col("actual_hours").cast("double") > 24) &
    F.col("start_ts").isNotNull() & F.col("end_ts").isNotNull() &
    (F.datediff(F.col("end_ts").cast("date"), F.col("start_ts").cast("date")) == 0)
).count()
check("CS-010","Consistency","raw.work_orders","MEDIUM","actual_hours > 24 on single-day WO",n_wo,failed)

# CS-011: Assignment end_time before start_time (HIGH)
asgn_times = asgn.withColumn("s", F.to_timestamp("start_time")) \
                 .withColumn("e", F.to_timestamp("end_time"))
failed = asgn_times.filter(
    F.col("s").isNotNull() & F.col("e").isNotNull() & (F.col("e") < F.col("s"))
).count()
check("CS-011","Consistency","raw.work_order_assignments","HIGH","Assignment end_time before start_time",n_asgn,failed)

# CS-012: hours_logged significantly mismatches actual start-end duration (MEDIUM)
# Flag if logged hours deviate > 1hr from calculated duration
asgn_hours = asgn_times.filter(
    F.col("s").isNotNull() & F.col("e").isNotNull() & F.col("hours_logged").isNotNull()
).withColumn("calc_hrs", (F.unix_timestamp("e") - F.unix_timestamp("s")) / 3600) \
 .withColumn("logged",   F.col("hours_logged").cast("double"))
failed = asgn_hours.filter(
    F.col("logged").isNotNull() &
    (F.abs(F.col("logged") - F.col("calc_hrs")) > 1.0)
).count()
check("CS-012","Consistency","raw.work_order_assignments","MEDIUM","hours_logged doesn't match start/end duration",n_asgn,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 6 — Timeliness / Freshness

# COMMAND ----------

print("\n── TIMELINESS ────────────────────────────────────────")

today = F.current_date()

# T-001: Customer created_date in the future (HIGH)
failed = cust.filter(
    F.to_date("created_date").isNotNull() &
    (F.to_date("created_date") > today)
).count()
check("T-001","Timeliness","raw.customers","HIGH","customer created_date in the future",n_cust,failed)

# T-002: Technician hire_date in the future (HIGH)
failed = tech.filter(
    F.to_date("hire_date").isNotNull() &
    (F.to_date("hire_date") > today)
).count()
check("T-002","Timeliness","raw.technicians","HIGH","hire_date in the future",n_tech,failed)

# T-003: COMPLETED WO with future scheduled_date (CRITICAL — impossible state)
failed = wo.filter(
    F.upper(F.trim(F.col("status"))).isin("COMPLETED","COMPLETE") &
    F.to_date("scheduled_date").isNotNull() &
    (F.to_date("scheduled_date") > today)
).count()
check("T-003","Timeliness","raw.work_orders","CRITICAL","Completed WO has future scheduled_date",n_wo,failed)

# T-004: Active customer not modified in 2+ years (LOW)
failed = cust.filter(
    F.upper(F.trim(F.col("account_status"))) == "ACTIVE" &
    F.to_timestamp("last_modified").isNotNull() &
    (F.datediff(today, F.to_date("last_modified")) > 730)
).count()
check("T-004","Timeliness","raw.customers","LOW","Active customer record stale > 2 years",n_cust,failed)

# T-005: IN_PROGRESS WO not updated in 90+ days (HIGH — zombie records)
failed = wo.filter(
    F.upper(F.trim(F.regexp_replace(F.col("status")," ","_"))) == "IN_PROGRESS" &
    F.to_timestamp("last_modified").isNotNull() &
    (F.datediff(today, F.to_date("last_modified")) > 90)
).count()
check("T-005","Timeliness","raw.work_orders","HIGH","IN_PROGRESS WO stale > 90 days",n_wo,failed)

# T-006: Unparseable created_date (MEDIUM)
failed = cust.filter(
    F.col("created_date").isNotNull() &
    F.to_date("created_date").isNull()
).count()
check("T-006","Timeliness","raw.customers","MEDIUM","created_date cannot be parsed",n_cust,failed)

# T-007: Unparseable hire_date on technicians (MEDIUM)
failed = tech.filter(
    F.col("hire_date").isNotNull() &
    F.to_date("hire_date").isNull()
).count()
check("T-007","Timeliness","raw.technicians","MEDIUM","hire_date cannot be parsed",n_tech,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DIMENSION 7 — Business Rules

# COMMAND ----------

print("\n── BUSINESS RULES ────────────────────────────────────")

# BR-001: COMPLETED WO with no assignment (HIGH — no tech logged)
comp_wo_ids = wo.filter(
    F.upper(F.trim(F.col("status"))).isin("COMPLETED","COMPLETE")
).select("work_order_id")

assigned_wo_ids = asgn.select("work_order_id").distinct()

failed = comp_wo_ids.join(assigned_wo_ids, "work_order_id", "left_anti").count()
total  = comp_wo_ids.count()
check("BR-001","Business Rules","raw.work_orders","HIGH","Completed WO has no assignment record",total,failed)

# BR-002: COMPLETED WO with no invoice and positive cost (HIGH — revenue leakage)
inv_wo_ids = inv.select("work_order_id").distinct()
uninvoiced = (wo.filter(F.upper(F.trim(F.col("status"))).isin("COMPLETED","COMPLETE"))
                .filter(F.col("total_cost").cast("double") > 0)
                .join(inv_wo_ids, "work_order_id", "left_anti"))
failed = uninvoiced.count()
check("BR-002","Business Rules","raw.work_orders","HIGH","Completed WO missing invoice (revenue leakage risk)",total,failed)

# BR-003: SLA flag inconsistency — recalculate from timestamps (HIGH)
sla_check = wo_sla.filter(
    F.col("end_ts").isNotNull() & F.col("sla_ts").isNotNull() & F.col("sla_met_up").isNotNull()
).withColumn("calc_sla", F.when(F.col("end_ts") <= F.col("sla_ts"), "Y").otherwise("N")) \
 .filter(F.col("calc_sla") != F.col("sla_met_up"))
failed = sla_check.count()
total  = wo_sla.filter(F.col("end_ts").isNotNull() & F.col("sla_ts").isNotNull() & F.col("sla_met_up").isNotNull()).count()
check("BR-003","Business Rules","raw.work_orders","HIGH","SLA flag does not match calculated SLA",total,failed)

# BR-004: PAID invoice with NULL payment_date (MEDIUM)
failed = inv.filter(
    F.upper(F.trim(F.col("invoice_status"))) == "PAID" &
    F.col("payment_date").isNull()
).count()
check("BR-004","Business Rules","raw.invoices","MEDIUM","PAID invoice missing payment_date",n_inv,failed)

# BR-005: estimated_hours = 0 on a COMPLETED WO (MEDIUM)
failed = wo.filter(
    F.upper(F.trim(F.col("status"))).isin("COMPLETED","COMPLETE") &
    F.col("estimated_hours").cast("double").isNotNull() &
    (F.col("estimated_hours").cast("double") == 0)
).count()
check("BR-005","Business Rules","raw.work_orders","MEDIUM","Completed WO has estimated_hours = 0",n_wo,failed)

# COMMAND ----------
# MAGIC %md
# MAGIC ## DQ Score Calculation & Results Persistence

# COMMAND ----------

SEVERITY_WEIGHT = {"CRITICAL": 30, "HIGH": 20, "MEDIUM": 10, "LOW": 5}

def calc_score(rule_list):
    max_w  = sum(SEVERITY_WEIGHT.get(r["severity"], 5) for r in rule_list)
    deduct = sum(
        SEVERITY_WEIGHT.get(r["severity"], 5) * (r["failed"] / max(r["total"], 1))
        for r in rule_list if r["failed"] > 0
    )
    return max(0, round(100 - (deduct / max_w * 100), 2)) if max_w > 0 else 0.0

def dim_score(dim_name):
    return calc_score([r for r in results if r["dq_dimension"] == dim_name])

overall         = calc_score(results)
completeness    = dim_score("Completeness")
uniqueness      = dim_score("Uniqueness")
validity        = dim_score("Validity")
consistency     = dim_score("Consistency")
timeliness      = dim_score("Timeliness")
ref_integrity   = dim_score("Referential Integrity")
business_rules  = dim_score("Business Rules")

critical_fails  = sum(1 for r in results if r["status"] == "FAIL" and r["severity"] == "CRITICAL")
high_fails      = sum(1 for r in results if r["status"] == "FAIL" and r["severity"] == "HIGH")

print(f"\n{'='*55}")
print(f"  OVERALL DQ SCORE : {overall:>5.1f} / 100")
grade = "A" if overall >= 90 else "B" if overall >= 75 else "C" if overall >= 60 else "D" if overall >= 50 else "F"
print(f"  GRADE            : {grade}")
print(f"  Critical failures: {critical_fails}")
print(f"  High failures    : {high_fails}")
print(f"{'='*55}")
for dim, sc in [
    ("Completeness",     completeness),
    ("Uniqueness",       uniqueness),
    ("Validity",         validity),
    ("Consistency",      consistency),
    ("Timeliness",       timeliness),
    ("Ref Integrity",    ref_integrity),
    ("Business Rules",   business_rules),
]:
    bar = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    print(f"  {dim:<18} [{bar}] {sc:>5.1f}%")

# Persist results to dq.dq_results
results_df = spark.createDataFrame(results)
results_df.write.format("delta").mode("append").saveAsTable(dq("dq_results"))

# Persist score summary
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType
score_row = [(
    RUN_ID, RUN_DATE, "ALL_TABLES",
    len(results),
    sum(1 for r in results if r["status"] == "PASS"),
    sum(1 for r in results if r["status"] in ("FAIL","WARNING")),
    critical_fails, high_fails,
    overall, completeness, uniqueness, validity, consistency, timeliness, ref_integrity
)]
score_schema = "run_id STRING, score_date DATE, target_table STRING, total_rules_run INT, rules_passed INT, rules_failed INT, critical_failures INT, high_failures INT, overall_score DOUBLE, completeness_score DOUBLE, uniqueness_score DOUBLE, validity_score DOUBLE, consistency_score DOUBLE, timeliness_score DOUBLE, ref_integrity_score DOUBLE"
score_df = spark.createDataFrame(score_row, score_schema)
score_df.write.format("delta").mode("append").saveAsTable(dq("dq_score_summary"))

print(f"\n  Results persisted to {dq('dq_results')}")
print(f"  Score persisted to   {dq('dq_score_summary')}")
print(f"\n  Next: Run notebook 04_staging_pipeline")
