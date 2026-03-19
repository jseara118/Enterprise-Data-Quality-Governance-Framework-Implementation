# Databricks Notebook — 04_STAGING_AND_CURATED_PIPELINE
# FieldOps Pro | Raw → Staging → Curated
# ─────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

COMMUNITY_EDITION = False
CATALOG  = "fieldops_dq"
RAW      = "raw"
STAGING  = "stg"
CURATED  = "curated"
DQ       = "dq"
AUDIT    = "audit"

def r(t): return f"{CATALOG}.{RAW}.{t}"     if not COMMUNITY_EDITION else f"{RAW}.{t}"
def s(t): return f"{CATALOG}.{STAGING}.{t}" if not COMMUNITY_EDITION else f"{STAGING}.{t}"
def c(t): return f"{CATALOG}.{CURATED}.{t}" if not COMMUNITY_EDITION else f"{CURATED}.{t}"
def dq(t): return f"{CATALOG}.{DQ}.{t}"     if not COMMUNITY_EDITION else f"{DQ}.{t}"

import uuid
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RUN_ID   = f"RUN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:6].upper()}"
BATCH_ID = "BATCH-20240110"
LOAD_TS  = datetime.utcnow()

print(f"Run ID: {RUN_ID}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Standardize and Stage Customers
# MAGIC - Normalize status/type casing
# MAGIC - Coerce credit_limit to double
# MAGIC - Flag and deduplicate by email
# MAGIC - Score each record 0–100
# MAGIC - Quarantine CRITICAL/HIGH failures

# COMMAND ----------

raw_cust = spark.table(r("customers"))

VALID_STATUS = {"active":"ACTIVE","activ":"ACTIVE","inactive":"INACTIVE",
                "suspended":"SUSPENDED","temp suspend":"SUSPENDED",
                "pending":"PENDING"}
VALID_TYPE   = {"enterprise":"ENTERPRISE","smb":"SMB","government":"GOVERNMENT"}

# Deduplication: keep latest by last_modified per email
email_window  = Window.partitionBy(F.lower(F.trim(F.col("email_address")))).orderBy(
                    F.to_timestamp("last_modified").desc())
id_window     = Window.partitionBy("customer_id").orderBy(F.col("_extract_timestamp").desc())

deduped = (raw_cust
    .withColumn("rn_email", F.row_number().over(email_window))
    .withColumn("rn_id",    F.row_number().over(id_window)))

# Build standardized columns
stg_cust = (deduped
    # Normalized status
    .withColumn("status_std", F.when(
        F.upper(F.trim(F.col("account_status"))) == "ACTIVE", "ACTIVE").when(
        F.upper(F.trim(F.col("account_status"))).isin("INACTIVE","INACT"), "INACTIVE").when(
        F.upper(F.trim(F.col("account_status"))).isin("SUSPENDED","TEMP SUSPEND"), "SUSPENDED").when(
        F.upper(F.trim(F.col("account_status"))) == "PENDING", "PENDING").otherwise(
        F.upper(F.trim(F.col("account_status")))))

    # Normalized type
    .withColumn("type_std", F.when(
        F.upper(F.trim(F.col("customer_type"))).isin("ENTERPRISE","ENTERP"), "ENTERPRISE").when(
        F.upper(F.trim(F.col("customer_type"))).isin("GOVERNMENT","GOVT","GOV"), "GOVERNMENT").when(
        F.upper(F.trim(F.col("customer_type"))) == "SMB", "SMB").otherwise("UNKNOWN"))

    # credit_limit — coerce to double, None if unparseable
    .withColumn("credit_limit_d", F.col("credit_limit").cast("double"))

    # Dates
    .withColumn("created_date_d",   F.to_date("created_date"))
    .withColumn("last_modified_ts", F.to_timestamp("last_modified"))

    # Quarantine flags
    .withColumn("q_null_name",      F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == ""))
    .withColumn("q_null_status",    F.col("account_status").isNull())
    .withColumn("q_invalid_status", ~F.upper(F.trim(F.col("account_status"))).isin(
                                     "ACTIVE","INACTIVE","SUSPENDED","PENDING","ACTIV","TEMP SUSPEND") & F.col("account_status").isNotNull())
    .withColumn("q_neg_credit",     F.col("credit_limit_d").isNotNull() & (F.col("credit_limit_d") < 0))
    .withColumn("q_bad_credit",     F.col("credit_limit").isNotNull() & F.col("credit_limit_d").isNull())
    .withColumn("q_future_date",    F.col("created_date_d").isNotNull() & (F.col("created_date_d") > F.current_date()))
    .withColumn("is_duplicate",     (F.col("rn_email") > 1) | (F.col("rn_id") > 1))

    .withColumn("should_quarantine",
        F.col("q_null_name") | F.col("q_null_status") |
        F.col("q_neg_credit") | F.col("q_bad_credit"))

    .withColumn("quarantine_reason",
        F.when(F.col("q_null_name"),      "CRITICAL: customer_name is NULL or blank")
        .when(F.col("q_null_status"),     "CRITICAL: account_status is NULL")
        .when(F.col("q_invalid_status"),  F.concat(F.lit("HIGH: invalid account_status ["), F.col("account_status"), F.lit("]")))
        .when(F.col("q_neg_credit"),      "HIGH: negative credit_limit")
        .when(F.col("q_bad_credit"),      F.concat(F.lit("HIGH: unparseable credit_limit ["), F.col("credit_limit"), F.lit("]")))
        .when(F.col("is_duplicate"),      "MEDIUM: duplicate record — lower priority version")
        .otherwise(F.lit(None)))

    # DQ score per record
    .withColumn("dq_score", F.greatest(F.lit(0.0),
        F.lit(100.0)
        - F.when(F.col("q_null_name"),      30.0).otherwise(0.0)
        - F.when(F.col("q_null_status"),    20.0).otherwise(0.0)
        - F.when(F.col("q_invalid_status"), 15.0).otherwise(0.0)
        - F.when(F.col("q_neg_credit"),     15.0).otherwise(0.0)
        - F.when(F.col("q_bad_credit"),     15.0).otherwise(0.0)
        - F.when(F.col("email_address").isNull(), 10.0).otherwise(0.0)
        - F.when(~F.col("email_address").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$") &
                 F.col("email_address").isNotNull(), 10.0).otherwise(0.0)
        - F.when(F.col("primary_contact").isNull(), 5.0).otherwise(0.0)
        - F.when(F.col("q_future_date"),    10.0).otherwise(0.0)
    ))
)

# Select final staging columns
stg_cust_final = stg_cust.select(
    F.col("customer_id"),
    F.trim(F.col("customer_name")).alias("customer_name"),
    F.col("type_std").alias("customer_type"),
    F.col("status_std").alias("account_status"),
    F.col("credit_limit_d").alias("credit_limit"),
    F.trim(F.col("primary_contact")).alias("primary_contact"),
    F.lower(F.trim(F.col("email_address"))).alias("email_address"),
    F.col("phone_number"),
    F.trim(F.col("billing_city")).alias("billing_city"),
    F.upper(F.trim(F.col("billing_state"))).alias("billing_state"),
    F.substring(F.trim(F.col("billing_zip")), 1, 5).alias("billing_zip"),
    F.upper(F.trim(F.col("region_code"))).alias("region_code"),
    F.col("created_date_d").alias("created_date"),
    F.col("last_modified_ts").alias("last_modified"),
    F.col("source_system"),
    (F.col("should_quarantine") | F.col("is_duplicate")).cast("boolean").alias("is_quarantined"),
    F.col("quarantine_reason"),
    F.col("dq_score"),
    F.col("is_duplicate"),
    F.lit(None).cast("string").alias("duplicate_of_key"),
    F.lit(LOAD_TS).alias("stg_load_date"),
    F.lit(BATCH_ID).alias("stg_batch_id"),
)

stg_cust_final.write.format("delta").mode("overwrite").saveAsTable(s("customers"))

total_cust      = stg_cust_final.count()
quarantined_cust = stg_cust_final.filter(F.col("is_quarantined")).count()
clean_cust      = total_cust - quarantined_cust

print(f"stg.customers: {total_cust} total | {clean_cust} clean | {quarantined_cust} quarantined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Quarantine Log — write rejected records

# COMMAND ----------

quarantine_records = stg_cust_final.filter(F.col("is_quarantined")).select(
    F.lit(RUN_ID).alias("run_id"),
    F.lit("raw.customers").alias("source_table"),
    F.col("customer_id").alias("source_key"),
    F.when(F.col("quarantine_reason").startswith("CRITICAL"), "C-001")
     .when(F.col("quarantine_reason").startswith("HIGH: neg"), "V-003")
     .when(F.col("quarantine_reason").startswith("HIGH: unpars"), "V-004")
     .when(F.col("quarantine_reason").startswith("MEDIUM"), "U-002")
     .otherwise("V-001").alias("rule_code"),
    F.when(F.col("quarantine_reason").startswith("CRITICAL"), "CRITICAL")
     .when(F.col("quarantine_reason").startswith("MEDIUM"), "MEDIUM")
     .otherwise("HIGH").alias("severity"),
    F.col("quarantine_reason"),
    F.lit(None).cast("string").alias("raw_record"),
    F.lit(LOAD_TS).alias("quarantine_date"),
    F.lit(False).alias("resolved_flag"),
    F.lit(None).cast("timestamp").alias("resolved_date"),
    F.lit(None).cast("string").alias("resolved_by"),
)

quarantine_records.write.format("delta").mode("append").saveAsTable(dq("quarantine_log"))
print(f"Quarantine log: {quarantined_cust} records written")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Stage Work Orders

# COMMAND ----------

raw_wo   = spark.table(r("work_orders"))
cust_ids = spark.table(r("customers")).select("customer_id").distinct()

wo_deduped = raw_wo.withColumn(
    "rn", F.row_number().over(Window.partitionBy("work_order_id").orderBy(F.col("_extract_timestamp").desc()))
).filter(F.col("rn") == 1)

stg_wo = (wo_deduped
    .withColumn("start_ts",   F.to_timestamp("actual_start"))
    .withColumn("end_ts",     F.to_timestamp("actual_end"))
    .withColumn("sla_ts",     F.to_timestamp("sla_due_date"))
    .withColumn("lc",         F.col("labor_cost").cast("double"))
    .withColumn("pc",         F.col("parts_cost").cast("double"))
    .withColumn("tc",         F.col("total_cost").cast("double"))
    .withColumn("ah",         F.col("actual_hours").cast("double"))
    .withColumn("eh",         F.col("estimated_hours").cast("double"))

    # Status normalization
    .withColumn("status_std",
        F.when(F.upper(F.trim(F.regexp_replace("status"," ","_"))).isin("SCHEDULED"),     "SCHEDULED")
        .when(F.upper(F.trim(F.regexp_replace("status"," ","_"))).isin("IN_PROGRESS"),    "IN_PROGRESS")
        .when(F.upper(F.trim(F.regexp_replace("status"," ","_"))).isin("COMPLETED","COMPLETE"), "COMPLETED")
        .when(F.upper(F.trim(F.regexp_replace("status"," ","_"))).isin("CANCELLED"),      "CANCELLED")
        .when(F.upper(F.trim(F.regexp_replace("status"," ","_"))).isin("ON_HOLD"),        "ON_HOLD")
        .otherwise("UNKNOWN"))

    # SLA met normalization
    .withColumn("sla_met_std",
        F.when(F.upper(F.trim(F.col("sla_met"))).isin("Y","YES","1","TRUE"), True)
        .when(F.upper(F.trim(F.col("sla_met"))).isin("N","NO","0","FALSE"), False)
        .otherwise(F.lit(None).cast("boolean")))

    # WO type normalization
    .withColumn("wo_type_std",
        F.when(F.upper(F.col("work_order_type")).isin(
            "PREVENTIVE_MAINTENANCE","CORRECTIVE_MAINTENANCE",
            "INSPECTION","EMERGENCY_REPAIR","INSTALLATION"), F.upper(F.col("work_order_type")))
        .otherwise("OTHER"))

    # Referential integrity
    .withColumn("ref_ok", F.col("customer_id").isin(
        [row.customer_id for row in cust_ids.collect()]))

    # Quarantine conditions
    .withColumn("q_null_cust",   F.col("customer_id").isNull())
    .withColumn("q_orphan_cust", ~F.col("ref_ok") & F.col("customer_id").isNotNull())
    .withColumn("q_inv_time",    F.col("start_ts").isNotNull() & F.col("end_ts").isNotNull() &
                                  (F.col("end_ts") < F.col("start_ts")))
    .withColumn("q_neg_cost",    (F.col("lc") < 0) | (F.col("pc") < 0))

    .withColumn("should_quarantine",
        F.col("q_null_cust") | F.col("q_orphan_cust") | F.col("q_inv_time") | F.col("q_neg_cost"))

    .withColumn("quarantine_reason",
        F.when(F.col("q_null_cust"),    "CRITICAL: customer_id is NULL")
        .when(F.col("q_orphan_cust"),   F.concat(F.lit("CRITICAL: orphaned customer_id ["), F.col("customer_id"), F.lit("]")))
        .when(F.col("q_inv_time"),      "CRITICAL: actual_end before actual_start")
        .when(F.col("q_neg_cost"),      "HIGH: negative cost value")
        .otherwise(F.lit(None)))

    .withColumn("dq_score", F.greatest(F.lit(0.0),
        F.lit(100.0)
        - F.when(F.col("q_null_cust"),    30.0).otherwise(0.0)
        - F.when(F.col("q_orphan_cust"),  25.0).otherwise(0.0)
        - F.when(F.col("site_id").isNull(), 15.0).otherwise(0.0)
        - F.when(F.col("q_inv_time"),     20.0).otherwise(0.0)
        - F.when(F.col("q_neg_cost"),     15.0).otherwise(0.0)
        - F.when(F.col("description").isNull(), 5.0).otherwise(0.0)
        - F.when(F.col("priority").isNull(), 5.0).otherwise(0.0)
    ))
)

stg_wo_final = stg_wo.select(
    "work_order_id","work_order_number","site_id","customer_id",
    F.col("wo_type_std").alias("work_order_type"),
    F.upper(F.trim(F.col("priority"))).alias("priority"),
    F.col("status_std").alias("status"),
    F.trim(F.col("description")).alias("description"),
    F.to_date("scheduled_date").alias("scheduled_date"),
    F.col("start_ts").alias("actual_start"),
    # Null out inverted end times — don't promote bad data
    F.when(F.col("q_inv_time"), F.lit(None)).otherwise(F.col("end_ts")).alias("actual_end"),
    F.col("eh").alias("estimated_hours"),
    # Cap impossible hours at NULL
    F.when((F.col("ah") > 24) | (F.col("ah") < 0), F.lit(None)).otherwise(F.col("ah")).alias("actual_hours"),
    # Replace negative costs with NULL
    F.when(F.col("lc") < 0, F.lit(None)).otherwise(F.col("lc")).alias("labor_cost"),
    F.when(F.col("pc") < 0, F.lit(None)).otherwise(F.col("pc")).alias("parts_cost"),
    F.when(F.col("tc") < 0, F.lit(None)).otherwise(F.col("tc")).alias("total_cost"),
    F.col("sla_ts").alias("sla_due_date"),
    F.col("sla_met_std").alias("sla_met"),
    F.trim(F.col("resolution_code")).alias("resolution_code"),
    F.to_date("created_date").alias("created_date"),
    F.to_timestamp("last_modified").alias("last_modified"),
    F.col("should_quarantine").alias("is_quarantined"),
    F.col("quarantine_reason"),
    F.col("dq_score"),
    F.col("ref_ok").alias("ref_integrity_ok"),
    F.lit(LOAD_TS).alias("stg_load_date"),
    F.lit(BATCH_ID).alias("stg_batch_id"),
)

stg_wo_final.write.format("delta").mode("overwrite").saveAsTable(s("work_orders"))

total_wo      = stg_wo_final.count()
quarantined_wo = stg_wo_final.filter(F.col("is_quarantined")).count()
print(f"stg.work_orders: {total_wo} total | {total_wo - quarantined_wo} clean | {quarantined_wo} quarantined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Stage Technicians

# COMMAND ----------

raw_tech = spark.table(r("technicians"))

stg_tech = (raw_tech
    .withColumn("rn", F.row_number().over(Window.partitionBy("technician_id").orderBy(F.col("_extract_timestamp").desc())))
    .filter(F.col("rn") == 1)
    .withColumn("hire_d", F.to_date("hire_date"))
    .withColumn("term_d", F.to_date("termination_date"))
    .withColumn("rate_d", F.col("hourly_rate").cast("double"))

    .withColumn("status_std",
        F.when(F.upper(F.trim(F.regexp_replace("employment_status"," ","_"))).isin("ACTIVE","ACTVE"), "ACTIVE")
        .when(F.upper(F.trim(F.regexp_replace("employment_status"," ","_"))).isin("TERMINATED"), "TERMINATED")
        .when(F.upper(F.trim(F.regexp_replace("employment_status"," ","_"))).isin("ON_LEAVE","ON-LEAVE"), "ON_LEAVE")
        .when(F.upper(F.trim(F.regexp_replace("employment_status"," ","_"))).isin("SUSPENDED"), "SUSPENDED")
        .otherwise("UNKNOWN"))

    .withColumn("q_null_emp", F.col("employee_number").isNull())
    .withColumn("q_neg_rate", F.col("rate_d").isNotNull() & (F.col("rate_d") < 0))
    .withColumn("q_future_hire", F.col("hire_d").isNotNull() & (F.col("hire_d") > F.current_date()))
    .withColumn("q_inv_dates", F.col("hire_d").isNotNull() & F.col("term_d").isNotNull() & (F.col("hire_d") > F.col("term_d")))

    .withColumn("should_quarantine", F.col("q_neg_rate") | F.col("q_inv_dates"))

    .withColumn("quarantine_reason",
        F.when(F.col("q_inv_dates"),    "HIGH: hire_date after termination_date")
        .when(F.col("q_neg_rate"),      "HIGH: negative hourly_rate")
        .when(F.col("q_future_hire"),   "HIGH: hire_date in the future")
        .when(F.col("q_null_emp"),      "MEDIUM: missing employee_number")
        .otherwise(F.lit(None)))

    .withColumn("dq_score", F.greatest(F.lit(0.0),
        F.lit(100.0)
        - F.when(F.col("q_neg_rate"),    20.0).otherwise(0.0)
        - F.when(F.col("q_inv_dates"),   20.0).otherwise(0.0)
        - F.when(F.col("q_future_hire"), 15.0).otherwise(0.0)
        - F.when(F.col("q_null_emp"),    10.0).otherwise(0.0)
        - F.when(F.col("full_name").isNull(), 10.0).otherwise(0.0)
        - F.when(F.col("skill_level").isNull(), 5.0).otherwise(0.0)
    ))
)

stg_tech_final = stg_tech.select(
    "technician_id","employee_number","first_name","last_name",
    # Derive full_name if missing
    F.when(F.col("full_name").isNotNull() & (F.trim(F.col("full_name")) != ""),
           F.trim(F.col("full_name")))
     .otherwise(F.concat_ws(" ", F.trim(F.col("first_name")), F.trim(F.col("last_name")))).alias("full_name"),
    F.col("status_std").alias("employment_status"),
    F.upper(F.trim(F.col("skill_level"))).alias("skill_level"),
    F.upper(F.trim(F.col("home_region"))).alias("home_region"),
    F.col("hire_d").alias("hire_date"),
    F.col("term_d").alias("termination_date"),
    # Replace negative/invalid rates with NULL
    F.when(F.col("rate_d") < 0, F.lit(None)).when(F.col("rate_d") > 500, F.lit(None)).otherwise(F.col("rate_d")).alias("hourly_rate"),
    "manager_id", "email", "mobile_phone",
    F.col("should_quarantine").alias("is_quarantined"),
    F.col("quarantine_reason"),
    F.col("dq_score"),
    F.lit(LOAD_TS).alias("stg_load_date"),
    F.lit(BATCH_ID).alias("stg_batch_id"),
)

stg_tech_final.write.format("delta").mode("overwrite").saveAsTable(s("technicians"))
total_tech = stg_tech_final.count()
q_tech     = stg_tech_final.filter(F.col("is_quarantined")).count()
print(f"stg.technicians: {total_tech} total | {total_tech - q_tech} clean | {q_tech} quarantined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Build Curated Dimension: dim_date

# COMMAND ----------

# Generate date dimension 2015-01-01 through 2027-12-31
from pyspark.sql.types import DateType
import datetime as dt

date_list = []
d = dt.date(2015, 1, 1)
end = dt.date(2027, 12, 31)
while d <= end:
    date_list.append((d,))
    d += dt.timedelta(days=1)

date_df = spark.createDataFrame(date_list, ["full_date"])

dim_date = date_df.select(
    (F.year("full_date") * 10000 + F.month("full_date") * 100 + F.dayofmonth("full_date")).alias("date_sk"),
    "full_date",
    F.year("full_date").alias("year_num"),
    F.quarter("full_date").alias("quarter_num"),
    F.month("full_date").alias("month_num"),
    F.date_format("full_date","MMMM").alias("month_name"),
    F.weekofyear("full_date").alias("week_num"),
    F.dayofweek("full_date").alias("day_of_week"),
    F.date_format("full_date","EEEE").alias("day_name"),
    F.dayofweek("full_date").isin(1,7).alias("is_weekend"),
    F.lit(False).alias("is_holiday"),
    F.year("full_date").alias("fiscal_year"),
    F.quarter("full_date").alias("fiscal_quarter"),
)

dim_date.write.format("delta").mode("overwrite").saveAsTable(c("dim_date"))
print(f"curated.dim_date: {dim_date.count()} dates loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Build Curated Dimension: dim_customer

# COMMAND ----------

clean_cust_df = spark.table(s("customers")).filter(
    (F.col("is_quarantined") == False) & (F.col("is_duplicate") == False)
)

dim_customer = clean_cust_df.select(
    "customer_id","customer_name","customer_type","account_status","region_code","credit_limit",
    F.coalesce(F.col("created_date"), F.lit(dt.date(2000, 1, 1))).alias("effective_from"),
    F.lit(None).cast("date").alias("effective_to"),
    F.lit(True).alias("is_current"),
    F.col("stg_batch_id").alias("record_source"),
    F.lit(LOAD_TS).alias("load_date"),
)

dim_customer.write.format("delta").mode("overwrite").saveAsTable(c("dim_customer"))
print(f"curated.dim_customer: {dim_customer.count()} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7 — Build Curated Fact: fact_work_orders

# COMMAND ----------

clean_wo   = spark.table(s("work_orders")).filter(F.col("is_quarantined") == False)
dim_cust   = spark.table(c("dim_customer")).filter(F.col("is_current") == True).select("customer_sk","customer_id")
dim_date_df = spark.table(c("dim_date")).select("date_sk","full_date")

fact_wo = (clean_wo
    .join(dim_cust, "customer_id", "left")
    .withColumn("sched_date_dt", F.col("scheduled_date"))
    .withColumn("comp_date_dt",  F.to_date(F.col("actual_end")))
    # Recalculate total_cost from components (fixes arithmetic errors)
    .withColumn("total_cost_calc",
        F.coalesce(F.col("labor_cost"), F.lit(0.0)) + F.coalesce(F.col("parts_cost"), F.lit(0.0)))
    .withColumn("hours_variance",
        F.when(F.col("actual_hours").isNotNull() & F.col("estimated_hours").isNotNull(),
               F.col("actual_hours") - F.col("estimated_hours"))
        .otherwise(F.lit(None).cast("double")))
)

# Join to dim_date for date surrogate keys
sched_dates = dim_date_df.withColumnRenamed("date_sk","scheduled_date_sk").withColumnRenamed("full_date","sched_date_dt")
comp_dates  = dim_date_df.withColumnRenamed("date_sk","completed_date_sk").withColumnRenamed("full_date","comp_date_dt")

fact_wo_final = (fact_wo
    .join(sched_dates, "sched_date_dt", "left")
    .join(comp_dates,  "comp_date_dt",  "left")
    .select(
        "work_order_id","work_order_number",
        "customer_sk","scheduled_date_sk","completed_date_sk",
        "work_order_type","priority","status",
        "estimated_hours","actual_hours",
        "labor_cost","parts_cost",
        F.col("total_cost_calc").alias("total_cost"),
        "sla_met","hours_variance",
        F.lit(LOAD_TS).alias("load_date"),
        F.col("stg_batch_id").alias("record_source"),
    )
)

fact_wo_final.write.format("delta").mode("overwrite").saveAsTable(c("fact_work_orders"))
print(f"curated.fact_work_orders: {fact_wo_final.count()} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8 — Before / After Comparison

# COMMAND ----------

print("\n" + "=" * 60)
print("  BEFORE vs AFTER — Data Quality Impact")
print("=" * 60)

raw_c   = spark.table(r("customers"))
stg_c   = spark.table(s("customers"))
cur_c   = spark.table(c("dim_customer")).filter(F.col("is_current"))

raw_wo_t   = spark.table(r("work_orders"))
stg_wo_t   = spark.table(s("work_orders"))
cur_wo_t   = spark.table(c("fact_work_orders"))

def pct(n, total): return f"{round(100*n/max(total,1),1)}%"

print(f"\n  {'Metric':<40} {'RAW':>8}  {'STAGING':>10}  {'CURATED':>10}")
print(f"  {'-'*70}")

# Customers
r_null_name = raw_c.filter(F.col("customer_name").isNull()).count()
s_null_name = stg_c.filter(F.col("is_quarantined") == False).filter(F.col("customer_name").isNull()).count()

r_inv_status = raw_c.filter(~F.upper(F.trim(F.col("account_status"))).isin("ACTIVE","INACTIVE","SUSPENDED","PENDING") | F.col("account_status").isNull()).count()
s_inv_status = stg_c.filter(F.col("is_quarantined") == False).filter(~F.col("account_status").isin("ACTIVE","INACTIVE","SUSPENDED","PENDING") | F.col("account_status").isNull()).count()

r_dups  = raw_c.count() - raw_c.select("customer_id").distinct().count()
s_dups  = stg_c.filter(F.col("is_quarantined") == False).count() - stg_c.filter(F.col("is_quarantined") == False).select("customer_id").distinct().count()

print(f"  {'Customers — NULL name':<40} {r_null_name:>8}  {s_null_name:>10}  {'0':>10}")
print(f"  {'Customers — Invalid status':<40} {r_inv_status:>8}  {s_inv_status:>10}  {'0':>10}")
print(f"  {'Customers — Duplicate IDs':<40} {r_dups:>8}  {s_dups:>10}  {'0':>10}")

r_neg = raw_wo_t.filter((F.col("labor_cost").cast("double") < 0) | (F.col("parts_cost").cast("double") < 0)).count()
s_neg = stg_wo_t.filter(F.col("is_quarantined") == False).filter((F.col("labor_cost") < 0) | (F.col("parts_cost") < 0)).count()
print(f"  {'Work Orders — Negative costs':<40} {r_neg:>8}  {s_neg:>10}  {'0':>10}")

r_inv_ts = raw_wo_t.filter(F.to_timestamp("actual_end") < F.to_timestamp("actual_start")).count()
s_inv_ts = stg_wo_t.filter(F.col("is_quarantined") == False).filter(
    F.col("actual_end").isNotNull() & F.col("actual_start").isNotNull() &
    (F.col("actual_end") < F.col("actual_start"))).count()
print(f"  {'Work Orders — Inverted timestamps':<40} {r_inv_ts:>8}  {s_inv_ts:>10}  {'0':>10}")

print(f"\n  {'Total customer records':<40} {raw_c.count():>8}  {stg_c.filter(F.col('is_quarantined')==False).count():>10}  {cur_c.count():>10}")
print(f"  {'Total work order records':<40} {raw_wo_t.count():>8}  {stg_wo_t.filter(F.col('is_quarantined')==False).count():>10}  {cur_wo_t.count():>10}")

# COMMAND ----------
# MAGIC %md ## Pipeline Run Log

# COMMAND ----------

run_log = spark.createDataFrame([(
    RUN_ID, "fieldops_dq_pipeline",
    LOAD_TS, datetime.utcnow(),
    "COMPLETED",
    raw_c.count() + raw_wo_t.count(),
    stg_c.count() + stg_wo_t.count(),
    stg_c.filter(F.col("is_quarantined")).count() + stg_wo_t.filter(F.col("is_quarantined")).count(),
    cur_c.count() + cur_wo_t.count(),
    None, None,
    "notebook"
)], "run_id STRING, pipeline_name STRING, run_start TIMESTAMP, run_end TIMESTAMP, status STRING, records_extracted LONG, records_staged LONG, records_quarantined LONG, records_curated LONG, dq_score DOUBLE, error_message STRING, triggered_by STRING")

run_log.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{AUDIT}.pipeline_runs" if not COMMUNITY_EDITION else f"{AUDIT}.pipeline_runs")

print(f"\nPipeline run logged: {RUN_ID}")
print("Next: Run notebook 05_python_connection to query from external Python")
