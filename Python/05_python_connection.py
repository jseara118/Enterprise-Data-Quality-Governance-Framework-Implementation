"""
FieldOps Pro — External Python DQ Client
=========================================
Connects to your Databricks workspace from your local machine
or from any external Python environment (CI, Airflow, etc.)

Supports two connection modes:
  1. Databricks SQL Connector  ← recommended for SQL queries
  2. databricks-connect         ← recommended for PySpark jobs

Setup:
    pip install databricks-sql-connector pandas tabulate python-dotenv

Environment variables (create a .env file — never commit it):
    DATABRICKS_HOST        = https://your-workspace.azuredatabricks.net
    DATABRICKS_TOKEN       = dapi...
    DATABRICKS_HTTP_PATH   = /sql/1.0/warehouses/your-warehouse-id
    DATABRICKS_CATALOG     = fieldops_dq         (omit for Community Edition)

Get these from:
  - DATABRICKS_HOST  : Databricks workspace URL
  - DATABRICKS_TOKEN : User Settings → Developer → Access Tokens → Generate
  - HTTP_PATH        : SQL Warehouses → your warehouse → Connection Details

Usage:
    python 05_python_connection.py                  # full DQ report
    python 05_python_connection.py --table customers  # single table
    python 05_python_connection.py --export-csv       # export results to CSV
"""

import os
import sys
import argparse
import uuid
from datetime import datetime
from dotenv import load_dotenv

# ── Argument parsing ──────────────────────────────────────────
parser = argparse.ArgumentParser(description="FieldOps DQ external client")
parser.add_argument("--table",      default=None,  help="Filter to specific table name")
parser.add_argument("--export-csv", action="store_true", help="Export results to CSV")
parser.add_argument("--catalog",    default=None,  help="Override catalog name")
parser.add_argument("--community",  action="store_true", help="Community Edition (no catalog)")
args = parser.parse_args()

# ── Load environment ──────────────────────────────────────────
load_dotenv()

HOST      = os.getenv("DATABRICKS_HOST")
TOKEN     = os.getenv("DATABRICKS_TOKEN")
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
CATALOG   = args.catalog or os.getenv("DATABRICKS_CATALOG", "fieldops_dq")

if not HOST or not TOKEN or not HTTP_PATH:
    print("ERROR: Missing environment variables.")
    print("  Create a .env file with:")
    print("    DATABRICKS_HOST=https://your-workspace.azuredatabricks.net")
    print("    DATABRICKS_TOKEN=dapi...")
    print("    DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123")
    sys.exit(1)

def schema(s, t=""):
    """Fully qualified table name."""
    prefix = f"{CATALOG}.{s}" if not args.community else s
    return prefix if not t else f"{prefix}.{t}"

# ── Connection ────────────────────────────────────────────────
try:
    from databricks import sql as dbsql
except ImportError:
    print("ERROR: databricks-sql-connector not installed.")
    print("  Run: pip install databricks-sql-connector")
    sys.exit(1)

def get_conn():
    return dbsql.connect(
        server_hostname = HOST.replace("https://", ""),
        http_path       = HTTP_PATH,
        access_token    = TOKEN,
    )

def query(conn, sql_str, params=None):
    """Execute a query and return list of dicts."""
    with conn.cursor() as cur:
        cur.execute(sql_str)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

# ── DQ Check Queries ──────────────────────────────────────────
# Each query returns: total, failed.
# We use Databricks SQL syntax (compatible with Spark SQL / Delta).

CHECKS = [
    # ── COMPLETENESS ──────────────────────────────────────────
    ("C-001","Completeness","raw.customers","CRITICAL","customer_name NULL or blank",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN customer_name IS NULL OR TRIM(customer_name)='' THEN 1 ELSE 0 END) AS failed FROM {schema('raw','customers')}"),
    ("C-002","Completeness","raw.customers","CRITICAL","account_status NULL",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN account_status IS NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','customers')}"),
    ("C-003","Completeness","raw.work_orders","CRITICAL","WO missing site_id or customer_id",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN site_id IS NULL OR customer_id IS NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','work_orders')}"),
    ("C-005","Completeness","raw.technicians","HIGH","Technician missing employee_number",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN employee_number IS NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','technicians')}"),

    # ── UNIQUENESS ────────────────────────────────────────────
    ("U-001","Uniqueness","raw.customers","CRITICAL","Duplicate customer_id",
     f"SELECT COUNT(*) AS total, COUNT(*)-COUNT(DISTINCT customer_id) AS failed FROM {schema('raw','customers')}"),
    ("U-002","Uniqueness","raw.customers","HIGH","Duplicate email address",
     f"""SELECT COUNT(*) AS total,
        (SELECT COUNT(*) FROM (SELECT lower(email_address) e FROM {schema('raw','customers')} WHERE email_address IS NOT NULL GROUP BY lower(email_address) HAVING COUNT(*) > 1)) AS failed
        FROM {schema('raw','customers')}"""),
    ("U-003","Uniqueness","raw.work_orders","CRITICAL","Duplicate work_order_id",
     f"SELECT COUNT(*) AS total, COUNT(*)-COUNT(DISTINCT work_order_id) AS failed FROM {schema('raw','work_orders')}"),
    ("U-004","Uniqueness","raw.invoices","HIGH","Multiple invoices per work order",
     f"""SELECT COUNT(*) AS total,
        (SELECT COALESCE(SUM(cnt-1),0) FROM (SELECT work_order_id, COUNT(*) cnt FROM {schema('raw','invoices')} GROUP BY work_order_id HAVING COUNT(*)>1) x) AS failed
        FROM {schema('raw','invoices')}"""),

    # ── VALIDITY ──────────────────────────────────────────────
    ("V-001","Validity","raw.customers","CRITICAL","account_status invalid domain",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN UPPER(TRIM(account_status)) NOT IN ('ACTIVE','INACTIVE','SUSPENDED','PENDING') OR account_status IS NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','customers')}"),
    ("V-003","Validity","raw.customers","HIGH","Negative credit_limit",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN TRY_CAST(credit_limit AS DOUBLE) < 0 THEN 1 ELSE 0 END) AS failed FROM {schema('raw','customers')}"),
    ("V-006","Validity","raw.work_orders","HIGH","Negative cost values on WO",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN TRY_CAST(labor_cost AS DOUBLE)<0 OR TRY_CAST(parts_cost AS DOUBLE)<0 THEN 1 ELSE 0 END) AS failed FROM {schema('raw','work_orders')}"),
    ("V-007","Validity","raw.invoices","CRITICAL","Negative financial amounts",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN TRY_CAST(subtotal AS DOUBLE)<0 OR TRY_CAST(total_amount AS DOUBLE)<0 THEN 1 ELSE 0 END) AS failed FROM {schema('raw','invoices')}"),
    ("V-010","Validity","raw.technicians","HIGH","Negative hourly_rate",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN TRY_CAST(hourly_rate AS DOUBLE)<0 THEN 1 ELSE 0 END) AS failed FROM {schema('raw','technicians')}"),

    # ── REFERENTIAL INTEGRITY ─────────────────────────────────
    ("R-001","Referential Integrity","raw.work_orders","CRITICAL","WO orphaned customer_id",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN c.customer_id IS NULL AND wo.customer_id IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','work_orders')} wo
        LEFT JOIN {schema('raw','customers')} c ON wo.customer_id = c.customer_id"""),
    ("R-002","Referential Integrity","raw.invoices","CRITICAL","Invoice orphaned work_order_id",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN wo.work_order_id IS NULL AND i.work_order_id IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','invoices')} i
        LEFT JOIN {schema('raw','work_orders')} wo ON i.work_order_id = wo.work_order_id"""),
    ("R-003","Referential Integrity","raw.work_order_assignments","HIGH","Assignment orphaned work_order_id",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN wo.work_order_id IS NULL AND a.work_order_id IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','work_order_assignments')} a
        LEFT JOIN {schema('raw','work_orders')} wo ON a.work_order_id = wo.work_order_id"""),
    ("R-004","Referential Integrity","raw.work_order_assignments","HIGH","Assignment orphaned technician_id",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN t.technician_id IS NULL AND a.technician_id IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','work_order_assignments')} a
        LEFT JOIN {schema('raw','technicians')} t ON a.technician_id = t.technician_id"""),

    # ── CONSISTENCY ───────────────────────────────────────────
    ("CS-001","Consistency","raw.work_orders","CRITICAL","actual_end before actual_start",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN TRY_CAST(actual_end AS TIMESTAMP) < TRY_CAST(actual_start AS TIMESTAMP)
                      AND actual_start IS NOT NULL AND actual_end IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','work_orders')}"""),
    ("CS-004","Consistency","raw.work_orders","HIGH","total_cost does not balance",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN ABS((TRY_CAST(labor_cost AS DOUBLE)+TRY_CAST(parts_cost AS DOUBLE))-TRY_CAST(total_cost AS DOUBLE)) > 0.01
                      AND labor_cost IS NOT NULL AND parts_cost IS NOT NULL AND total_cost IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','work_orders')}"""),
    ("CS-005","Consistency","raw.invoices","CRITICAL","Invoice total does not balance",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN ABS((TRY_CAST(subtotal AS DOUBLE)+TRY_CAST(tax_amount AS DOUBLE)-TRY_CAST(discount_amount AS DOUBLE))-TRY_CAST(total_amount AS DOUBLE)) > 0.01
                      AND subtotal IS NOT NULL AND tax_amount IS NOT NULL AND discount_amount IS NOT NULL AND total_amount IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','invoices')}"""),
    ("CS-009","Consistency","raw.invoices","HIGH","due_date before invoice_date",
     f"""SELECT COUNT(*) AS total,
        SUM(CASE WHEN TRY_CAST(due_date AS DATE) < TRY_CAST(invoice_date AS DATE)
                      AND due_date IS NOT NULL AND invoice_date IS NOT NULL THEN 1 ELSE 0 END) AS failed
        FROM {schema('raw','invoices')}"""),

    # ── TIMELINESS ────────────────────────────────────────────
    ("T-001","Timeliness","raw.customers","HIGH","customer created_date in the future",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN TRY_CAST(created_date AS DATE) > CURRENT_DATE AND created_date IS NOT NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','customers')}"),
    ("T-002","Timeliness","raw.technicians","HIGH","hire_date in the future",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN TRY_CAST(hire_date AS DATE) > CURRENT_DATE AND hire_date IS NOT NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','technicians')}"),
    ("T-003","Timeliness","raw.work_orders","CRITICAL","Completed WO has future scheduled_date",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN UPPER(TRIM(status)) IN ('COMPLETED','COMPLETE') AND TRY_CAST(scheduled_date AS DATE) > CURRENT_DATE THEN 1 ELSE 0 END) AS failed FROM {schema('raw','work_orders')}"),
    ("T-005","Timeliness","raw.work_orders","HIGH","IN_PROGRESS WO stale > 90 days",
     f"SELECT COUNT(*) AS total, SUM(CASE WHEN UPPER(TRIM(status))='IN_PROGRESS' AND DATEDIFF(CURRENT_DATE, TRY_CAST(last_modified AS DATE)) > 90 AND last_modified IS NOT NULL THEN 1 ELSE 0 END) AS failed FROM {schema('raw','work_orders')}"),
]

# ── Scoring ───────────────────────────────────────────────────
WEIGHT = {"CRITICAL": 30, "HIGH": 20, "MEDIUM": 10, "LOW": 5}

def score(rule_results):
    max_w  = sum(WEIGHT.get(r["severity"], 5) for r in rule_results)
    deduct = sum(WEIGHT.get(r["severity"], 5) * (r["failed"] / max(r["total"], 1))
                 for r in rule_results if r["failed"] > 0)
    return round(max(0, 100 - deduct / max_w * 100), 1) if max_w else 0.0

# ── Main ──────────────────────────────────────────────────────
def main():
    run_id = f"EXT-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:6].upper()}"
    print(f"\n{'='*60}")
    print(f"  FieldOps Pro — External DQ Check")
    print(f"  Run ID:    {run_id}")
    print(f"  Workspace: {HOST}")
    print(f"  Catalog:   {CATALOG if not args.community else '(Community Edition)'}")
    print(f"{'='*60}\n")

    try:
        conn = get_conn()
        print("  [✓] Connected to Databricks SQL\n")
    except Exception as e:
        print(f"  [✗] Connection failed: {e}")
        sys.exit(1)

    # Filter checks if --table specified
    checks_to_run = CHECKS
    if args.table:
        checks_to_run = [c for c in CHECKS if args.table in c[2]]
        print(f"  Filtering to table: {args.table} ({len(checks_to_run)} checks)\n")

    rule_results = []
    for rule_code, dimension, table, severity, desc, sql in checks_to_run:
        try:
            rows = query(conn, sql)
            total  = rows[0]["total"] or 0
            failed = rows[0]["failed"] or 0
            rate   = round(100 * (total - failed) / max(total, 1), 1)
            status = "PASS" if failed == 0 else ("FAIL" if severity in ("CRITICAL","HIGH") else "WARN")
            icon   = "✓" if status == "PASS" else "✗"
            print(f"  [{icon}] {rule_code:<10} {severity:<10} {status:<6}  {failed:>4}/{total:<6} {desc}")
            rule_results.append({"rule_code": rule_code, "dimension": dimension, "table": table,
                                  "severity": severity, "description": desc,
                                  "total": total, "failed": failed, "status": status})
        except Exception as e:
            print(f"  [!] {rule_code} ERROR: {e}")

    # ── Score report ──────────────────────────────────────────
    overall = score(rule_results)
    dims    = {}
    for r in rule_results:
        dims.setdefault(r["dimension"], []).append(r)

    print(f"\n{'='*60}")
    print(f"  OVERALL DQ SCORE: {overall:>5.1f} / 100")
    grade = "A" if overall >= 90 else "B" if overall >= 75 else "C" if overall >= 60 else "D" if overall >= 50 else "F"
    print(f"  GRADE:            {grade}")
    print(f"{'='*60}")
    for dim, rs in dims.items():
        sc  = score(rs)
        bar = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
        print(f"  {dim:<22} [{bar}] {sc:>5.1f}%")

    failures = [r for r in rule_results if r["status"] in ("FAIL","WARN")]
    if failures:
        print(f"\n  Failed checks ({len(failures)}):")
        for r in sorted(failures, key=lambda x: WEIGHT.get(x["severity"], 0), reverse=True):
            print(f"    {r['rule_code']:<10} {r['severity']:<10} {r['failed']:>4} failures  — {r['description']}")

    # ── Query curated scores for trending ────────────────────
    print("\n  Recent DQ Score History (from dq.dq_score_summary):")
    try:
        history = query(conn, f"""
            SELECT score_date, overall_score, completeness_score, validity_score,
                   critical_failures, high_failures
            FROM {schema('dq','dq_score_summary')}
            ORDER BY score_date DESC
            LIMIT 5
        """)
        for h in history:
            print(f"    {h['score_date']}  overall={h['overall_score']:.1f}  "
                  f"completeness={h['completeness_score']:.1f}  "
                  f"critical_fails={h['critical_failures']}")
    except Exception as e:
        print(f"    (score history not available: {e})")

    # ── Export to CSV ─────────────────────────────────────────
    if args.export_csv:
        import csv
        filename = f"dq_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["rule_code","dimension","table","severity","description","total","failed","status"])
            writer.writeheader()
            writer.writerows(rule_results)
        print(f"\n  Results exported to: {filename}")

    conn.close()
    print(f"\n  Done. Run ID: {run_id}\n")

if __name__ == "__main__":
    main()
