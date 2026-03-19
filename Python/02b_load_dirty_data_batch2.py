# Databricks Notebook — 02b_LOAD_DIRTY_DATA_BATCH2
# FieldOps Pro | Second extract — BATCH-20240111
# ─────────────────────────────────────────────────────────────
# Run AFTER 02_load_dirty_data.py.
# This notebook APPENDs to existing raw tables so both batches
# are present together, simulating a real incremental load.
#
# NEW problems introduced in this batch:
#   Customers   : mixed-encoding artifacts, city/state mismatch,
#                 overlapping active records, credit_limit as
#                 scientific notation, same name diff email/phone
#   Technicians : region_code not in allowed domain, email domain
#                 mismatch with employee record, manager_id
#                 pointing to non-existent manager, skill_level
#                 typo, rate stored with currency symbol
#   Work Orders : priority/type cross-contamination, WO linked to
#                 INACTIVE customer, description is only whitespace,
#                 parts_cost > total_cost, scheduled < created,
#                 two techs both flagged LEAD on same WO
#   Invoices    : tax_rate impossible (>100%), VOID invoice with
#                 payment_received, invoice for CANCELLED WO,
#                 payment_date before invoice_date, same invoice_number
#                 on two different invoice_ids
#   Assignments : start_time in the future for COMPLETED assignment,
#                 hours_logged = 0 on COMPLETED, both techs LEAD same WO,
#                 assignment for ON_HOLD WO marked ACTIVE,
#                 scheduled 30 days after WO scheduled_date
#   Parts       : quantity_used = 0, unit_cost = 0 on charged part,
#                 total_cost ≠ qty × unit_cost, orphaned WO,
#                 part_number NULL, duplicate part line on same WO
# ─────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------

COMMUNITY_EDITION = False
CATALOG  = "fieldops_dq"
RAW      = "raw"
BATCH_ID = "BATCH-20240111"   # ← second day extract

def t(table):
    return f"{CATALOG}.{RAW}.{table}" if not COMMUNITY_EDITION else f"{RAW}.{table}"

from pyspark.sql import Row
from datetime import datetime
now = datetime.utcnow()

# ── helper: append to raw table ──────────────────────────────
def append(df, table_name):
    df.write.format("delta").mode("append").saveAsTable(t(table_name))
    print(f"  Appended {df.count():>4} rows → {t(table_name)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customers — 30 new records (CUST-056 … CUST-085)
# MAGIC NEW problems: encoding artifacts, city/state mismatch,
# MAGIC overlapping duplicate keys from different extract windows,
# MAGIC scientific-notation credit limit, same name / different contacts,
# MAGIC invisible characters in email, credit_limit with currency symbol

# COMMAND ----------

customers = [
    # ── GOOD new records ─────────────────────────────────────
    ("CUST-056","Redwood Basin Authority",       "GOVERNMENT","ACTIVE",   "340000.00","Howard Kline",     "hkline@redwoodbasin.gov",      "(916) 555-5601","Sacramento",   "CA","95815","NW","2020-06-14","2024-01-11 08:00:00","admin",         "FIELDOPS_APP"),
    ("CUST-057","Iron Range Power LLC",          "ENTERPRISE","ACTIVE",   "290000.00","Alicia Drummond",  "adrummond@ironrangepower.com", "(218) 555-5701","Duluth",       "MN","55801","MW","2019-02-09","2024-01-11 08:30:00","admin",         "FIELDOPS_APP"),
    ("CUST-058","Copper Basin Utilities",        "ENTERPRISE","ACTIVE",   "470000.00","Lionel Graves",    "lgraves@copperbasinu.com",     "(928) 555-5801","Tucson",       "AZ","85702","SW","2016-10-12","2024-01-11 09:00:00","admin",         "FIELDOPS_APP"),
    ("CUST-059","Blue Mesa Water Authority",     "GOVERNMENT","ACTIVE",   "195000.00","Celia Okonkwo",    "cokonkwo@bluemesawa.gov",      "(970) 555-5901","Grand Junction","CO","81502","CW","2018-03-27","2024-01-11 09:30:00","admin",         "FIELDOPS_APP"),
    ("CUST-060","Emerald Coast Power",           "ENTERPRISE","ACTIVE",   "410000.00","Derek Fontaine",   "dfontaine@emeraldcpwr.com",    "(850) 555-6001","Pensacola",    "FL","32501","SE","2017-07-19","2024-01-11 10:00:00","admin",         "FIELDOPS_APP"),
    ("CUST-061","Lakeshore Municipal Electric",  "GOVERNMENT","INACTIVE", "120000.00","Paula Brennan",    "pbrennan@lakeshoremelec.gov",  "(414) 555-6101","Milwaukee",    "WI","53203","MW","2015-11-05","2024-01-11 10:30:00","admin",         "FIELDOPS_APP"),
    ("CUST-062","Saguaro Energy Partners",       "ENTERPRISE","ACTIVE",   "560000.00","Renaldo Vance",    "rvance@saguaroenergy.com",     "(480) 555-6201","Scottsdale",   "AZ","85251","SW","2014-05-22","2024-01-11 11:00:00","admin",         "FIELDOPS_APP"),
    ("CUST-063","Tundra Gas & Pipeline",         "ENTERPRISE","ACTIVE",   "830000.00","Freya Olsen",      "folsen@tundragp.com",          "(907) 555-6301","Fairbanks",    "AK","99701","NW","2013-12-01","2024-01-11 11:30:00","admin",         "FIELDOPS_APP"),
    ("CUST-064","Cardinal Power Cooperative",    "GOVERNMENT","ACTIVE",   "265000.00","Jabari Mensah",    "jmensah@cardinalcoop.gov",     "(614) 555-6401","Columbus",     "OH","43215","MW","2018-08-14","2024-01-11 12:00:00","admin",         "FIELDOPS_APP"),
    ("CUST-065","High Plains Energy",            "ENTERPRISE","ACTIVE",   "315000.00","Sylvia Cortez",    "scortez@highplainsenergy.com", "(806) 555-6501","Amarillo",     "TX","79101","SW","2020-01-28","2024-01-11 12:30:00","admin",         "FIELDOPS_APP"),

    # ── PROBLEM: Encoding artifact — mojibake in customer_name ──────────────────
    # Simulates UTF-8 data stored in Latin-1 then re-read incorrectly
    ("CUST-066","Valle de EnergÃ­a SA",          "ENTERPRISE","ACTIVE",   "180000.00","Juan MartÃ­nez",   "jmartinez@valleenergia.com",   "(210) 555-6601","San Antonio",  "TX","78201","SW","2022-04-15","2024-01-11 08:00:00","legacy_import",  "LEGACY_DB"),
    ("CUST-067","RÃ©seau Ã‰lectrique Nord",       "ENTERPRISE","ACTIVE",   "225000.00","Ã‰milie Tremblay", "etremblay@reseaunord.ca",      "(514) 555-6701","Montreal",     "QC","H2X1Y7","NE","2021-09-01","2024-01-11 08:15:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: City/state mismatch (Miami is not in TX) ───────────────────────
    ("CUST-068","Suncoast Power District",       "GOVERNMENT","ACTIVE",   "310000.00","Darius Osei",      "dosei@suncoastpd.gov",         "(305) 555-6801","Miami",        "TX","33101","SW","2019-06-20","2024-01-11 08:30:00","admin",          "FIELDOPS_APP"),
    # Portland is not in FL
    ("CUST-069","Cascade Falls Energy",          "ENTERPRISE","ACTIVE",   "270000.00","Helena Ward",      "hward@cascadefalls.com",       "(503) 555-6901","Portland",     "FL","97202","SE","2020-11-11","2024-01-11 08:45:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: Credit limit stored with currency symbol / text ────────────────
    ("CUST-070","Lava Flow Geothermal",          "ENTERPRISE","ACTIVE",   "$450000",  "Kalani Akana",     "kakana@lavaflowgeo.com",       "(808) 555-7001","Hilo",         "HI","96720","NW","2021-03-14","2024-01-11 09:00:00","legacy_import",  "LEGACY_DB"),
    ("CUST-071","Prairie Wind Energy Coop",      "GOVERNMENT","ACTIVE",   "USD125000","Harold Yost",      "hyost@prairiewindcoop.gov",    "(605) 555-7101","Pierre",       "SD","57501","MW","2020-07-07","2024-01-11 09:15:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: Credit limit in scientific notation (technically parseable, suspicious) ──
    ("CUST-072","Nexus Grid Infrastructure",     "ENTERPRISE","ACTIVE",   "2.5e5",    "Tobias Lehmann",   "tlehmann@nexusgrid.com",       "(312) 555-7201","Chicago",      "IL","60602","MW","2023-01-05","2024-01-11 09:30:00","api_feed",       "EXTERNAL_API"),

    # ── PROBLEM: Same name, different email/phone — ambiguous fuzzy duplicate ────
    ("CUST-073","Blue Ridge Telecom",            "ENTERPRISE","ACTIVE",   "190000.00","Larry Stone",      "lstone.work@blueridgetelecom.com","(704) 555-9999","Charlotte",  "NC","28202","SE","2021-04-22","2024-01-11 09:45:00","mobile_sync",   "MOBILE_APP"),
    # Note: CUST-017 already has lstone@blueridgetelecom.com — close enough to be suspicious

    # ── PROBLEM: Invisible/zero-width character in email (hard to catch) ─────────
    # The email below has a zero-width space after "apex"
    ("CUST-074","Apex Power Corporation",        "ENTERPRISE","ACTIVE",   "250000.00","Sandra Okafor",    "sokafor@apex\u200bpower.com",  "(512) 555-0101","Austin",       "TX","78701","SW","2019-03-15","2024-01-11 10:00:00","api_feed",       "EXTERNAL_API"),

    # ── PROBLEM: account_status is a number (data type confusion) ───────────────
    ("CUST-075","Granite Ridge Power",           "SMB",       "1",        "45000.00", "Wade Simmons",     "wsimmons@graniteridge.com",    "(801) 555-7501","Provo",        "UT","84601","NW","2023-03-20","2024-01-11 10:15:00","api_feed",       "EXTERNAL_API"),

    # ── PROBLEM: region_code that doesn't match billing_state ───────────────────
    # Houston TX should be SW, not NE
    ("CUST-076","Bayfront Energy Services",      "ENTERPRISE","ACTIVE",   "230000.00","Carmen Rivera",    "crivera@bayfront-es.com",      "(713) 555-7601","Houston",      "TX","77003","NE","2020-09-18","2024-01-11 10:30:00","admin",          "FIELDOPS_APP"),
    # Atlanta GA should be SE, not MW
    ("CUST-077","Southern Grid Alliance",        "GOVERNMENT","ACTIVE",   "175000.00","Elton Briggs",     "ebriggs@southerngrid.gov",     "(404) 555-7701","Atlanta",      "GA","30302","MW","2019-05-05","2024-01-11 10:45:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: billing_zip too long / wrong format ─────────────────────────────
    ("CUST-078","Delta Grid Partners",           "ENTERPRISE","ACTIVE",   "185000.00","Rosa Chambers",    "rchambers@deltagrid.com",      "(601) 555-7801","Jackson",      "MS","39201-4872","SE","2022-02-14","2024-01-11 11:00:00","admin",        "FIELDOPS_APP"),
    # 9-digit zip is valid but our system only handles 5 — tests trimming
    ("CUST-079","Sundance Transmission",         "ENTERPRISE","ACTIVE",   "220000.00","Travis Holbrook",  "tholbrook@sundancetx.com",     "(307) 555-7901","Cheyenne",     "WY","82001-1234","CW","2021-06-30","2024-01-11 11:15:00","admin",        "FIELDOPS_APP"),

    # ── PROBLEM: created_date is valid but last_modified is BEFORE created_date ──
    ("CUST-080","Overlook Energy Systems",       "SMB",       "ACTIVE",   "62000.00", "Miriam Schultz",   "mschultz@overlookergy.com",    "(937) 555-8001","Dayton",       "OH","45401","MW","2023-08-15","2022-01-01 09:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: Exact re-extract of CUST-001 in new batch (delta duplicate) ────
    ("CUST-001","Apex Power Corporation",        "ENTERPRISE","ACTIVE",   "250000.00","Sandra Okafor",    "sokafor@apexpower.com",        "(512) 555-0101","Austin",       "TX","78701","SW","2019-03-15","2024-01-11 08:30:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: First name only in primary_contact field ───────────────────────
    ("CUST-081","Snowpack Hydro Authority",      "GOVERNMENT","ACTIVE",   "145000.00","Bob",               "bsmith@snowpackhydro.gov",     "(208) 555-8101","Boise",        "ID","83701","NW","2021-12-01","2024-01-11 11:45:00","mobile_sync",   "MOBILE_APP"),

    # ── PROBLEM: email is valid format but clearly a test/placeholder ──────────
    ("CUST-082","Coastal Surge Power",           "ENTERPRISE","ACTIVE",   "355000.00","Test User",        "test@test.com",                "(000) 000-0000","Tampa",        "FL","33601","SE","2024-01-10","2024-01-11 12:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: customer_name is all numbers ────────────────────────────────────
    ("CUST-083","123456",                        "SMB",       "ACTIVE",   "28000.00", "Tom Ford",         "tford@company123.com",         "(555) 555-8301","Phoenix",      "AZ","85002","SW","2023-11-20","2024-01-11 12:15:00","mobile_sync",   "MOBILE_APP"),

    # ── PROBLEM: Credit limit astronomically large (possible data entry error) ───
    ("CUST-084","Titan Power Networks",          "ENTERPRISE","ACTIVE",   "99999999999.00","Victor Marsh","vmarsh@titanpower.com",        "(212) 555-8401","New York",     "NY","10002","NE","2018-09-01","2024-01-11 12:30:00","legacy_import", "LEGACY_DB"),

    # ── PROBLEM: status value is empty string instead of NULL ───────────────────
    ("CUST-085","Willow Creek Utilities",        "SMB",       "",         "35000.00", "Lisa Park",        "lpark@willowcreekutil.com",    "(651) 555-8501","St. Paul",     "MN","55101","MW","2023-06-10","2024-01-11 12:45:00","mobile_sync",   "MOBILE_APP"),
]

customer_cols = [
    "customer_id","customer_name","customer_type","account_status","credit_limit",
    "primary_contact","email_address","phone_number","billing_city","billing_state",
    "billing_zip","region_code","created_date","last_modified","created_by","source_system"
]
df_cust2 = spark.createDataFrame(
    [Row(**dict(zip(customer_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in customers]
)
append(df_cust2, "customers")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Technicians — 20 new records (TECH-031 … TECH-050)
# MAGIC NEW problems: invalid region_code, email domain mismatch,
# MAGIC non-existent manager_id, skill_level typo, rate with currency symbol,
# MAGIC duplicate full_name (different ID), NULL home_region,
# MAGIC hourly_rate stored as range string

# COMMAND ----------

technicians = [
    # ── GOOD new records ─────────────────────────────────────
    ("TECH-031","EMP-10074","Dani",    "Reyes",    "Dani Reyes",      "ACTIVE",    "JUNIOR","SW","2023-05-01",None,         "40.00", "MGR-001","dreyes@meridian.com",     "(210) 555-4031","2023-05-01","2024-01-11 08:00:00"),
    ("TECH-032","EMP-10075","Kobi",    "Asher",    "Kobi Asher",      "ACTIVE",    "SENIOR","NE","2014-02-10",None,         "74.00", "MGR-005","kasher@meridian.com",     "(617) 555-4032","2014-02-10","2024-01-11 08:15:00"),
    ("TECH-033","EMP-10076","Minh",    "Tran",     "Minh Tran",       "ACTIVE",    "MID",   "NW","2019-06-25",None,         "57.00", "MGR-002","mtran@meridian.com",      "(503) 555-4033","2019-06-25","2024-01-11 08:30:00"),
    ("TECH-034","EMP-10077","Adaeze",  "Eze",      "Adaeze Eze",      "ACTIVE",    "SENIOR","SE","2015-10-17",None,         "70.00", "MGR-004","aeze@meridian.com",       "(813) 555-4034","2015-10-17","2024-01-11 08:45:00"),
    ("TECH-035","EMP-10078","Lorenzo", "Bianchi",  "Lorenzo Bianchi", "ACTIVE",    "MID",   "NE","2020-03-03",None,         "55.00", "MGR-005","lbianchi@meridian.com",   "(212) 555-4035","2020-03-03","2024-01-11 09:00:00"),
    ("TECH-036","EMP-10079","Nadia",   "Volkov",   "Nadia Volkov",    "ACTIVE",    "JUNIOR","MW","2022-07-11",None,         "42.00", "MGR-003","nvolkov@meridian.com",    "(312) 555-4036","2022-07-11","2024-01-11 09:15:00"),

    # ── PROBLEM: region_code not in allowed domain ─────────────────────────────
    ("TECH-037","EMP-10080","Omar",    "Khalid",   "Omar Khalid",     "ACTIVE",    "SENIOR","SOUTHWEST","2016-04-12",None,  "68.00", "MGR-001","okhalid@meridian.com",    "(602) 555-4037","2016-04-12","2024-01-11 09:30:00"),
    ("TECH-038","EMP-10081","Sofia",   "Reyes",    "Sofia Reyes",     "ACTIVE",    "MID",   "SOUTH",  "2021-08-20",None,   "53.00", "MGR-004","sreyes@meridian.com",     "(404) 555-4038","2021-08-20","2024-01-11 09:45:00"),

    # ── PROBLEM: email domain doesn't match company domain (personal email) ──────
    ("TECH-039","EMP-10082","Pete",    "Holman",   "Pete Holman",     "ACTIVE",    "MID",   "CW","2020-10-01",None,         "51.00", "MGR-006","pete.holman@gmail.com",   "(720) 555-4039","2020-10-01","2024-01-11 10:00:00"),
    ("TECH-040","EMP-10083","Grace",   "Oduya",    "Grace Oduya",     "ACTIVE",    "JUNIOR","SE","2023-01-09",None,         "39.00", "MGR-004","goduya@yahoo.com",        "(404) 555-4040","2023-01-09","2024-01-11 10:15:00"),

    # ── PROBLEM: manager_id points to non-existent manager ──────────────────────
    ("TECH-041","EMP-10084","Femi",    "Adebayo",  "Femi Adebayo",    "ACTIVE",    "SENIOR","SW","2017-03-14",None,         "67.00", "MGR-999","fadebayo@meridian.com",   "(602) 555-4041","2017-03-14","2024-01-11 10:30:00"),
    ("TECH-042","EMP-10085","Hana",    "Takahashi","Hana Takahashi",  "ACTIVE",    "MID",   "NW","2019-09-22",None,         "56.00", "MGR-888","htakahashi@meridian.com", "(206) 555-4042","2019-09-22","2024-01-11 10:45:00"),

    # ── PROBLEM: skill_level typo / not in domain ────────────────────────────────
    ("TECH-043","EMP-10086","Caleb",   "Adkins",   "Caleb Adkins",    "ACTIVE",    "SENOIR","NE","2015-07-07",None,         "71.00", "MGR-005","cadkins@meridian.com",    "(617) 555-4043","2015-07-07","2024-01-11 11:00:00"),
    ("TECH-044","EMP-10087","Nia",     "Freeman",  "Nia Freeman",     "ACTIVE",    "Mid-Level","MW","2021-12-15",None,      "55.00", "MGR-003","nfreeman@meridian.com",   "(312) 555-4044","2021-12-15","2024-01-11 11:15:00"),

    # ── PROBLEM: hourly_rate stored as range string ──────────────────────────────
    ("TECH-045","EMP-10088","Bruno",   "Souza",    "Bruno Souza",     "ACTIVE",    "MID",   "SW","2020-05-18",None,         "50-60", "MGR-001","bsouza@meridian.com",     "(210) 555-4045","2020-05-18","2024-01-11 11:30:00"),

    # ── PROBLEM: hourly_rate with currency symbol ─────────────────────────────────
    ("TECH-046","EMP-10089","Kim",     "Ngo",      "Kim Ngo",         "ACTIVE",    "JUNIOR","NW","2023-04-01",None,         "$41.00","MGR-002","kngo@meridian.com",       "(503) 555-4046","2023-04-01","2024-01-11 11:45:00"),

    # ── PROBLEM: Duplicate full_name as another active technician (TECH-001) ────
    ("TECH-047","EMP-10090","Carlos",  "Mendez",   "Carlos Mendez",   "ACTIVE",    "JUNIOR","SE","2023-08-01",None,         "40.00", "MGR-004","cmendez2@meridian.com",   "(813) 555-4047","2023-08-01","2024-01-11 12:00:00"),

    # ── PROBLEM: NULL home_region ────────────────────────────────────────────────
    ("TECH-048","EMP-10091","Aiko",    "Yamada",   "Aiko Yamada",     "ACTIVE",    "SENIOR",None,"2016-11-14",None,         "69.00", "MGR-002","ayamada@meridian.com",    "(503) 555-4048","2016-11-14","2024-01-11 12:15:00"),

    # ── PROBLEM: SUSPENDED status — valid domain but no notes/reason stored ─────
    ("TECH-049","EMP-10092","Max",     "Brandt",   "Max Brandt",      "SUSPENDED", "MID",   "CW","2018-06-01",None,         "52.00", "MGR-006","mbrandt@meridian.com",    "(720) 555-4049","2018-06-01","2024-01-11 12:30:00"),

    # ── PROBLEM: both first_name and last_name are NULL ──────────────────────────
    ("TECH-050","EMP-10093",None,      None,       "Unknown Tech",    "ACTIVE",    "JUNIOR","NE","2024-01-01",None,         "38.00", "MGR-005","unknown@meridian.com",    "(617) 555-4050","2024-01-01","2024-01-11 12:45:00"),
]

tech_cols = [
    "technician_id","employee_number","first_name","last_name","full_name",
    "employment_status","skill_level","home_region","hire_date","termination_date",
    "hourly_rate","manager_id","email","mobile_phone","created_date","last_modified"
]
df_tech2 = spark.createDataFrame(
    [Row(**dict(zip(tech_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in technicians]
)
append(df_tech2, "technicians")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Work Orders — 30 new records (WO-10051 … WO-10080)
# MAGIC NEW problems: WO for INACTIVE/SUSPENDED customer, description whitespace,
# MAGIC parts_cost > total_cost, scheduled_date < created_date,
# MAGIC CANCELLED WO with invoice payment received, priority = type value,
# MAGIC both techs marked LEAD (covered in assignments), multi-assign contradiction

# COMMAND ----------

work_orders = [
    # ── GOOD new records ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    ("WO-10051","WO-2024-10051","SITE-051","CUST-056","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Annual relay testing — substation 4",             "2024-01-06","2024-01-06 08:00:00","2024-01-06 12:00:00","4.0", "4.0", "260.00","85.00",  "345.00",  "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 12:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10052","WO-2024-10052","SITE-052","CUST-057","INSPECTION",            "LOW",    "COMPLETED",  "Surge arrester condition assessment",             "2024-01-07","2024-01-07 09:00:00","2024-01-07 11:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 11:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10053","WO-2024-10053","SITE-053","CUST-058","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Replace failed contactor in MCC panel",           "2024-01-08","2024-01-08 07:00:00","2024-01-08 13:00:00","6.0", "6.5", "422.50","370.00", "792.50",  "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 13:30:00","dispatch03","FIELDOPS_APP"),
    ("WO-10054","WO-2024-10054","SITE-054","CUST-059","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Pump impeller wear inspection",                   "2024-01-09","2024-01-09 10:00:00","2024-01-09 13:00:00","3.0", "3.0", "195.00","0.00",   "195.00",  "2024-01-12 17:00:00","Y","RESOLVED_COMPLETE","2024-01-07","2024-01-09 13:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10055","WO-2024-10055","SITE-055","CUST-060","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Transformer oil spill — emergency containment",   "2024-01-10","2024-01-10 03:00:00","2024-01-10 11:00:00","8.0", "8.0", "520.00","1200.00","1720.00", "2024-01-10 09:00:00","N","RESOLVED_PARTIAL", "2024-01-10","2024-01-10 11:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10056","WO-2024-10056","SITE-056","CUST-062","INSPECTION",            "MEDIUM", "COMPLETED",  "Lightning arrester bank inspection",              "2024-01-05","2024-01-05 08:00:00","2024-01-05 10:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 10:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10057","WO-2024-10057","SITE-057","CUST-063","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Gas regulator seat replacement",                  "2024-01-06","2024-01-06 09:00:00","2024-01-06 15:00:00","6.0", "6.0", "390.00","840.00", "1230.00", "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 15:30:00","dispatch03","FIELDOPS_APP"),
    ("WO-10058","WO-2024-10058","SITE-058","CUST-064","PREVENTIVE_MAINTENANCE","LOW",    "SCHEDULED",  "Annual relay coordination review",                "2024-01-22",None,                 None,                 "4.0", None,  None,    None,    None,      "2024-01-25 17:00:00",None,None,               "2024-01-11","2024-01-11 09:00:00","dispatch01","FIELDOPS_APP"),
    ("WO-10059","WO-2024-10059","SITE-059","CUST-065","CORRECTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Faulty recloser replacement — feeder 7",          "2024-01-08","2024-01-08 08:00:00","2024-01-08 14:00:00","6.0", "6.0", "390.00","720.00", "1110.00", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 14:30:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: WO linked to INACTIVE customer (valid FK, bad business rule) ────
    ("WO-10060","WO-2024-10060","SITE-060","CUST-024","PREVENTIVE_MAINTENANCE","LOW",    "COMPLETED",  "Annual valve inspection — inactive site",         "2024-01-05","2024-01-05 10:00:00","2024-01-05 12:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 12:30:00","dispatch03","FIELDOPS_APP"),
    # WO for SUSPENDED customer
    ("WO-10061","WO-2024-10061","SITE-061","CUST-030","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Pressure regulator fault — suspended account",    "2024-01-07","2024-01-07 08:00:00","2024-01-07 14:00:00","6.0", "6.0", "390.00","450.00", "840.00",  "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 14:30:00","dispatch01","FIELDOPS_APP"),

    # ── PROBLEM: description is whitespace only ──────────────────────────────────
    ("WO-10062","WO-2024-10062","SITE-062","CUST-056","INSPECTION",            "MEDIUM", "COMPLETED",  "   ",                                             "2024-01-06","2024-01-06 09:00:00","2024-01-06 11:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 11:30:00","mobile_user","MOBILE_APP"),

    # ── PROBLEM: parts_cost > total_cost (total can't be less than components) ───
    ("WO-10063","WO-2024-10063","SITE-063","CUST-057","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Switchboard busbar replacement",                  "2024-01-07","2024-01-07 07:00:00","2024-01-07 16:00:00","9.0", "9.0", "585.00","2800.00","2100.00", "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 16:30:00","dispatch02","FIELDOPS_APP"),
    # labor_cost > total_cost
    ("WO-10064","WO-2024-10064","SITE-064","CUST-058","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Motor winding resistance test",                   "2024-01-08","2024-01-08 09:00:00","2024-01-08 12:00:00","3.0", "3.0", "650.00","0.00",   "195.00",  "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 12:30:00","dispatch03","FIELDOPS_APP"),

    # ── PROBLEM: scheduled_date is BEFORE created_date ──────────────────────────
    ("WO-10065","WO-2024-10065","SITE-065","CUST-059","INSPECTION",            "LOW",    "SCHEDULED",  "Water quality sensor calibration",                "2024-01-05","2024-01-10 08:00:00",None,                 "2.0", None,  None,    None,    None,      "2024-01-13 17:00:00",None,None,               "2024-01-11","2024-01-11 09:00:00","dispatch01","FIELDOPS_APP"),

    # ── PROBLEM: CANCELLED WO — should have no costs / active assignments ────────
    ("WO-10066","WO-2024-10066","SITE-066","CUST-060","CORRECTIVE_MAINTENANCE","HIGH",   "CANCELLED",  "Emergency isolation — cancelled by customer",     "2024-01-09",None,                 None,                 "4.0", None,  "520.00","0.00",   "520.00",  "2024-01-12 17:00:00",None,None,               "2024-01-09","2024-01-09 14:00:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: priority value looks like a type value ──────────────────────────
    ("WO-10067","WO-2024-10067","SITE-067","CUST-062","PREVENTIVE_MAINTENANCE","INSPECTION","COMPLETED","Transformer tap changer inspection",            "2024-01-04","2024-01-04 08:00:00","2024-01-04 11:00:00","3.0", "3.0", "195.00","0.00",   "195.00",  "2024-01-07 17:00:00","Y","RESOLVED_COMPLETE","2024-01-02","2024-01-04 11:30:00","dispatch03","FIELDOPS_APP"),

    # ── PROBLEM: actual_hours = 0 on COMPLETED WO ───────────────────────────────
    ("WO-10068","WO-2024-10068","SITE-068","CUST-063","INSPECTION",            "LOW",    "COMPLETED",  "Instrument transformer calibration",              "2024-01-05","2024-01-05 09:00:00","2024-01-05 09:00:00","2.0", "0.0", "0.00",  "0.00",   "0.00",    "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 09:01:00","mobile_user","MOBILE_APP"),

    # ── PROBLEM: WO type INSTALLATION but status COMPLETED with no parts cost ────
    ("WO-10069","WO-2024-10069","SITE-069","CUST-064","INSTALLATION",         "HIGH",   "COMPLETED",  "New smart meter installation — batch 7",          "2024-01-06","2024-01-06 08:00:00","2024-01-06 16:00:00","8.0", "8.0", "520.00","0.00",   "520.00",  "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 16:30:00","dispatch01","FIELDOPS_APP"),

    # ── PROBLEM: IN_PROGRESS WO with actual_end already populated ────────────────
    ("WO-10070","WO-2024-10070","SITE-070","CUST-065","CORRECTIVE_MAINTENANCE","HIGH",   "IN_PROGRESS","Capacitor bank fault investigation",              "2024-01-09","2024-01-09 08:00:00","2024-01-09 15:00:00","6.0", "6.0", "390.00","0.00",   "390.00",  "2024-01-12 17:00:00",None,None,               "2024-01-08","2024-01-09 15:30:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: WO with NULL work_order_number ───────────────────────────────────
    ("WO-10071",None,            "SITE-071","CUST-056","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Panel board torque check",                        "2024-01-07","2024-01-07 09:00:00","2024-01-07 11:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 11:30:00","mobile_user","MOBILE_APP"),

    # ── PROBLEM: resolution_code populated but WO not yet COMPLETED ──────────────
    ("WO-10072","WO-2024-10072","SITE-072","CUST-057","INSPECTION",            "LOW",    "IN_PROGRESS","Boiler feedwater pump inspection",                "2024-01-10","2024-01-10 08:00:00",None,                 "3.0", None,  None,    None,    None,      "2024-01-13 17:00:00",None,"RESOLVED_COMPLETE","2024-01-09","2024-01-10 09:00:00","dispatch03","FIELDOPS_APP"),

    # ── Good additional volume ─────────────────────────────────────────────────────
    ("WO-10073","WO-2024-10073","SITE-073","CUST-058","PREVENTIVE_MAINTENANCE","LOW",    "COMPLETED",  "Drive belt and pulley inspection",                "2024-01-06","2024-01-06 10:00:00","2024-01-06 12:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 12:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10074","WO-2024-10074","SITE-074","CUST-059","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Solenoid valve replacement — line C",             "2024-01-08","2024-01-08 07:30:00","2024-01-08 13:30:00","6.0", "6.0", "390.00","280.00", "670.00",  "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 14:00:00","dispatch02","FIELDOPS_APP"),
    ("WO-10075","WO-2024-10075","SITE-075","CUST-060","INSPECTION",            "MEDIUM", "COMPLETED",  "Earthing system resistance check",                "2024-01-09","2024-01-09 09:00:00","2024-01-09 11:00:00","2.0", "2.0", "130.00","0.00",   "130.00",  "2024-01-12 17:00:00","Y","RESOLVED_COMPLETE","2024-01-07","2024-01-09 11:30:00","dispatch03","FIELDOPS_APP"),
    ("WO-10076","WO-2024-10076","SITE-076","CUST-062","PREVENTIVE_MAINTENANCE","LOW",    "COMPLETED",  "Insulation resistance test — MV cables",          "2024-01-05","2024-01-05 08:00:00","2024-01-05 11:00:00","3.0", "3.0", "195.00","0.00",   "195.00",  "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 11:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10077","WO-2024-10077","SITE-077","CUST-063","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Gas regulator relief valve stuck open",           "2024-01-10","2024-01-10 05:00:00","2024-01-10 12:00:00","7.0", "7.0", "455.00","690.00", "1145.00", "2024-01-10 10:00:00","N","RESOLVED_PARTIAL", "2024-01-10","2024-01-10 12:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10078","WO-2024-10078","SITE-078","CUST-064","INSPECTION",            "MEDIUM", "SCHEDULED",  "Quarterly metering equipment check",              "2024-01-20",None,                 None,                 "2.0", None,  None,    None,    None,      "2024-01-23 17:00:00",None,None,               "2024-01-11","2024-01-11 10:00:00","dispatch03","FIELDOPS_APP"),
    ("WO-10079","WO-2024-10079","SITE-079","CUST-065","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Bus differential relay trip — reset and test",    "2024-01-07","2024-01-07 06:00:00","2024-01-07 14:00:00","8.0", "8.5", "552.50","0.00",   "552.50",  "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 14:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10080","WO-2024-10080","SITE-080","CUST-056","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Vibration analysis — rotating machinery",         "2024-01-08","2024-01-08 08:00:00","2024-01-08 12:00:00","4.0", "4.0", "260.00","0.00",   "260.00",  "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 12:30:00","dispatch02","FIELDOPS_APP"),
]

wo_cols = [
    "work_order_id","work_order_number","site_id","customer_id","work_order_type",
    "priority","status","description","scheduled_date","actual_start","actual_end",
    "estimated_hours","actual_hours","labor_cost","parts_cost","total_cost",
    "sla_due_date","sla_met","resolution_code","created_date","last_modified",
    "created_by","source_system"
]
df_wo2 = spark.createDataFrame(
    [Row(**dict(zip(wo_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in work_orders]
)
append(df_wo2, "work_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Invoices — 25 new records (INV-5031 … INV-5055)
# MAGIC NEW problems: tax_rate > 100%, VOID with payment received,
# MAGIC invoice for CANCELLED WO, payment_date before invoice_date,
# MAGIC same invoice_number on two different IDs

# COMMAND ----------

invoices = [
    # ── GOOD new records ──────────────────────────────────────
    ("INV-5031","INV-2024-5031","WO-10051","CUST-056","2024-01-06","2024-02-05","PAID",    "345.00",  "27.60", "0.00",   "372.60",  "NET-30","372.60",  "2024-01-19","2024-01-06","2024-01-19 10:00:00","billing01"),
    ("INV-5032","INV-2024-5032","WO-10052","CUST-057","2024-01-07","2024-02-06","PAID",    "130.00",  "10.40", "0.00",   "140.40",  "NET-30","140.40",  "2024-01-20","2024-01-07","2024-01-20 09:00:00","billing02"),
    ("INV-5033","INV-2024-5033","WO-10053","CUST-058","2024-01-08","2024-02-07","SENT",    "792.50",  "63.40", "0.00",   "855.90",  "NET-30",None,      None,        "2024-01-08","2024-01-08 14:00:00","billing01"),
    ("INV-5034","INV-2024-5034","WO-10054","CUST-059","2024-01-09","2024-02-08","PAID",    "195.00",  "15.60", "0.00",   "210.60",  "NET-30","210.60",  "2024-01-22","2024-01-09","2024-01-22 11:00:00","billing02"),
    ("INV-5035","INV-2024-5035","WO-10055","CUST-060","2024-01-10","2024-02-09","SENT",    "1720.00", "137.60","0.00",   "1857.60", "NET-30",None,      None,        "2024-01-10","2024-01-10 12:00:00","billing01"),
    ("INV-5036","INV-2024-5036","WO-10056","CUST-062","2024-01-05","2024-02-04","PAID",    "130.00",  "10.40", "0.00",   "140.40",  "NET-30","140.40",  "2024-01-18","2024-01-05","2024-01-18 09:00:00","billing02"),
    ("INV-5037","INV-2024-5037","WO-10057","CUST-063","2024-01-06","2024-02-05","SENT",    "1230.00", "98.40", "0.00",   "1328.40", "NET-30",None,      None,        "2024-01-06","2024-01-06 16:00:00","billing01"),
    ("INV-5038","INV-2024-5038","WO-10059","CUST-065","2024-01-08","2024-02-07","PAID",    "1110.00", "88.80", "0.00",   "1198.80", "NET-30","1198.80", "2024-01-21","2024-01-08","2024-01-21 10:00:00","billing02"),
    ("INV-5039","INV-2024-5039","WO-10073","CUST-058","2024-01-06","2024-02-05","PAID",    "130.00",  "10.40", "0.00",   "140.40",  "NET-30","140.40",  "2024-01-19","2024-01-06","2024-01-19 11:00:00","billing01"),
    ("INV-5040","INV-2024-5040","WO-10074","CUST-059","2024-01-08","2024-02-07","SENT",    "670.00",  "53.60", "0.00",   "723.60",  "NET-30",None,      None,        "2024-01-08","2024-01-08 14:30:00","billing02"),
    ("INV-5041","INV-2024-5041","WO-10075","CUST-060","2024-01-09","2024-02-08","PAID",    "130.00",  "10.40", "0.00",   "140.40",  "NET-30","140.40",  "2024-01-22","2024-01-09","2024-01-22 10:00:00","billing01"),

    # ── PROBLEM: tax_amount results in effective rate > 100% of subtotal ─────────
    ("INV-5042","INV-2024-5042","WO-10076","CUST-062","2024-01-05","2024-02-04","SENT",    "195.00",  "290.00","0.00",   "485.00",  "NET-30",None,      None,        "2024-01-05","2024-01-05 12:00:00","billing02"),

    # ── PROBLEM: VOID invoice with payment_received > 0 (logically contradictory)─
    ("INV-5043","INV-2024-5043","WO-10077","CUST-063","2024-01-10","2024-02-09","VOID",    "1145.00", "91.60", "0.00",   "1236.60", "NET-30","1236.60", "2024-01-11","2024-01-10","2024-01-11 09:00:00","billing01"),

    # ── PROBLEM: Invoice for a CANCELLED work order ──────────────────────────────
    ("INV-5044","INV-2024-5044","WO-10066","CUST-060","2024-01-09","2024-02-08","SENT",    "520.00",  "41.60", "0.00",   "561.60",  "NET-30",None,      None,        "2024-01-09","2024-01-09 15:00:00","billing02"),

    # ── PROBLEM: payment_date is BEFORE invoice_date ─────────────────────────────
    ("INV-5045","INV-2024-5045","WO-10079","CUST-065","2024-01-07","2024-02-06","PAID",    "552.50",  "44.20", "0.00",   "596.70",  "NET-30","596.70",  "2024-01-06","2024-01-07","2024-01-07 15:00:00","billing01"),

    # ── PROBLEM: Same invoice_number on two different invoice_ids ────────────────
    ("INV-5046","INV-2024-5046","WO-10080","CUST-056","2024-01-08","2024-02-07","SENT",    "260.00",  "20.80", "0.00",   "280.80",  "NET-30",None,      None,        "2024-01-08","2024-01-08 13:00:00","billing02"),
    ("INV-5047","INV-2024-5046","WO-10071","CUST-056","2024-01-07","2024-02-06","DRAFT",   "130.00",  "10.40", "0.00",   "140.40",  "NET-30",None,      None,        "2024-01-07","2024-01-07 12:00:00","billing01"),
    # Note: INV-5046 and INV-5047 share invoice_number "INV-2024-5046"

    # ── PROBLEM: invoice_status PAID but total_amount = 0 ────────────────────────
    ("INV-5048","INV-2024-5048","WO-10068","CUST-063","2024-01-05","2024-02-04","PAID",    "0.00",    "0.00",  "0.00",   "0.00",    "NET-30","0.00",    "2024-01-12","2024-01-05","2024-01-12 10:00:00","billing02"),

    # ── PROBLEM: Discount applied making total negative ───────────────────────────
    ("INV-5049","INV-2024-5049","WO-10060","CUST-024","2024-01-05","2024-02-04","SENT",    "130.00",  "10.40", "200.00", "-59.60",  "NET-30",None,      None,        "2024-01-05","2024-01-05 13:00:00","billing01"),

    # ── PROBLEM: payment_received = total but status still SENT (not updated) ─────
    ("INV-5050","INV-2024-5050","WO-10061","CUST-030","2024-01-07","2024-02-06","SENT",    "840.00",  "67.20", "0.00",   "907.20",  "NET-30","907.20",  "2024-01-18","2024-01-07","2024-01-18 11:00:00","billing02"),

    # ── PROBLEM: invoice_number is NULL ──────────────────────────────────────────
    ("INV-5051",None,            "WO-10062","CUST-056","2024-01-06","2024-02-05","SENT",    "130.00",  "10.40", "0.00",   "140.40",  "NET-30",None,      None,        "2024-01-06","2024-01-06 12:00:00","billing01"),

    # ── PROBLEM: Future invoice_date ──────────────────────────────────────────────
    ("INV-5052","INV-2024-5052","WO-10072","CUST-057","2026-06-01","2026-07-01","DRAFT",   "0.00",    "0.00",  "0.00",   "0.00",    "NET-30",None,      None,        "2024-01-10","2024-01-10 10:00:00","billing02"),

    # ── Good draft invoices ───────────────────────────────────────────────────────
    ("INV-5053","INV-2024-5053","WO-10058","CUST-064","2024-01-11","2024-02-10","DRAFT",   "0.00",    "0.00",  "0.00",   "0.00",    "NET-30",None,      None,        "2024-01-11","2024-01-11 09:30:00","billing01"),
    ("INV-5054","INV-2024-5054","WO-10078","CUST-064","2024-01-11","2024-02-10","DRAFT",   "0.00",    "0.00",  "0.00",   "0.00",    "NET-30",None,      None,        "2024-01-11","2024-01-11 10:00:00","billing01"),
    ("INV-5055","INV-2024-5055","WO-10063","CUST-057","2024-01-07","2024-02-06","SENT",    "2100.00", "168.00","0.00",   "2268.00", "NET-30",None,      None,        "2024-01-07","2024-01-07 17:00:00","billing02"),
]

inv_cols = [
    "invoice_id","invoice_number","work_order_id","customer_id","invoice_date","due_date",
    "invoice_status","subtotal","tax_amount","discount_amount","total_amount",
    "payment_terms","payment_received","payment_date","created_date","last_modified","created_by"
]
df_inv2 = spark.createDataFrame(
    [Row(**dict(zip(inv_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in invoices]
)
append(df_inv2, "invoices")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Assignments — 25 new records (ASGN-036 … ASGN-060)
# MAGIC NEW problems: start_time in future for COMPLETED, hours_logged = 0 on
# MAGIC COMPLETED, two LEAD techs on same WO, assignment for ON_HOLD WO active,
# MAGIC assignment date 30+ days after WO scheduled date

# COMMAND ----------

assignments = [
    # ── GOOD new records ──────────────────────────────────────────────────────────
    ("ASGN-036","WO-10051","TECH-031","LEAD",    "2024-01-04","2024-01-06 08:00:00","2024-01-06 12:00:00","4.0", "COMPLETED"),
    ("ASGN-037","WO-10052","TECH-032","SOLO",    "2024-01-05","2024-01-07 09:00:00","2024-01-07 11:00:00","2.0", "COMPLETED"),
    ("ASGN-038","WO-10053","TECH-033","LEAD",    "2024-01-06","2024-01-08 07:00:00","2024-01-08 13:00:00","6.5", "COMPLETED"),
    ("ASGN-039","WO-10054","TECH-034","SOLO",    "2024-01-07","2024-01-09 10:00:00","2024-01-09 13:00:00","3.0", "COMPLETED"),
    ("ASGN-040","WO-10055","TECH-035","LEAD",    "2024-01-10","2024-01-10 03:00:00","2024-01-10 11:00:00","8.0", "COMPLETED"),
    ("ASGN-041","WO-10056","TECH-036","SOLO",    "2024-01-03","2024-01-05 08:00:00","2024-01-05 10:00:00","2.0", "COMPLETED"),
    ("ASGN-042","WO-10057","TECH-031","LEAD",    "2024-01-04","2024-01-06 09:00:00","2024-01-06 15:00:00","6.0", "COMPLETED"),
    ("ASGN-043","WO-10059","TECH-032","LEAD",    "2024-01-06","2024-01-08 08:00:00","2024-01-08 14:00:00","6.0", "COMPLETED"),
    ("ASGN-044","WO-10073","TECH-033","SOLO",    "2024-01-04","2024-01-06 10:00:00","2024-01-06 12:00:00","2.0", "COMPLETED"),
    ("ASGN-045","WO-10074","TECH-034","LEAD",    "2024-01-06","2024-01-08 07:30:00","2024-01-08 13:30:00","6.0", "COMPLETED"),
    ("ASGN-046","WO-10075","TECH-035","SOLO",    "2024-01-07","2024-01-09 09:00:00","2024-01-09 11:00:00","2.0", "COMPLETED"),
    ("ASGN-047","WO-10076","TECH-036","LEAD",    "2024-01-03","2024-01-05 08:00:00","2024-01-05 11:00:00","3.0", "COMPLETED"),
    ("ASGN-048","WO-10079","TECH-031","LEAD",    "2024-01-05","2024-01-07 06:00:00","2024-01-07 14:00:00","8.5", "COMPLETED"),
    ("ASGN-049","WO-10080","TECH-032","SOLO",    "2024-01-06","2024-01-08 08:00:00","2024-01-08 12:00:00","4.0", "COMPLETED"),

    # ── PROBLEM: start_time is in the future for a COMPLETED assignment ────────
    ("ASGN-050","WO-10060","TECH-033","SOLO",    "2024-01-03","2025-06-15 09:00:00","2025-06-15 11:00:00","2.0", "COMPLETED"),

    # ── PROBLEM: hours_logged = 0 on COMPLETED assignment ─────────────────────
    ("ASGN-051","WO-10062","TECH-034","SOLO",    "2024-01-04","2024-01-06 09:00:00","2024-01-06 11:00:00","0.0", "COMPLETED"),

    # ── PROBLEM: Two LEAD techs on the same WO (only one should be LEAD) ────────
    ("ASGN-052","WO-10063","TECH-035","LEAD",    "2024-01-05","2024-01-07 07:00:00","2024-01-07 16:00:00","9.0", "COMPLETED"),
    ("ASGN-053","WO-10063","TECH-036","LEAD",    "2024-01-05","2024-01-07 07:00:00","2024-01-07 16:00:00","9.0", "COMPLETED"),

    # ── PROBLEM: Assignment for ON_HOLD WO marked ACTIVE ──────────────────────
    ("ASGN-054","WO-10049","TECH-031","LEAD",    "2024-01-11","2024-01-11 08:00:00",None,                 None,  "ACTIVE"),
    # WO-10049 is ON_HOLD — assignment shouldn't be active

    # ── PROBLEM: Assignment for CANCELLED WO ──────────────────────────────────
    ("ASGN-055","WO-10066","TECH-032","LEAD",    "2024-01-09","2024-01-09 08:00:00",None,                 None,  "ACTIVE"),

    # ── PROBLEM: assigned_date is 45 days after WO's scheduled_date ────────────
    # WO-10014 was scheduled 2024-01-15; assigning on 2024-03-01 makes no sense
    ("ASGN-056","WO-10014","TECH-033","SUPPORT", "2024-03-01",None,                 None,                 None,  "SCHEDULED"),

    # ── PROBLEM: hours_logged exceeds 12 for a single-shift assignment ─────────
    ("ASGN-057","WO-10064","TECH-034","SOLO",    "2024-01-06","2024-01-08 06:00:00","2024-01-08 18:00:00","16.0","COMPLETED"),

    # ── PROBLEM: assignment_status is NULL ──────────────────────────────────────
    ("ASGN-058","WO-10077","TECH-035","LEAD",    "2024-01-10","2024-01-10 05:00:00","2024-01-10 12:00:00","7.0", None),

    # ── PROBLEM: assigned_date after start_time ────────────────────────────────
    ("ASGN-059","WO-10070","TECH-036","LEAD",    "2024-01-11","2024-01-09 08:00:00","2024-01-09 15:00:00","7.0", "ACTIVE"),

    # ── PROBLEM: Duplicate assignment — exact copy of ASGN-043 ─────────────────
    ("ASGN-060","WO-10059","TECH-032","LEAD",    "2024-01-06","2024-01-08 08:00:00","2024-01-08 14:00:00","6.0", "COMPLETED"),
]

asgn_cols = [
    "assignment_id","work_order_id","technician_id","assignment_role",
    "assigned_date","start_time","end_time","hours_logged","assignment_status"
]
df_asgn2 = spark.createDataFrame(
    [Row(**dict(zip(asgn_cols, r)), notes=None, _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in assignments]
)
append(df_asgn2, "work_order_assignments")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Work Order Parts — NEW TABLE, 35 records
# MAGIC Problems: qty = 0, unit_cost = 0 on non-free part, total_cost ≠ qty×unit,
# MAGIC orphaned WO, NULL part_number, duplicate part line on same WO,
# MAGIC negative quantity, unit_cost with currency symbol

# COMMAND ----------

parts = [
    # ── GOOD records ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    ("PART-001","WO-10001","TRF-2045",  "Transformer gasket set",         "2",   "42.50",  "85.00",  "WH-SW01","cmendez",  "2024-01-08"),
    ("PART-002","WO-10002","PMP-3318",  "Centrifugal pump impeller",      "1",   "890.00", "890.00", "WH-NW02","aosei",    "2024-01-07"),
    ("PART-003","WO-10002","SEAL-112",  "Mechanical seal kit",            "1",   "360.00", "360.00", "WH-NW02","aosei",    "2024-01-07"),
    ("PART-004","WO-10006","VLV-0044",  "Gas isolation valve 2-inch",     "2",   "375.00", "750.00", "WH-SW01","cmendez",  "2024-01-08"),
    ("PART-005","WO-10012","BATT-6612", "Substation battery cell 12V",    "24",  "175.00", "4200.00","WH-MW03","skowalski","2024-01-09"),
    ("PART-006","WO-10016","VLV-0092",  "Pressure relief valve 1-inch",   "1",   "680.00", "680.00", "WH-SW01","kantwi",   "2024-01-06"),
    ("PART-007","WO-10021","TRF-1190",  "Transformer winding coil",       "1",   "980.00", "980.00", "WH-MW03","pflynn",   "2024-01-06"),
    ("PART-008","WO-10024","CABLE-220", "MV cable insulation tape roll",  "12",  "118.33", "1420.00","WH-NE04","rpatel",   "2024-01-03"),
    ("PART-009","WO-10027","ISO-0077",  "Gas isolation flange set",       "4",   "130.00", "520.00", "WH-SW01","cmendez",  "2024-01-09"),
    ("PART-010","WO-10045","BUS-004",   "Bus bar connector 200A",         "4",   "220.00", "880.00", "WH-MW03","aosei",    "2024-01-09"),
    ("PART-011","WO-10047","NGR-001",   "Neutral grounding resistor 100R","1",   "1150.00","1150.00","WH-NE04","rpatel",   "2024-01-07"),
    ("PART-012","WO-10053","CTR-008",   "Contactor 40A 3-pole",           "2",   "185.00", "370.00", "WH-SW01","dreyes",   "2024-01-08"),
    ("PART-013","WO-10055","OIL-054",   "Transformer oil 25L drum",       "6",   "200.00", "1200.00","WH-SE05","kantwi",   "2024-01-10"),
    ("PART-014","WO-10057","REG-022",   "Gas regulator seat assembly",    "1",   "840.00", "840.00", "WH-NW02","mtran",    "2024-01-06"),
    ("PART-015","WO-10059","RECL-003",  "Recloser mechanism assembly",    "1",   "720.00", "720.00", "WH-SW01","smoradi",  "2024-01-08"),

    # ── PROBLEM: quantity_used = 0 (part recorded but nothing used) ─────────────
    ("PART-016","WO-10003","SEAL-215",  "Pipeline O-ring set",            "0",   "28.00",  "0.00",   "WH-SW01","cmendez",  "2024-01-09"),
    ("PART-017","WO-10025","GREASE-01", "High-temp bearing grease 500g",  "0",   "15.50",  "0.00",   "WH-CW06","tmorrison","2024-01-02"),

    # ── PROBLEM: unit_cost = 0 on a non-consumable part (missing price) ─────────
    ("PART-018","WO-10028","FUSE-440",  "HRC fuse 100A",                  "3",   "0.00",   "0.00",   "WH-MW03","skowalski","2024-01-08"),
    ("PART-019","WO-10048","BELT-330",  "Drive belt B-section",           "2",   "0.00",   "0.00",   "WH-CW06","tmorrison","2024-01-06"),

    # ── PROBLEM: total_cost ≠ quantity × unit_cost ───────────────────────────────
    ("PART-020","WO-10029","LUB-002",   "Bearing lubricant 1L",           "4",   "22.50",  "150.00", "WH-SW01","dreyes",   "2024-01-07"),
    # 4 × 22.50 = 90.00 but recorded as 150.00
    ("PART-021","WO-10039","FILT-011",  "Air filter coarse",              "3",   "18.00",  "90.00",  "WH-SE05","jokafor",  "2024-01-07"),
    # 3 × 18.00 = 54.00 but recorded as 90.00

    # ── PROBLEM: Orphaned work_order_id ─────────────────────────────────────────
    ("PART-022","WO-66666","CABLE-100", "Control cable 1.5mm 50m roll",   "1",   "95.00",  "95.00",  "WH-NE04","rpatel",   "2024-01-08"),
    ("PART-023","WO-44444","TERM-007",  "Cable terminal lugs pack",       "1",   "35.00",  "35.00",  "WH-MW03","pflynn",   "2024-01-07"),

    # ── PROBLEM: NULL part_number ────────────────────────────────────────────────
    ("PART-024","WO-10033",None,         "Unknown part — emergency use",   "1",   "85.00",  "85.00",  "WH-MW03","skowalski","2024-01-07"),
    ("PART-025","WO-10038",None,         "Battery — type unspecified",     "1",   "145.00", "145.00", "WH-SE05","kantwi",   "2024-01-08"),

    # ── PROBLEM: Duplicate part line on same WO (same part_number, same date) ────
    ("PART-026","WO-10002","PMP-3318",  "Centrifugal pump impeller",      "1",   "890.00", "890.00", "WH-NW02","aosei",    "2024-01-07"),
    # PART-002 already recorded this exact part for WO-10002

    # ── PROBLEM: Negative quantity_used ─────────────────────────────────────────
    ("PART-027","WO-10022","OIL-020",   "Turbine oil 5L can",             "-2",  "45.00",  "-90.00", "WH-NW02","ykim",     "2024-01-05"),
    ("PART-028","WO-10041","GREASE-02", "Molybdenum grease 500g",         "-1",  "18.00",  "-18.00", "WH-SE05","jokafor",  "2024-01-06"),

    # ── PROBLEM: unit_cost stored with currency symbol ───────────────────────────
    ("PART-029","WO-10043","WIRE-055",  "Copper wire 16mm² 10m",          "2",   "$85.00", "$170.00","WH-SE05","kantwi",   "2024-01-04"),

    # ── PROBLEM: warehouse_code not in known domain ───────────────────────────────
    ("PART-030","WO-10046","TERM-020",  "Heat shrink terminal assortment","1",   "22.00",  "22.00",  "WH-UNKNOWN","tmorrison","2024-01-08"),

    # ── PROBLEM: recorded_date in the future ─────────────────────────────────────
    ("PART-031","WO-10051","REL-009",   "Protection relay module",        "1",   "85.00",  "85.00",  "WH-NW02","dreyes",   "2025-06-01"),

    # ── PROBLEM: recorded_by is NULL ─────────────────────────────────────────────
    ("PART-032","WO-10073","BELT-110",  "V-belt section A",               "2",   "14.00",  "28.00",  "WH-SW01",None,       "2024-01-06"),

    # ── Good additional records ──────────────────────────────────────────────────
    ("PART-033","WO-10063","BUS-012",   "Busbar 100A copper 500mm",       "4",   "700.00", "2800.00","WH-NW02","mtran",    "2024-01-07"),
    ("PART-034","WO-10077","VLV-0108",  "Gas relief valve 3-inch",        "1",   "690.00", "690.00", "WH-SW01","cmendez",  "2024-01-10"),
    ("PART-035","WO-10079","RELAY-220", "Differential relay 87T",         "0",   "0.00",   "0.00",   "WH-NE04","kasher",   "2024-01-07"),
    # PART-035: both qty=0 and unit_cost=0 — doubly bad
]

part_cols = [
    "part_usage_id","work_order_id","part_number","part_description",
    "quantity_used","unit_cost","total_cost","warehouse_code","recorded_by","recorded_date"
]
df_parts = spark.createDataFrame(
    [Row(**dict(zip(part_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in parts]
)
append(df_parts, "work_order_parts")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Technician Certifications — 30 records
# MAGIC Problems: expiry_date before issued_date, expired cert on ACTIVE tech,
# MAGIC NULL certification_code, duplicate cert for same tech,
# MAGIC cert_status not in domain, future issued_date

# COMMAND ----------

certifications = [
    # ── GOOD records ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    ("CERT-001","TECH-001","ELEC-HV-1","HV Electrical Safety Level 1",         "2020-03-15","2025-03-15","NFPA",      "ACTIVE"),
    ("CERT-002","TECH-001","GAS-SAFE-2","Gas Safety Certificate Level 2",      "2021-06-01","2026-06-01","AGA",       "ACTIVE"),
    ("CERT-003","TECH-002","ELEC-HV-2","HV Electrical Safety Level 2",         "2019-08-20","2024-08-20","NFPA",      "ACTIVE"),
    ("CERT-004","TECH-002","PUMP-CERT", "Pump Systems Specialist",              "2022-01-10","2025-01-10","HI",        "ACTIVE"),
    ("CERT-005","TECH-003","ELEC-HV-1","HV Electrical Safety Level 1",         "2021-09-05","2026-09-05","NFPA",      "ACTIVE"),
    ("CERT-006","TECH-003","CONFINED",  "Confined Space Entry",                 "2022-04-12","2025-04-12","OSHA",      "ACTIVE"),
    ("CERT-007","TECH-014","ELEC-HV-2","HV Electrical Safety Level 2",         "2018-07-18","2023-07-18","NFPA",      "EXPIRED"),
    ("CERT-008","TECH-014","GAS-SAFE-1","Gas Safety Certificate Level 1",       "2020-11-22","2025-11-22","AGA",       "ACTIVE"),
    ("CERT-009","TECH-016","ELEC-HV-3","HV Electrical Safety Level 3",         "2017-02-14","2022-02-14","NFPA",      "EXPIRED"),
    ("CERT-010","TECH-016","CONFINED",  "Confined Space Entry",                 "2021-07-09","2024-07-09","OSHA",      "ACTIVE"),
    ("CERT-011","TECH-019","ELEC-HV-2","HV Electrical Safety Level 2",         "2016-05-27","2021-05-27","NFPA",      "EXPIRED"),
    ("CERT-012","TECH-019","GAS-SAFE-2","Gas Safety Certificate Level 2",       "2019-10-03","2024-10-03","AGA",       "ACTIVE"),
    ("CERT-013","TECH-031","ELEC-HV-1","HV Electrical Safety Level 1",         "2023-06-01","2028-06-01","NFPA",      "ACTIVE"),
    ("CERT-014","TECH-032","ELEC-HV-3","HV Electrical Safety Level 3",         "2016-03-10","2021-03-10","NFPA",      "EXPIRED"),
    ("CERT-015","TECH-034","GAS-SAFE-2","Gas Safety Certificate Level 2",       "2018-12-05","2023-12-05","AGA",       "EXPIRED"),

    # ── PROBLEM: expiry_date before issued_date ──────────────────────────────────
    ("CERT-016","TECH-004","ELEC-HV-1","HV Electrical Safety Level 1",         "2023-08-01","2022-08-01","NFPA",      "ACTIVE"),
    ("CERT-017","TECH-018","CONFINED",  "Confined Space Entry",                 "2023-11-15","2023-01-15","OSHA",      "ACTIVE"),

    # ── PROBLEM: ACTIVE tech with all certs EXPIRED (can't legally work) ─────────
    # TECH-007 is TERMINATED so this is also a FK/status problem
    ("CERT-018","TECH-005","ELEC-HV-1","HV Electrical Safety Level 1",         "2016-04-20","2021-04-20","NFPA",      "EXPIRED"),
    ("CERT-019","TECH-005","GAS-SAFE-1","Gas Safety Certificate Level 1",       "2017-09-11","2022-09-11","AGA",       "EXPIRED"),
    # TECH-005 is active with ALL expired certs — business rule violation

    # ── PROBLEM: NULL certification_code ─────────────────────────────────────────
    ("CERT-020","TECH-020",None,         "General Site Safety Induction",        "2021-05-17","2024-05-17","MERIDIAN",  "ACTIVE"),
    ("CERT-021","TECH-036",None,         "Electrical Awareness Course",          "2022-07-11","2025-07-11","MERIDIAN",  "ACTIVE"),

    # ── PROBLEM: Duplicate cert — same tech, same code, overlapping dates ─────────
    ("CERT-022","TECH-002","PUMP-CERT", "Pump Systems Specialist",              "2022-01-10","2025-01-10","HI",        "ACTIVE"),
    # Exact duplicate of CERT-004

    # ── PROBLEM: cert_status not in allowed domain ───────────────────────────────
    ("CERT-023","TECH-015","ELEC-HV-1","HV Electrical Safety Level 1",         "2021-08-01","2026-08-01","NFPA",      "Pending Renewal"),
    ("CERT-024","TECH-017","GAS-SAFE-1","Gas Safety Certificate Level 1",       "2020-05-15","2025-05-15","AGA",       "under review"),

    # ── PROBLEM: issued_date in the future ───────────────────────────────────────
    ("CERT-025","TECH-033","ELEC-HV-2","HV Electrical Safety Level 2",         "2026-03-01","2031-03-01","NFPA",      "ACTIVE"),

    # ── PROBLEM: cert on TERMINATED technician (probably fine to keep, but
    #    creates a business rule question — cert still shown ACTIVE) ───────────────
    ("CERT-026","TECH-006","ELEC-HV-2","HV Electrical Safety Level 2",         "2020-04-14","2025-04-14","NFPA",      "ACTIVE"),
    # TECH-006 terminated 2023-11-30 but cert shows ACTIVE through 2025

    # ── PROBLEM: issuing_body NULL ───────────────────────────────────────────────
    ("CERT-027","TECH-035","CONFINED",  "Confined Space Entry",                 "2022-01-20","2025-01-20",None,        "ACTIVE"),

    # ── PROBLEM: expiry_date already passed but status still ACTIVE ──────────────
    ("CERT-028","TECH-003","ELEC-HV-3","HV Electrical Safety Level 3",         "2018-06-10","2023-06-10","NFPA",      "ACTIVE"),
    # expiry 2023-06-10 is in the past — should be EXPIRED

    # Good records
    ("CERT-029","TECH-032","GAS-SAFE-2","Gas Safety Certificate Level 2",       "2021-02-28","2026-02-28","AGA",       "ACTIVE"),
    ("CERT-030","TECH-047","ELEC-HV-1","HV Electrical Safety Level 1",         "2023-08-01","2028-08-01","NFPA",      "ACTIVE"),
]

cert_cols = [
    "cert_id","technician_id","certification_code","certification_name",
    "issued_date","expiry_date","issuing_body","cert_status"
]
df_certs = spark.createDataFrame(
    [Row(**dict(zip(cert_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in certifications]
)
append(df_certs, "technician_certifications")

# COMMAND ----------
# MAGIC %md ## Final Summary

# COMMAND ----------

print("=" * 60)
print("  BATCH 2 LOAD COMPLETE")
print("=" * 60)
totals = {}
for tbl in ["customers","technicians","work_orders","invoices",
            "work_order_assignments","work_order_parts","technician_certifications"]:
    n = spark.table(t(tbl)).count()
    totals[tbl] = n
    print(f"  {t(tbl):<50} {n:>5} rows total")

print(f"\n  Grand total records: {sum(totals.values())}")
print("""
  DQ problem inventory across both batches:
  ─────────────────────────────────────────
  Customers   : NULL name/status, duplicates (exact/fuzzy/cross-system),
                bad domain values, future dates, unparseable dates,
                encoding artifacts, city/state mismatch, garbage credit_limit,
                invisible chars in email, wrong region code, stale records,
                whitespace-only fields, numeric status, last_modified < created

  Technicians : status casing, negative/symbol/range hourly_rate,
                hire > termination, future hire_date, NULL employee/skill,
                duplicate employee_number, impossible rate, NULL full_name,
                terminated with no date, unparseable hire, invalid region,
                personal email, missing manager, skill_level typo, NULL region

  Work Orders : invalid status/type/priority, inverted timestamps,
                NULL site/customer, orphaned FKs, negative costs,
                future completed, cost arithmetic mismatch, actual_hours >24/=0,
                stale IN_PROGRESS, SLA flag contradiction, WO for inactive cust,
                whitespace description, parts > total, scheduled < created,
                IN_PROGRESS with end_time populated, NULL WO number,
                resolution_code on open WO, CANCELLED with costs

  Invoices    : imbalanced totals, orphaned WO, duplicate WO invoice,
                negative amounts, overpayment, due < invoice date,
                invalid status, PAID + no payment_date, discount > subtotal,
                VOID + payment, CANCELLED WO invoice, payment < invoice date,
                duplicate invoice_number, NULL invoice_number, future date,
                tax > subtotal, zero total on PAID

  Assignments : orphaned WO/tech, terminated tech on open WO,
                duplicate assignment, negative/zero hours, invalid status/role,
                inverted times, hours ≠ duration, future start on COMPLETED,
                two LEADs on same WO, ON_HOLD WO + ACTIVE assignment,
                CANCELLED WO + ACTIVE assignment, assigned date >> WO date,
                NULL status, assigned_date after start_time

  Parts       : qty=0, unit_cost=0, total ≠ qty×unit, orphaned WO,
                NULL part_number, duplicate line same WO, negative qty,
                currency symbol in cost, unknown warehouse, future date,
                NULL recorded_by, both qty=0 AND cost=0

  Certs       : expiry < issued, all certs expired on active tech,
                NULL cert_code, duplicate cert, invalid status domain,
                future issued_date, ACTIVE cert on terminated tech,
                NULL issuing_body, expired but status=ACTIVE
""")
print("  Next: Re-run notebook 03 (validation) to see updated DQ score")
