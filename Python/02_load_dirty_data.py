# Databricks Notebook — 02_LOAD_DIRTY_DATA
# FieldOps Pro | Intentionally dirty source data
# Every record annotated with the DQ problem it demonstrates
# ─────────────────────────────────────────────────────────────

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration — must match notebook 01

# COMMAND ----------

COMMUNITY_EDITION = False
CATALOG  = "fieldops_dq"
RAW      = "raw"
BATCH_ID = "BATCH-20240110"

def t(table):
    if COMMUNITY_EDITION:
        return f"{RAW}.{table}"
    return f"{CATALOG}.{RAW}.{table}"

from pyspark.sql import Row
from datetime import datetime
now = datetime.utcnow()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customers — 55 records
# MAGIC Problems: NULLs, duplicates (exact + fuzzy + cross-system),
# MAGIC invalid domain values, bad emails, negative credit limits,
# MAGIC unparseable dates, future dates, stale records, missing contacts,
# MAGIC wrong customer_type, garbage credit_limit text

# COMMAND ----------

customers = [
    # ── GOOD BASELINE RECORDS ────────────────────────────────
    ("CUST-001","Apex Power Corporation",         "ENTERPRISE","ACTIVE",   "250000.00","Sandra Okafor",    "sokafor@apexpower.com",       "(512) 555-0101","Austin",        "TX","78701","SW","2019-03-15","2024-01-10 08:30:00","admin",          "FIELDOPS_APP"),
    ("CUST-002","Valley Water District",          "GOVERNMENT","ACTIVE",   "500000.00","Marcus Chen",      "mchen@valleywater.gov",       "(916) 555-0202","Sacramento",    "CA","95814","NW","2018-07-22","2024-01-08 14:15:00","admin",          "FIELDOPS_APP"),
    ("CUST-003","Riverstone Gas & Electric",      "ENTERPRISE","ACTIVE",   "750000.00","Priya Nair",       "pnair@riverstone-ge.com",     "(713) 555-0303","Houston",       "TX","77002","SW","2017-11-01","2024-01-09 11:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-023","Sunbelt Power Cooperative",      "GOVERNMENT","ACTIVE",   "280000.00","Calvin Brooks",    "cbrooks@sunbeltcoop.gov",     "(404) 555-2301","Atlanta",       "GA","30301","SE","2016-11-08","2024-01-09 07:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-024","Alpine Water Authority",         "GOVERNMENT","INACTIVE", "150000.00","Donna Fischer",    "dfischer@alpinewater.gov",    "(801) 555-2401","Salt Lake City","UT","84101","NW","2019-05-12","2024-01-08 11:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-025","Metro Grid Solutions",           "ENTERPRISE","ACTIVE",   "320000.00","Barry White",      "bwhite@metrogrid.com",        "(212) 555-2501","New York",      "NY","10001","NE","2017-09-19","2024-01-07 13:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-028","SolarField Energy Inc",          "SMB",       "ACTIVE",   "65000.00", "Natasha Rivera",   "nrivera@solarfield.com",      "(702) 555-2801","Las Vegas",     "NV","89101","SW","2023-02-28","2024-01-05 10:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-029","Great Lakes Utilities",          "ENTERPRISE","ACTIVE",   "420000.00","Kenneth Shaw",     "kshaw@greatlakesutil.com",    "(313) 555-2901","Detroit",       "MI","48201","MW","2018-04-03","2024-01-04 08:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-030","Gulf Shore Pipeline Co",         "ENTERPRISE","SUSPENDED","200000.00","Irene Castro",     "icastro@gulfshore.com",       "(504) 555-3001","New Orleans",   "LA","70112","SE","2016-08-22","2024-01-03 14:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-031","Cascade Hydroelectric Corp",     "ENTERPRISE","ACTIVE",   "610000.00","Tom Nakamura",     "tnakamura@cascadehydro.com",  "(206) 555-3101","Seattle",       "WA","98101","NW","2015-04-18","2024-01-09 09:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-032","Lone Star Grid Authority",       "GOVERNMENT","ACTIVE",   "380000.00","Maria Gonzalez",   "mgonzalez@lsgrid.gov",        "(214) 555-3201","Dallas",        "TX","75201","SW","2016-02-28","2024-01-08 10:30:00","admin",          "FIELDOPS_APP"),
    ("CUST-033","Keystone Transmission Inc",      "ENTERPRISE","ACTIVE",   "540000.00","Philip Barrett",   "pbarrett@keystonexm.com",     "(215) 555-3301","Philadelphia",  "PA","19101","NE","2014-09-12","2024-01-07 08:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-034","Rocky Mountain Power Co",        "ENTERPRISE","ACTIVE",   "295000.00","Susan Whitfield",  "swhitfield@rmpowerco.com",    "(303) 555-3401","Denver",        "CO","80201","CW","2018-06-15","2024-01-06 11:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-035","Bayou Energy Solutions",         "SMB",       "ACTIVE",   "88000.00", "Andre Thibodaux",  "athibodaux@bayouenergy.com",  "(985) 555-3501","Baton Rouge",   "LA","70801","SE","2021-10-04","2024-01-05 14:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: Duplicate — exact same company, different CUST-ID (cross-system) ──
    ("CUST-004","Apex Power Corporation",         "ENTERPRISE","ACTIVE",   "250000.00","Sandra Okafor",    "sokafor@apexpower.com",       "(512) 555-0101","Austin",        "TX","78701","SW","2019-03-15","2024-01-11 09:00:00","mobile_sync",    "MOBILE_APP"),
    ("CUST-005","APEX POWER CORP",                "Enterprise","Active",   "250000",   "S. Okafor",        "sokafor@apexpower.com",       "512-555-0101",  "Austin",        "TX","78701","SW","2019-03-15","2024-01-11 09:01:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: Duplicate — Valley Water District repeated verbatim ───────────
    ("CUST-027","Valley Water District",          "GOVERNMENT","ACTIVE",   "500000.00","Marcus Chen",      "mchen@valleywater.gov",       "(916) 555-0202","Sacramento",    "CA","95814","NW","2018-07-22","2024-01-08 14:15:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: NULL customer_name ─────────────────────────────────────────────
    ("CUST-006",None,                             "SMB",       "ACTIVE",   "15000.00", "Tom Bradley",      "tbradley@company.com",        "(303) 555-0601","Denver",        "CO","80203","CW","2023-05-20","2024-01-07 10:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: NULL account_status ────────────────────────────────────────────
    ("CUST-007","Pacific Northwest Utilities",    "ENTERPRISE",None,       "300000.00","Janet Wu",         "jwu@pnwutil.com",             "(503) 555-0701","Portland",      "OR","97201","NW","2020-08-14","2024-01-06 15:30:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: NULL all three contact fields ───────────────────────────────────
    ("CUST-022","Northgate Industrial Park",      "SMB",       "ACTIVE",   "30000.00", None,               None,                          None,            "Seattle",       "WA","98101","NW","2023-08-15","2023-12-22 10:30:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: Invalid account_status — not in domain ─────────────────────────
    ("CUST-008","Summit Energy Partners",         "ENTERPRISE","PENDING_REVIEW","180000.00","Alex Torres", "atorres@summitenergy.com",    "(720) 555-0801","Denver",        "CO","80202","CW","2022-12-01","2024-01-05 08:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-009","Lakeside Municipal Water",       "GOVERNMENT","TEMP SUSPEND",  "100000.00","Rob Petersen", "rpetersen@lakesidewater.gov", "(414) 555-0901","Milwaukee",     "WI","53202","MW","2021-06-10","2024-01-04 12:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-010","Brightfield Solar LLC",          "SMB",       "activ",    "50000.00", "Dana Kim",         "dkim@brightfieldsolar.com",   "(415) 555-1001","San Francisco", "CA","94102","NW","2023-09-05","2024-01-03 09:15:00","mobile_sync",    "MOBILE_APP"),
    ("CUST-036","Tidewater Power Services",       "ENTERPRISE","Suspended", "175000.00","Kira Thompson",   "kthompson@tidewaterpwr.com",  "(757) 555-3601","Norfolk",       "VA","23501","SE","2020-03-10","2024-01-02 08:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-037","Olympia Water Services",         "GOVERNMENT","CLOSED",   "90000.00", "Fred Harrison",    "fharrison@olympiaws.gov",     "(360) 555-3701","Olympia",       "WA","98501","NW","2019-07-22","2023-12-31 09:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: Future created_date ────────────────────────────────────────────
    ("CUST-011","Northern Grid Authority",        "GOVERNMENT","ACTIVE",   "400000.00","Christine Bell",   "cbell@norgrid.gov",           "(651) 555-1101","Minneapolis",   "MN","55401","MW","2027-01-01","2024-01-02 07:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-038","Horizon Energy Partners",        "ENTERPRISE","ACTIVE",   "210000.00","Leo Vance",        "lvance@horizonep.com",        "(702) 555-3801","Reno",          "NV","89501","SW","2026-06-15","2024-01-01 11:00:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: Unparseable created_date ───────────────────────────────────────
    ("CUST-012","MidWest Power Group",            "ENTERPRISE","ACTIVE",   "600000.00","James Porter",     "jporter@mwpowergroup.com",    "(312) 555-1201","Chicago",       "IL","60601","MW","UNKNOWN",   "2024-01-01 14:00:00","legacy_import",  "LEGACY_DB"),
    ("CUST-039","Delta Basin Utilities",          "GOVERNMENT","ACTIVE",   "330000.00","Irma Delgado",     "idelgado@deltabasin.gov",     "(662) 555-3901","Jackson",       "MS","39201","SE","N/A",       "2023-12-30 08:00:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: Negative credit_limit ──────────────────────────────────────────
    ("CUST-013","Clearwater Treatment Co",        "SMB",       "ACTIVE",   "-5000.00", "Helen Park",       "hpark@clearwatertreat.com",   "(615) 555-1301","Nashville",     "TN","37201","SE","2022-03-18","2023-12-28 10:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-040","Tri-State Pipeline LLC",         "ENTERPRISE","ACTIVE",   "-12000.00","Walt Gibson",      "wgibson@tristatepipe.com",    "(918) 555-4001","Tulsa",         "OK","74101","CW","2021-08-09","2023-12-27 14:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: Invalid email format ───────────────────────────────────────────
    ("CUST-014","Desert Sun Energy",              "SMB",       "ACTIVE",   "75000.00", "Miguel Reyes",     "not-an-email",                "(480) 555-1401","Phoenix",       "AZ","85001","SW","2023-01-15","2023-12-30 11:00:00","mobile_sync",    "MOBILE_APP"),
    ("CUST-015","Coastal Power Services",         "ENTERPRISE","ACTIVE",   "350000.00","Yuki Tanaka",      "ytanaka@coastalpwr",          "(858) 555-1501","San Diego",     "CA","92101","NW","2020-05-20","2023-12-29 16:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-041","Silverton Gas Corp",             "ENTERPRISE","ACTIVE",   "160000.00","Beth Carlson",     "bcarlson@@silverton.com",     "(970) 555-4101","Grand Junction","CO","81501","CW","2022-05-14","2023-12-26 12:00:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: credit_limit is garbage text ───────────────────────────────────
    ("CUST-019","Sunridge Residential HOA",       "SMB",       "ACTIVE",   "TBD",      "Fiona Bell",       "fbell@sunridgehoa.org",       "(858) 555-1901","Escondido",     "CA","92025","NW","2023-11-01","2023-12-24 08:00:00","mobile_sync",    "MOBILE_APP"),
    ("CUST-020","Eastern Corridor Rail",          "ENTERPRISE","ACTIVE",   "N/A",      "George Huang",     "ghuang@ecrail.com",           "(617) 555-2001","Boston",        "MA","02101","NE","2022-07-30","2023-12-23 14:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-042","Frontier Telecom Holdings",      "ENTERPRISE","ACTIVE",   "PENDING",  "Chuck Waverly",    "cwaverly@frontierth.com",     "(316) 555-4201","Wichita",       "KS","67201","MW","2023-04-01","2023-12-22 11:00:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: Invalid customer_type ──────────────────────────────────────────
    ("CUST-018","Granite Falls Mining Corp",      "INDUSTRIAL","ACTIVE",   "450000.00","Oscar Nielsen",    "onielsen@granitefalls.com",   "(907) 555-1801","Anchorage",     "AK","99501","NW","2018-02-14","2023-12-25 10:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-043","Bridgeport Commercial Dev",      "COMMERCIAL","ACTIVE",   "125000.00","Diane Wu",         "dwu@bridgeportcd.com",        "(203) 555-4301","Bridgeport",    "CT","06601","NE","2022-01-19","2023-12-21 09:00:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: Stale last_modified (2+ years) — ACTIVE status suspicious ──────
    ("CUST-021","Pacific Gas Holdings",           "ENTERPRISE","ACTIVE",   "500000.00","Rachel Green",     "rgreen@pacgashold.com",       "(213) 555-2101","Los Angeles",   "CA","90001","NW","2015-03-01","2021-06-30 12:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-044","Pioneer Rural Electric",         "GOVERNMENT","ACTIVE",   "95000.00", "Dale Owens",       "dowens@pioneerre.gov",        "(405) 555-4401","Oklahoma City", "OK","73101","CW","2016-09-01","2020-11-15 08:00:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: Invalid billing_state (trailing space / non-US) ────────────────
    ("CUST-016","Heartland Gas Distribution",     "ENTERPRISE","ACTIVE",   "220000.00","Brenda Walsh",     "bwalsh@heartlandgas.com",     "(913) 555-1601","Kansas City",   "KS ","66101","MW","2019-09-10","2023-12-27 09:30:00","admin",          "FIELDOPS_APP"),
    ("CUST-026","TransCanada Operations",         "ENTERPRISE","ACTIVE",   "800000.00","Lena Marchetti",   "lmarchetti@transcanada.com",  "(403) 555-2601","Calgary",       "AB","T2P0S8","NW","2020-01-15","2024-01-06 09:30:00","admin",          "FIELDOPS_APP"),

    # ── PROBLEM: Inconsistent region_code casing ────────────────────────────────
    ("CUST-017","Blue Ridge Telecom",             "ENTERPRISE","ACTIVE",   "190000.00","Larry Stone",      "lstone@blueridgetelecom.com", "(704) 555-1701","Charlotte",     "NC","28201","se","2021-04-22","2023-12-26 13:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-045","Gulf Coast Metering Co",         "SMB",       "ACTIVE",   "55000.00", "Hank Robicheaux",  "hrobicheaux@gulfmetering.com","(337) 555-4501","Lafayette",     "LA","70501","South-East","2023-06-12","2023-12-20 10:00:00","mobile_sync","MOBILE_APP"),

    # ── PROBLEM: phone_number with letters / garbage ─────────────────────────────
    ("CUST-046","Thunder Basin Power",            "ENTERPRISE","ACTIVE",   "245000.00","Cliff Sanderson",  "csanderson@thunderbasin.com", "CALL-MAIN-OFFICE","Casper",      "WY","82601","CW","2020-11-30","2023-12-19 08:00:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: Blank strings instead of NULL (invisible bad data) ──────────────
    ("CUST-047","Ridgecrest Energy LLC",          "SMB",       "ACTIVE",   "42000.00", " ",                " ",                           " ",             "Bakersfield",   "CA","93301","NW","2023-07-04","2023-12-18 14:00:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: customer_name is whitespace-only ─────────────────────────────────
    ("CUST-048","   ",                            "SMB",       "ACTIVE",   "18000.00", "Unknown User",     "unknown@company.com",         "(000) 000-0000","Unknown",       "XX","00000","??","2023-10-10","2023-12-17 12:00:00","mobile_sync",    "MOBILE_APP"),

    # ── PROBLEM: Mixed-case duplicate of CUST-003 from different source ──────────
    ("CUST-049","RIVERSTONE GAS & ELECTRIC",      "enterprise","ACTIVE",   "750000",   "P. Nair",          "pnair@riverstone-ge.com",     "7135550303",    "houston",       "tx","77002","sw","2017-11-01","2024-01-09 11:30:00","legacy_import",  "LEGACY_DB"),

    # ── PROBLEM: Zero credit_limit on ENTERPRISE account (suspicious) ────────────
    ("CUST-050","Windstream Infrastructure",      "ENTERPRISE","ACTIVE",   "0.00",     "Pamela Cross",     "pcross@windstreaminfra.com",  "(501) 555-5001","Little Rock",   "AR","72201","SE","2022-09-17","2024-01-03 09:00:00","admin",          "FIELDOPS_APP"),

    # ── Good records — additional clean volume ────────────────────────────────────
    ("CUST-051","Mojave Desert Utilities",        "ENTERPRISE","ACTIVE",   "310000.00","Arnold Stein",     "astein@mojaveutil.com",       "(760) 555-5101","Victorville",   "CA","92392","NW","2018-12-01","2024-01-08 07:30:00","admin",          "FIELDOPS_APP"),
    ("CUST-052","Appalachian Power Coop",         "GOVERNMENT","ACTIVE",   "175000.00","Joyce Lester",     "jlester@appcoop.gov",         "(304) 555-5201","Charleston",    "WV","25301","SE","2017-03-14","2024-01-07 09:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-053","Northwoods Electric",            "ENTERPRISE","ACTIVE",   "265000.00","Dan Korhonen",     "dkorhonen@northwoodsec.com",  "(715) 555-5301","Eau Claire",    "WI","54701","MW","2019-08-20","2024-01-06 10:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-054","Sonoran Grid Partners",          "ENTERPRISE","ACTIVE",   "400000.00","Renata Vega",      "rvega@sonorangrid.com",       "(520) 555-5401","Tucson",        "AZ","85701","SW","2016-05-11","2024-01-05 11:00:00","admin",          "FIELDOPS_APP"),
    ("CUST-055","Piedmont Power Authority",       "GOVERNMENT","SUSPENDED","130000.00","Byron Knight",     "bknight@piedmontpa.gov",      "(336) 555-5501","Greensboro",    "NC","27401","SE","2020-10-28","2024-01-04 13:00:00","admin",          "FIELDOPS_APP"),
]

# Build schema matching the raw table
customer_cols = [
    "customer_id","customer_name","customer_type","account_status","credit_limit",
    "primary_contact","email_address","phone_number","billing_city","billing_state",
    "billing_zip","region_code","created_date","last_modified","created_by","source_system"
]

df_customers = spark.createDataFrame(
    [Row(**dict(zip(customer_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in customers]
)

df_customers.write.format("delta").mode("overwrite").saveAsTable(t("customers"))
print(f"Loaded {df_customers.count()} customer records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Technicians — 30 records
# MAGIC Problems: inconsistent status casing, negative hourly rate,
# MAGIC hire_date > termination_date, future hire_date, NULL employee_number,
# MAGIC NULL skill_level, duplicate employee_number, terminated tech still active,
# MAGIC impossibly high hourly rate

# COMMAND ----------

technicians = [
    # ── GOOD BASELINE ────────────────────────────────────────
    ("TECH-001","EMP-10045","Carlos",  "Mendez",   "Carlos Mendez",   "ACTIVE",    "SENIOR","SW","2015-06-01",None,         "65.00", "MGR-001","cmendez@meridian.com",    "(512) 555-4001","2015-06-01","2024-01-10 08:00:00"),
    ("TECH-002","EMP-10046","Amara",   "Osei",     "Amara Osei",      "ACTIVE",    "SENIOR","NW","2016-03-15",None,         "68.00", "MGR-002","aosei@meridian.com",      "(503) 555-4002","2016-03-15","2024-01-10 07:45:00"),
    ("TECH-003","EMP-10047","Steven",  "Kowalski", "Steven Kowalski", "ACTIVE",    "MID",   "MW","2018-09-10",None,         "52.00", "MGR-003","skowalski@meridian.com",  "(312) 555-4003","2018-09-10","2024-01-10 09:00:00"),
    ("TECH-004","EMP-10048","Leila",   "Ahmadi",   "Leila Ahmadi",    "ACTIVE",    "JUNIOR","SW","2022-01-20",None,         "38.00", "MGR-001","lahmadi@meridian.com",    "(713) 555-4004","2022-01-20","2024-01-09 10:00:00"),
    ("TECH-014","EMP-10057","Kofi",    "Antwi",    "Kofi Antwi",      "ACTIVE",    "SENIOR","SE","2016-08-22",None,         "67.00", "MGR-004","kantwi@meridian.com",     "(404) 555-4014","2016-08-22","2024-01-09 08:00:00"),
    ("TECH-015","EMP-10058","Tanya",   "Morrison", "Tanya Morrison",  "ACTIVE",    "MID",   "CW","2020-04-06",None,         "54.00", "MGR-006","tmorrison@meridian.com",  "(720) 555-4015","2020-04-06","2024-01-08 14:00:00"),
    ("TECH-016","EMP-10059","Rachel",  "Patel",    "Rachel Patel",    "ACTIVE",    "SENIOR","NE","2014-07-14",None,         "71.00", "MGR-005","rpatel@meridian.com",     "(617) 555-4016","2014-07-14","2024-01-08 08:00:00"),
    ("TECH-017","EMP-10060","Jerome",  "Okafor",   "Jerome Okafor",   "ACTIVE",    "MID",   "SE","2019-11-03",None,         "55.00", "MGR-004","jokafor@meridian.com",    "(813) 555-4017","2019-11-03","2024-01-07 09:00:00"),
    ("TECH-018","EMP-10061","Yoon",    "Kim",      "Yoon Kim",        "ACTIVE",    "JUNIOR","NW","2023-02-14",None,         "41.00", "MGR-002","ykim@meridian.com",       "(206) 555-4018","2023-02-14","2024-01-07 10:00:00"),
    ("TECH-019","EMP-10062","Patrick", "Flynn",    "Patrick Flynn",   "ACTIVE",    "SENIOR","MW","2013-08-05",None,         "73.00", "MGR-003","pflynn@meridian.com",     "(312) 555-4019","2013-08-05","2024-01-06 08:00:00"),
    ("TECH-020","EMP-10063","Shirin",  "Moradi",   "Shirin Moradi",   "ACTIVE",    "MID",   "SW","2021-05-17",None,         "56.00", "MGR-001","smoradi@meridian.com",    "(602) 555-4020","2021-05-17","2024-01-06 09:00:00"),

    # ── PROBLEM: Inconsistent employment_status casing ───────
    ("TECH-005","EMP-10049","Brian",   "Nguyen",   "Brian Nguyen",    "active",    "Senior","NW","2014-11-03",None,         "70.00", "MGR-002","bnguyen@meridian.com",    "(206) 555-4005","2014-11-03","2024-01-08 08:00:00"),
    ("TECH-021","EMP-10064","Camille", "Bouchard", "Camille Bouchard","Active",    "SENIOR","NE","2015-12-01",None,         "69.00", "MGR-005","cbouchard@meridian.com",  "(617) 555-4021","2015-12-01","2024-01-05 08:00:00"),
    ("TECH-022","EMP-10065","Kwame",   "Asante",   "Kwame Asante",    "ACTVE",     "MID",   "SE","2020-07-15",None,         "53.00", "MGR-004","kasante@meridian.com",    "(404) 555-4022","2020-07-15","2024-01-04 09:00:00"),

    # ── PROBLEM: TERMINATED records ───────────────────────────
    ("TECH-006","EMP-10050","Diana",   "Ross",     "Diana Ross",      "Terminated","SENIOR","SE","2013-05-12","2023-11-30", "72.00", "MGR-004","dross@meridian.com",      "(404) 555-4006","2013-05-12","2023-12-01 00:00:00"),
    ("TECH-007","EMP-10051","Ahmad",   "Hassan",   "Ahmad Hassan",    "TERMINATED","MID",   "MW","2019-07-08","2024-01-05", "50.00", "MGR-003","ahassan@meridian.com",    "(312) 555-4007","2019-07-08","2024-01-06 00:00:00"),
    ("TECH-023","EMP-10066","Dora",    "Martinez", "Dora Martinez",   "terminated","JUNIOR","SW","2021-03-22","2023-09-15", "39.00", "MGR-001","dmartinez@meridian.com",  "(602) 555-4023","2021-03-22","2023-09-16 00:00:00"),

    # ── PROBLEM: ON_LEAVE with inconsistent spelling ──────────
    ("TECH-008","EMP-10052","Patrice", "Dubois",   "Patrice Dubois",  "ON LEAVE",  "SENIOR","NE","2017-02-28",None,         "66.00", "MGR-005","pdubois@meridian.com",    "(617) 555-4008","2017-02-28","2024-01-07 11:00:00"),
    ("TECH-024","EMP-10067","Sam",     "Ingram",   "Sam Ingram",      "On-Leave",  "MID",   "CW","2018-10-11",None,         "57.00", "MGR-006","singram@meridian.com",    "(720) 555-4024","2018-10-11","2024-01-03 10:00:00"),

    # ── PROBLEM: Negative hourly_rate ─────────────────────────
    ("TECH-009","EMP-10053","Jerome",  "Banks",    "Jerome Banks",    "ACTIVE",    "JUNIOR","SE","2023-06-05",None,         "-38.50","MGR-004","jbanks@meridian.com",     "(813) 555-4009","2023-06-05","2024-01-05 09:00:00"),
    ("TECH-025","EMP-10068","Fatou",   "Diallo",   "Fatou Diallo",    "ACTIVE",    "MID",   "NW","2022-08-18",None,         "-52.00","MGR-002","fdiallo@meridian.com",    "(503) 555-4025","2022-08-18","2024-01-02 08:00:00"),

    # ── PROBLEM: hire_date AFTER termination_date ─────────────
    ("TECH-010","EMP-10054","Sara",    "Lindqvist","Sara Lindqvist",  "TERMINATED","MID",   "NW","2023-12-01","2022-06-30", "55.00", "MGR-002","slindqvist@meridian.com", "(503) 555-4010","2023-12-01","2023-12-31 00:00:00"),

    # ── PROBLEM: Future hire_date ─────────────────────────────
    ("TECH-012","EMP-10056","Nina",    "Petrov",   "Nina Petrov",     "ACTIVE",    "JUNIOR","NE","2026-01-01",None,         "40.00", "MGR-005","npetrov@meridian.com",    "(212) 555-4012","2026-01-01","2024-01-10 08:00:00"),

    # ── PROBLEM: NULL employee_number and NULL skill_level ────
    ("TECH-011",None,        "Marcus",  "Cole",     "Marcus Cole",     "ACTIVE",    None,    "SW","2021-03-14",None,         "48.00", "MGR-001","mcole@meridian.com",      "(602) 555-4011","2021-03-14","2024-01-04 12:00:00"),

    # ── PROBLEM: Duplicate employee_number (TECH-013 = TECH-001 employee) ────────
    ("TECH-013","EMP-10045","Carlos",  "Mendez",   "Carlos Mendez",   "ACTIVE",    "SENIOR","SW","2015-06-01",None,         "65.00", "MGR-001","cmendez@meridian.com",    "(512) 555-4001","2015-06-01","2024-01-10 09:30:00"),

    # ── PROBLEM: Impossibly high hourly_rate ($9,999/hr) ─────
    ("TECH-026","EMP-10069","Victor",  "Halvorsen","Victor Halvorsen", "ACTIVE",   "SENIOR","MW","2012-04-01",None,         "9999.00","MGR-003","vhalvorsen@meridian.com","(612) 555-4026","2012-04-01","2024-01-01 08:00:00"),

    # ── PROBLEM: NULL full_name (name fields present but concat missing) ─────────
    ("TECH-027","EMP-10070","Carlos",  "Rivera",   None,              "ACTIVE",    "MID",   "SW","2020-09-01",None,         "51.00", "MGR-001","crivera@meridian.com",    "(210) 555-4027","2020-09-01","2024-01-10 07:00:00"),

    # ── PROBLEM: Valid terminated but no termination_date ─────
    ("TECH-028","EMP-10071","Brianna", "Scott",    "Brianna Scott",   "TERMINATED","JUNIOR","SE",None,        None,         "37.00", "MGR-004","bscott@meridian.com",     "(305) 555-4028",None,        "2024-01-09 08:00:00"),

    # ── PROBLEM: hire_date is unparseable ─────────────────────
    ("TECH-029","EMP-10072","Owen",    "Burke",    "Owen Burke",      "ACTIVE",    "MID",   "NE","UNKNOWN",   None,         "53.00", "MGR-005","oburke@meridian.com",     "(617) 555-4029","UNKNOWN",   "2024-01-08 09:00:00"),
    ("TECH-030","EMP-10073","Ingrid",  "Solberg",  "Ingrid Solberg",  "ACTIVE",    "SENIOR","NW","N/A",       None,         "66.00", "MGR-002","isolberg@meridian.com",   "(503) 555-4030","N/A",       "2024-01-07 10:00:00"),
]

tech_cols = [
    "technician_id","employee_number","first_name","last_name","full_name",
    "employment_status","skill_level","home_region","hire_date","termination_date",
    "hourly_rate","manager_id","email","mobile_phone","created_date","last_modified"
]

df_technicians = spark.createDataFrame(
    [Row(**dict(zip(tech_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in technicians]
)
df_technicians.write.format("delta").mode("overwrite").saveAsTable(t("technicians"))
print(f"Loaded {df_technicians.count()} technician records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Work Orders — 50 records
# MAGIC Problems: invalid status, actual_end before actual_start,
# MAGIC NULL site/customer, orphaned FKs, negative costs, future completed dates,
# MAGIC NULL priority on non-draft, total_cost arithmetic mismatch,
# MAGIC impossible actual_hours, stale IN_PROGRESS, SLA flag contradiction,
# MAGIC wrong WO type, exact duplicate WO, completed with no invoice

# COMMAND ----------

work_orders = [
    # ── GOOD BASELINE ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
    ("WO-10001","WO-2024-10001","SITE-001","CUST-001","PREVENTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Annual transformer inspection and maintenance",   "2024-01-08","2024-01-08 08:00:00","2024-01-08 14:30:00","6.0", "6.5", "422.50", "185.00", "607.50", "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-08 15:00:00","dispatch01","FIELDOPS_APP"),
    ("WO-10002","WO-2024-10002","SITE-002","CUST-002","CORRECTIVE_MAINTENANCE","CRITICAL","COMPLETED", "Emergency pump failure at main station",           "2024-01-07","2024-01-07 06:00:00","2024-01-07 18:00:00","10.0","12.0","960.00", "1250.00","2210.00","2024-01-07 12:00:00","N","RESOLVED_PARTIAL",  "2024-01-07","2024-01-07 18:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10003","WO-2024-10003","SITE-003","CUST-003","INSPECTION",            "MEDIUM", "COMPLETED",  "Pipeline pressure test — segment B7",             "2024-01-09","2024-01-09 09:00:00","2024-01-09 12:00:00","3.0", "3.0", "195.00", "0.00",   "195.00", "2024-01-12 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-09 12:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10014","WO-2024-10014","SITE-014","CUST-028","INSPECTION",            "LOW",    "SCHEDULED",  "Solar panel array efficiency test",               "2024-01-15",None,                 None,                 "4.0", None,  None,     None,    None,    "2024-01-18 17:00:00",None,None,               "2024-01-10","2024-01-10 09:00:00","dispatch03","FIELDOPS_APP"),
    ("WO-10015","WO-2024-10015","SITE-015","CUST-023","PREVENTIVE_MAINTENANCE","MEDIUM", "IN_PROGRESS","HVAC plant service — Building A and B",           "2024-01-10","2024-01-10 08:00:00",None,                 "6.0", None,  None,     None,    None,    "2024-01-13 17:00:00",None,None,               "2024-01-08","2024-01-10 10:00:00","dispatch01","FIELDOPS_APP"),
    ("WO-10019","WO-2024-10019","SITE-019","CUST-002","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Pump station annual service",                     "2024-01-08","2024-01-08 07:30:00","2024-01-08 15:00:00","7.0", "7.5", "487.50", "220.00", "707.50", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-08 15:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10020","WO-2024-10020","SITE-020","CUST-025","INSPECTION",            "HIGH",   "COMPLETED",  "Electrical distribution board check",             "2024-01-09","2024-01-09 08:00:00","2024-01-09 10:30:00","3.0", "2.5", "162.50", "50.00",  "212.50", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-07","2024-01-09 11:00:00","dispatch03","FIELDOPS_APP"),
    ("WO-10021","WO-2024-10021","SITE-021","CUST-029","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Replace worn transformer windings",               "2024-01-06","2024-01-06 08:00:00","2024-01-06 16:00:00","8.0", "8.5", "552.50", "980.00", "1532.50","2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 16:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10022","WO-2024-10022","SITE-022","CUST-031","PREVENTIVE_MAINTENANCE","LOW",    "COMPLETED",  "Turbine blade inspection — Unit 2",               "2024-01-05","2024-01-05 09:00:00","2024-01-05 13:00:00","4.0", "4.0", "260.00", "0.00",   "260.00", "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 13:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10023","WO-2024-10023","SITE-023","CUST-032","INSPECTION",            "MEDIUM", "COMPLETED",  "Voltage regulator calibration",                   "2024-01-04","2024-01-04 10:00:00","2024-01-04 12:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-07 17:00:00","Y","RESOLVED_COMPLETE","2024-01-02","2024-01-04 12:30:00","dispatch03","FIELDOPS_APP"),
    ("WO-10024","WO-2024-10024","SITE-024","CUST-033","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Feeder cable insulation replacement",             "2024-01-03","2024-01-03 07:00:00","2024-01-03 15:00:00","8.0", "9.0", "585.00", "1420.00","2005.00","2024-01-06 17:00:00","Y","RESOLVED_COMPLETE","2024-01-01","2024-01-03 15:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10025","WO-2024-10025","SITE-025","CUST-034","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Annual cooling system service",                   "2024-01-02","2024-01-02 08:00:00","2024-01-02 11:00:00","3.0", "3.0", "195.00", "85.00",  "280.00", "2024-01-05 17:00:00","Y","RESOLVED_COMPLETE","2023-12-30","2024-01-02 11:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10026","WO-2024-10026","SITE-026","CUST-035","INSPECTION",            "LOW",    "SCHEDULED",  "Quarterly safety inspection — Site 26",           "2024-01-20",None,                 None,                 "2.0", None,  None,     None,    None,    "2024-01-23 17:00:00",None,None,               "2024-01-10","2024-01-10 11:00:00","dispatch03","FIELDOPS_APP"),
    ("WO-10027","WO-2024-10027","SITE-027","CUST-051","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Gas pressure surge — emergency isolation",        "2024-01-09","2024-01-09 02:00:00","2024-01-09 08:00:00","6.0", "6.0", "390.00", "520.00", "910.00", "2024-01-09 08:00:00","Y","RESOLVED_COMPLETE","2024-01-09","2024-01-09 08:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10028","WO-2024-10028","SITE-028","CUST-052","CORRECTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Control panel short circuit repair",              "2024-01-08","2024-01-08 09:00:00","2024-01-08 13:00:00","4.0", "4.0", "260.00", "310.00", "570.00", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 13:30:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: status not in allowed domain ─────────────────────────────────────
    ("WO-10004","WO-2024-10004","SITE-004","CUST-004","CORRECTIVE_MAINTENANCE","HIGH",   "IN PROGRESS","Faulty circuit breaker — Building C",             "2024-01-10",None,                 None,                 "4.0", None,  None,     None,    None,    "2024-01-12 17:00:00",None,None,               "2024-01-09","2024-01-10 11:00:00","dispatch03","FIELDOPS_APP"),
    ("WO-10005","WO-2024-10005","SITE-005","CUST-005","INSPECTION",            "LOW",    "Completed",  "Meter calibration — customer site",               "2024-01-06","2024-01-06 10:00:00","2024-01-06 11:30:00","2.0", "1.5", "97.50",  "0.00",   "97.50",  "2024-01-09 17:00:00","y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 12:00:00","mobile_user","MOBILE_APP"),
    ("WO-10029","WO-2024-10029","SITE-029","CUST-053","PREVENTIVE_MAINTENANCE","LOW",    "complete",   "Motor bearing lubrication service",               "2024-01-07","2024-01-07 09:00:00","2024-01-07 11:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 11:30:00","mobile_user","MOBILE_APP"),

    # ── PROBLEM: actual_end BEFORE actual_start (time inversion) ─────────────────
    ("WO-10006","WO-2024-10006","SITE-006","CUST-003","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Gas leak detected — isolation required",           "2024-01-08","2024-01-08 14:00:00","2024-01-08 09:00:00","5.0", "5.0", "325.00", "750.00", "1075.00","2024-01-08 16:00:00","N","RESOLVED_COMPLETE","2024-01-08","2024-01-08 14:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10030","WO-2024-10030","SITE-030","CUST-054","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Capacitor bank discharge fault",                  "2024-01-05","2024-01-05 16:00:00","2024-01-05 11:00:00","5.0", "5.0", "325.00", "420.00", "745.00", "2024-01-08 17:00:00","N","RESOLVED_COMPLETE","2024-01-03","2024-01-05 16:30:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: NULL site_id ─────────────────────────────────────────────────────
    ("WO-10007","WO-2024-10007",None,     "CUST-002","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Valve inspection — sector 4",                     "2024-01-05","2024-01-05 08:00:00","2024-01-05 10:30:00","3.0", "2.5", "162.50", "0.00",   "162.50", "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 11:00:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: Orphaned customer_id (CUST-999 does not exist) ──────────────────
    ("WO-10008","WO-2024-10008","SITE-008","CUST-999","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Replace failed relay — panel 7",                  "2024-01-06","2024-01-06 07:30:00","2024-01-06 14:00:00","7.0", "6.5", "422.50", "320.00", "742.50", "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 14:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10031","WO-2024-10031","SITE-031","CUST-888","INSPECTION",            "LOW",    "COMPLETED",  "Fire suppression system test",                    "2024-01-04","2024-01-04 10:00:00","2024-01-04 12:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-07 17:00:00","Y","RESOLVED_COMPLETE","2024-01-02","2024-01-04 12:30:00","dispatch03","FIELDOPS_APP"),

    # ── PROBLEM: Negative cost values ─────────────────────────────────────────────
    ("WO-10009","WO-2024-10009","SITE-009","CUST-001","PREVENTIVE_MAINTENANCE","LOW",    "COMPLETED",  "Annual cooling tower service",                    "2024-01-04","2024-01-04 09:00:00","2024-01-04 12:00:00","3.0", "3.0", "-195.00","-85.00", "-280.00","2024-01-07 17:00:00","Y","RESOLVED_COMPLETE","2024-01-02","2024-01-04 12:30:00","admin",      "FIELDOPS_APP"),
    ("WO-10032","WO-2024-10032","SITE-032","CUST-025","CORRECTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Fan coil unit repair — floor 3",                  "2024-01-08","2024-01-08 10:00:00","2024-01-08 14:00:00","4.0", "4.0", "-260.00","190.00", "-70.00", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 14:30:00","admin",      "FIELDOPS_APP"),

    # ── PROBLEM: COMPLETED status + future scheduled_date (impossible) ────────────
    ("WO-10010","WO-2024-10010","SITE-010","CUST-023","INSPECTION",            "MEDIUM", "COMPLETED",  "Fire suppression system check",                   "2025-06-15","2025-06-15 09:00:00","2025-06-15 11:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2025-06-18 17:00:00","Y","RESOLVED_COMPLETE","2024-01-09","2024-01-10 08:00:00","admin",      "FIELDOPS_APP"),

    # ── PROBLEM: NULL priority on non-draft WO ────────────────────────────────────
    ("WO-10011","WO-2024-10011","SITE-011","CUST-025","CORRECTIVE_MAINTENANCE",None,    "COMPLETED",  None,                                              "2024-01-03","2024-01-03 10:00:00","2024-01-03 13:00:00","3.0", "3.0", "195.00", "120.00", "315.00", "2024-01-06 17:00:00","Y","RESOLVED_COMPLETE","2024-01-02","2024-01-03 13:30:00","dispatch03","FIELDOPS_APP"),

    # ── PROBLEM: total_cost ≠ labor + parts ───────────────────────────────────────
    ("WO-10012","WO-2024-10012","SITE-012","CUST-029","PREVENTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Substation battery bank replacement",             "2024-01-09","2024-01-09 07:00:00","2024-01-09 17:00:00","10.0","10.0","650.00", "4200.00","3500.00","2024-01-12 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-09 17:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10033","WO-2024-10033","SITE-033","CUST-029","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Switch gear timing adjustment",                   "2024-01-07","2024-01-07 08:00:00","2024-01-07 12:00:00","4.0", "4.0", "260.00", "85.00",  "500.00", "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 12:30:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: actual_hours > 24 in one calendar day ───────────────────────────
    ("WO-10013","WO-2024-10013","SITE-013","CUST-017","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Full switchgear overhaul",                        "2024-01-07","2024-01-07 08:00:00","2024-01-07 17:00:00","8.0", "31.0","2015.00","1200.00","3215.00","2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 17:30:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: Exact duplicate WO-ID ───────────────────────────────────────────
    ("WO-10001","WO-2024-10001","SITE-001","CUST-001","PREVENTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Annual transformer inspection and maintenance",   "2024-01-08","2024-01-08 08:00:00","2024-01-08 14:30:00","6.0", "6.5", "422.50", "185.00", "607.50", "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-08 15:01:00","dispatch01","FIELDOPS_APP"),

    # ── PROBLEM: SLA_MET='Y' but actual_end > sla_due_date ──────────────────────
    ("WO-10016","WO-2024-10016","SITE-016","CUST-003","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Pressure relief valve replacement",               "2024-01-06","2024-01-06 06:00:00","2024-01-06 20:00:00","8.0", "14.0","910.00", "680.00", "1590.00","2024-01-06 12:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-06 20:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10034","WO-2024-10034","SITE-034","CUST-032","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Transformer explosion — immediate containment",   "2024-01-03","2024-01-03 04:00:00","2024-01-03 22:00:00","10.0","18.0","1170.00","2400.00","3570.00","2024-01-03 10:00:00","Y","RESOLVED_PARTIAL","2024-01-03","2024-01-03 22:30:00","dispatch01","FIELDOPS_APP"),

    # ── PROBLEM: Invalid work_order_type ──────────────────────────────────────────
    ("WO-10017","WO-2024-10017","SITE-017","CUST-016","SITE_SURVEY",           "LOW",    "COMPLETED",  "New site assessment for expansion",               "2024-01-04","2024-01-04 13:00:00","2024-01-04 15:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-07 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-04 15:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10035","WO-2024-10035","SITE-035","CUST-035","GENERAL_MAINTENANCE",   "MEDIUM", "COMPLETED",  "General site cleanup and tool inventory",         "2024-01-05","2024-01-05 08:00:00","2024-01-05 10:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 10:30:00","dispatch03","FIELDOPS_APP"),

    # ── PROBLEM: Stale IN_PROGRESS (last_modified 90+ days ago) ─────────────────
    ("WO-10018","WO-2024-10018","SITE-018","CUST-029","CORRECTIVE_MAINTENANCE","HIGH",   "IN_PROGRESS","Transformer oil replacement — Unit 3",            "2023-08-15","2023-08-15 09:00:00",None,                 "6.0", None,  None,     None,    None,    "2023-08-18 17:00:00",None,None,               "2023-08-14","2023-08-15 10:00:00","dispatch01","FIELDOPS_APP"),
    ("WO-10036","WO-2024-10036","SITE-036","CUST-033","PREVENTIVE_MAINTENANCE","MEDIUM", "IN_PROGRESS","Cable tray inspection — floors 5–10",             "2023-09-20","2023-09-20 08:00:00",None,                 "5.0", None,  None,     None,    None,    "2023-09-22 17:00:00",None,None,               "2023-09-19","2023-09-20 09:00:00","dispatch02","FIELDOPS_APP"),

    # ── PROBLEM: COMPLETED WO with no assignment (no technician logged) ───────────
    ("WO-10037","WO-2024-10037","SITE-037","CUST-034","INSPECTION",            "LOW",    "COMPLETED",  "Instrument panel calibration",                    "2024-01-02","2024-01-02 09:00:00","2024-01-02 11:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-05 17:00:00","Y","RESOLVED_COMPLETE","2023-12-31","2024-01-02 11:30:00","dispatch03","FIELDOPS_APP"),

    # ── PROBLEM: estimated_hours = 0 on a COMPLETED WO ───────────────────────────
    ("WO-10038","WO-2024-10038","SITE-038","CUST-051","CORRECTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Emergency lighting battery replacement",           "2024-01-08","2024-01-08 11:00:00","2024-01-08 12:30:00","0.0", "1.5", "97.50",  "145.00", "242.50", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 13:00:00","mobile_user","MOBILE_APP"),

    # ── PROBLEM: NULL created_by ──────────────────────────────────────────────────
    ("WO-10039","WO-2024-10039","SITE-039","CUST-052","PREVENTIVE_MAINTENANCE","LOW",    "COMPLETED",  "Air compressor service",                          "2024-01-07","2024-01-07 10:00:00","2024-01-07 12:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05",None,              None,        "MOBILE_APP"),

    # ── PROBLEM: Labor cost present but parts_cost and total_cost both NULL ──────
    ("WO-10040","WO-2024-10040","SITE-040","CUST-053","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Busbar connection tightening — switchroom",       "2024-01-09","2024-01-09 08:00:00","2024-01-09 11:00:00","3.0", "3.0", "195.00", None,    None,    "2024-01-12 17:00:00","Y","RESOLVED_COMPLETE","2024-01-07","2024-01-09 11:30:00","dispatch01","FIELDOPS_APP"),

    # ── Good additional volume ─────────────────────────────────────────────────────
    ("WO-10041","WO-2024-10041","SITE-041","CUST-054","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Chiller plant monthly service",                   "2024-01-06","2024-01-06 09:00:00","2024-01-06 13:00:00","4.0", "4.0", "260.00", "120.00", "380.00", "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 13:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10042","WO-2024-10042","SITE-042","CUST-055","INSPECTION",            "MEDIUM", "COMPLETED",  "Grounding system continuity test",                "2024-01-05","2024-01-05 09:00:00","2024-01-05 11:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-08 17:00:00","Y","RESOLVED_COMPLETE","2024-01-03","2024-01-05 11:30:00","dispatch03","FIELDOPS_APP"),
    ("WO-10043","WO-2024-10043","SITE-043","CUST-023","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Overhead line fault clearance",                   "2024-01-04","2024-01-04 07:00:00","2024-01-04 15:00:00","8.0", "8.0", "520.00", "0.00",   "520.00", "2024-01-07 17:00:00","Y","RESOLVED_COMPLETE","2024-01-02","2024-01-04 15:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10044","WO-2024-10044","SITE-044","CUST-025","PREVENTIVE_MAINTENANCE","LOW",    "SCHEDULED",  "UPS battery bank test — data center",             "2024-01-18",None,                 None,                 "3.0", None,  None,     None,    None,    "2024-01-21 17:00:00",None,None,               "2024-01-10","2024-01-10 11:00:00","dispatch02","FIELDOPS_APP"),
    ("WO-10045","WO-2024-10045","SITE-045","CUST-029","EMERGENCY_REPAIR",      "CRITICAL","COMPLETED", "Substation bus fault — emergency isolation",      "2024-01-09","2024-01-09 03:00:00","2024-01-09 10:00:00","7.0", "7.0", "455.00", "880.00", "1335.00","2024-01-09 08:00:00","N","RESOLVED_PARTIAL","2024-01-09","2024-01-09 10:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10046","WO-2024-10046","SITE-046","CUST-032","INSPECTION",            "MEDIUM", "COMPLETED",  "Lightning protection system check",               "2024-01-08","2024-01-08 09:00:00","2024-01-08 11:00:00","2.0", "2.0", "130.00", "0.00",   "130.00", "2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 11:30:00","dispatch03","FIELDOPS_APP"),
    ("WO-10047","WO-2024-10047","SITE-047","CUST-033","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Neutral grounding resistor replacement",          "2024-01-07","2024-01-07 08:00:00","2024-01-07 14:00:00","6.0", "6.0", "390.00", "1150.00","1540.00","2024-01-10 17:00:00","Y","RESOLVED_COMPLETE","2024-01-05","2024-01-07 14:30:00","dispatch02","FIELDOPS_APP"),
    ("WO-10048","WO-2024-10048","SITE-048","CUST-034","PREVENTIVE_MAINTENANCE","MEDIUM", "COMPLETED",  "Cooling tower blowdown and water treatment",      "2024-01-06","2024-01-06 07:00:00","2024-01-06 11:00:00","4.0", "4.0", "260.00", "380.00", "640.00", "2024-01-09 17:00:00","Y","RESOLVED_COMPLETE","2024-01-04","2024-01-06 11:30:00","dispatch01","FIELDOPS_APP"),
    ("WO-10049","WO-2024-10049","SITE-049","CUST-035","INSPECTION",            "LOW",    "ON_HOLD",    "Hydraulic lift platform inspection",              "2024-01-05",None,                 None,                 "2.0", None,  None,     None,    None,    "2024-01-08 17:00:00",None,None,               "2024-01-03","2024-01-05 10:00:00","dispatch03","FIELDOPS_APP"),
    ("WO-10050","WO-2024-10050","SITE-050","CUST-054","CORRECTIVE_MAINTENANCE","HIGH",   "COMPLETED",  "Arc flash hazard remediation — panel 12",         "2024-01-08","2024-01-08 06:00:00","2024-01-08 14:00:00","8.0", "8.0", "520.00", "670.00", "1190.00","2024-01-11 17:00:00","Y","RESOLVED_COMPLETE","2024-01-06","2024-01-08 14:30:00","dispatch02","FIELDOPS_APP"),
]

wo_cols = [
    "work_order_id","work_order_number","site_id","customer_id","work_order_type",
    "priority","status","description","scheduled_date","actual_start","actual_end",
    "estimated_hours","actual_hours","labor_cost","parts_cost","total_cost",
    "sla_due_date","sla_met","resolution_code","created_date","last_modified",
    "created_by","source_system"
]

df_wo = spark.createDataFrame(
    [Row(**dict(zip(wo_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in work_orders]
)
df_wo.write.format("delta").mode("overwrite").saveAsTable(t("work_orders"))
print(f"Loaded {df_wo.count()} work order records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Invoices — 30 records

# COMMAND ----------

invoices = [
    # ── GOOD ──────────────────────────────────────────────────
    ("INV-5001","INV-2024-5001","WO-10001","CUST-001","2024-01-08","2024-02-07","PAID",    "607.50",  "48.60", "0.00",  "656.10",  "NET-30","656.10",  "2024-01-25","2024-01-08","2024-01-25 10:00:00","billing01"),
    ("INV-5002","INV-2024-5002","WO-10002","CUST-002","2024-01-07","2024-02-06","PAID",    "2210.00", "176.80","0.00",  "2386.80", "NET-30","2386.80", "2024-01-20","2024-01-07","2024-01-20 09:00:00","billing01"),
    ("INV-5003","INV-2024-5003","WO-10003","CUST-003","2024-01-09","2024-02-08","SENT",    "195.00",  "15.60", "0.00",  "210.60",  "NET-30",None,      None,        "2024-01-09","2024-01-09 13:00:00","billing02"),
    ("INV-5013","INV-2024-5013","WO-10021","CUST-029","2024-01-06","2024-02-05","PAID",    "1532.50", "122.60","0.00",  "1655.10", "NET-30","1655.10", "2024-01-18","2024-01-06","2024-01-18 11:00:00","billing01"),
    ("INV-5014","INV-2024-5014","WO-10022","CUST-031","2024-01-05","2024-02-04","PAID",    "260.00",  "20.80", "0.00",  "280.80",  "NET-30","280.80",  "2024-01-17","2024-01-05","2024-01-17 10:00:00","billing02"),
    ("INV-5015","INV-2024-5015","WO-10023","CUST-032","2024-01-04","2024-02-03","PAID",    "130.00",  "10.40", "0.00",  "140.40",  "NET-30","140.40",  "2024-01-16","2024-01-04","2024-01-16 09:00:00","billing01"),
    ("INV-5016","INV-2024-5016","WO-10024","CUST-033","2024-01-03","2024-02-02","SENT",    "2005.00", "160.40","0.00",  "2165.40", "NET-30",None,      None,        "2024-01-03","2024-01-03 16:00:00","billing02"),
    ("INV-5017","INV-2024-5017","WO-10025","CUST-034","2024-01-02","2024-02-01","PAID",    "280.00",  "22.40", "0.00",  "302.40",  "NET-30","302.40",  "2024-01-15","2024-01-02","2024-01-15 10:00:00","billing01"),
    ("INV-5018","INV-2024-5018","WO-10027","CUST-051","2024-01-09","2024-02-08","SENT",    "910.00",  "72.80", "0.00",  "982.80",  "NET-30",None,      None,        "2024-01-09","2024-01-09 09:00:00","billing02"),
    ("INV-5019","INV-2024-5019","WO-10028","CUST-052","2024-01-08","2024-02-07","PAID",    "570.00",  "45.60", "0.00",  "615.60",  "NET-30","615.60",  "2024-01-22","2024-01-08","2024-01-22 11:00:00","billing01"),
    ("INV-5020","INV-2024-5020","WO-10019","CUST-002","2024-01-08","2024-02-07","SENT",    "707.50",  "56.60", "0.00",  "764.10",  "NET-30",None,      None,        "2024-01-08","2024-01-08 16:00:00","billing02"),
    ("INV-5021","INV-2024-5021","WO-10020","CUST-025","2024-01-09","2024-02-08","PAID",    "212.50",  "17.00", "0.00",  "229.50",  "NET-30","229.50",  "2024-01-23","2024-01-09","2024-01-23 09:00:00","billing01"),

    # ── PROBLEM: total_amount does not balance (subtotal+tax-discount ≠ total) ────
    ("INV-5004","INV-2024-5004","WO-10012","CUST-029","2024-01-09","2024-02-08","SENT",    "3500.00", "280.00","0.00",  "4200.00", "NET-30",None,      None,        "2024-01-09","2024-01-09 18:00:00","billing01"),
    ("INV-5022","INV-2024-5022","WO-10033","CUST-029","2024-01-07","2024-02-06","SENT",    "500.00",  "40.00", "0.00",  "345.00",  "NET-30",None,      None,        "2024-01-07","2024-01-07 13:00:00","billing02"),

    # ── PROBLEM: Orphaned invoice — work_order_id does not exist ─────────────────
    ("INV-5005","INV-2024-5005","WO-99999","CUST-001","2024-01-06","2024-02-05","PAID",    "450.00",  "36.00", "0.00",  "486.00",  "NET-30","486.00",  "2024-01-22","2024-01-06","2024-01-22 11:00:00","billing02"),
    ("INV-5023","INV-2024-5023","WO-77777","CUST-003","2024-01-05","2024-02-04","SENT",    "1200.00", "96.00", "0.00",  "1296.00", "NET-30",None,      None,        "2024-01-05","2024-01-05 14:00:00","billing01"),

    # ── PROBLEM: Duplicate invoice for same work order ────────────────────────────
    ("INV-5006","INV-2024-5006","WO-10001","CUST-001","2024-01-09","2024-02-08","SENT",    "607.50",  "48.60", "0.00",  "656.10",  "NET-30",None,      None,        "2024-01-09","2024-01-09 08:00:00","billing01"),
    ("INV-5024","INV-2024-5024","WO-10002","CUST-002","2024-01-08","2024-02-07","DRAFT",   "2210.00", "176.80","0.00",  "2386.80", "NET-30",None,      None,        "2024-01-08","2024-01-08 10:00:00","billing02"),

    # ── PROBLEM: Negative subtotal and totals ─────────────────────────────────────
    ("INV-5007","INV-2024-5007","WO-10009","CUST-001","2024-01-04","2024-02-03","PAID",    "-280.00", "-22.40","0.00",  "-302.40", "NET-30","-302.40", "2024-01-15","2024-01-04","2024-01-15 09:00:00","admin"),
    ("INV-5025","INV-2024-5025","WO-10032","CUST-025","2024-01-08","2024-02-07","PAID",    "-70.00",  "-5.60", "0.00",  "-75.60",  "NET-30","-75.60",  "2024-01-20","2024-01-08","2024-01-20 10:00:00","admin"),

    # ── PROBLEM: payment_received > total_amount (overpayment) ───────────────────
    ("INV-5008","INV-2024-5008","WO-10019","CUST-002","2024-01-08","2024-02-07","PAID",    "707.50",  "56.60", "0.00",  "764.10",  "NET-30","1500.00", "2024-01-20","2024-01-08","2024-01-20 10:00:00","billing02"),

    # ── PROBLEM: due_date before invoice_date ─────────────────────────────────────
    ("INV-5009","INV-2024-5009","WO-10020","CUST-025","2024-01-09","2024-01-05","SENT",    "212.50",  "17.00", "0.00",  "229.50",  "NET-30",None,      None,        "2024-01-09","2024-01-09 11:30:00","billing01"),
    ("INV-5026","INV-2024-5026","WO-10047","CUST-033","2024-01-07","2024-01-03","SENT",    "1540.00", "123.20","0.00",  "1663.20", "NET-30",None,      None,        "2024-01-07","2024-01-07 15:00:00","billing02"),

    # ── PROBLEM: Invalid invoice_status ───────────────────────────────────────────
    ("INV-5010","INV-2024-5010","WO-10016","CUST-003","2024-01-06","2024-02-05","AWAITING_APPROVAL","1590.00","127.20","0.00","1717.20","NET-30",None,None,"2024-01-06","2024-01-07 08:00:00","billing02"),
    ("INV-5027","INV-2024-5027","WO-10045","CUST-029","2024-01-09","2024-02-08","PENDING_PAYMENT",  "1335.00","106.80","0.00","1441.80","NET-30",None,None,"2024-01-09","2024-01-09 11:00:00","billing01"),

    # ── PROBLEM: PAID invoice with NULL payment_date ──────────────────────────────
    ("INV-5028","INV-2024-5028","WO-10048","CUST-034","2024-01-06","2024-02-05","PAID",    "640.00",  "51.20", "0.00",  "691.20",  "NET-30","691.20",  None,        "2024-01-06","2024-01-06 12:00:00","billing02"),

    # ── PROBLEM: discount_amount > subtotal (impossible) ─────────────────────────
    ("INV-5029","INV-2024-5029","WO-10050","CUST-054","2024-01-08","2024-02-07","SENT",    "1190.00", "95.20", "2000.00","-714.80", "NET-30",None,      None,        "2024-01-08","2024-01-08 15:00:00","billing01"),

    # ── PROBLEM: Zero total on non-draft, non-zero cost WO ───────────────────────
    ("INV-5030","INV-2024-5030","WO-10043","CUST-023","2024-01-04","2024-02-03","SENT",    "0.00",    "0.00",  "0.00",  "0.00",    "NET-30",None,      None,        "2024-01-04","2024-01-04 16:00:00","billing02"),

    # ── Good draft invoices for open WOs ─────────────────────────────────────────
    ("INV-5011","INV-2024-5011","WO-10014","CUST-028","2024-01-10","2024-02-09","DRAFT",   "0.00",    "0.00",  "0.00",  "0.00",    "NET-30",None,      None,        "2024-01-10","2024-01-10 09:30:00","billing01"),
    ("INV-5012","INV-2024-5012","WO-10015","CUST-023","2024-01-10","2024-02-09","DRAFT",   "0.00",    "0.00",  "0.00",  "0.00",    "NET-30",None,      None,        "2024-01-10","2024-01-10 10:00:00","billing01"),
]

inv_cols = [
    "invoice_id","invoice_number","work_order_id","customer_id","invoice_date","due_date",
    "invoice_status","subtotal","tax_amount","discount_amount","total_amount",
    "payment_terms","payment_received","payment_date","created_date","last_modified","created_by"
]
df_inv = spark.createDataFrame(
    [Row(**dict(zip(inv_cols, r)), _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in invoices]
)
df_inv.write.format("delta").mode("overwrite").saveAsTable(t("invoices"))
print(f"Loaded {df_inv.count()} invoice records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Work Order Assignments — 35 records

# COMMAND ----------

assignments = [
    # ── GOOD ──────────────────────────────────────────────────────────────────────
    ("ASGN-001","WO-10001","TECH-001","LEAD",    "2024-01-05","2024-01-08 08:00:00","2024-01-08 14:30:00","6.5", "COMPLETED"),
    ("ASGN-002","WO-10002","TECH-002","LEAD",    "2024-01-07","2024-01-07 06:00:00","2024-01-07 18:00:00","12.0","COMPLETED"),
    ("ASGN-003","WO-10003","TECH-001","SOLO",    "2024-01-06","2024-01-09 09:00:00","2024-01-09 12:00:00","3.0", "COMPLETED"),
    ("ASGN-009","WO-10005","TECH-005","SOLO",    "2024-01-04","2024-01-06 10:00:00","2024-01-06 11:30:00","1.5", "COMPLETED"),
    ("ASGN-010","WO-10019","TECH-002","LEAD",    "2024-01-05","2024-01-08 07:30:00","2024-01-08 15:00:00","7.5", "COMPLETED"),
    ("ASGN-011","WO-10020","TECH-014","SOLO",    "2024-01-07","2024-01-09 08:00:00","2024-01-09 10:30:00","2.5", "COMPLETED"),
    ("ASGN-012","WO-10016","TECH-014","LEAD",    "2024-01-06","2024-01-06 06:00:00","2024-01-06 20:00:00","14.0","COMPLETED"),
    ("ASGN-013","WO-10014","TECH-015","SOLO",    "2024-01-10",None,                 None,                 None,  "SCHEDULED"),
    ("ASGN-014","WO-10015","TECH-003","LEAD",    "2024-01-08","2024-01-10 08:00:00",None,                 None,  "ACTIVE"),
    ("ASGN-015","WO-10013","TECH-001","LEAD",    "2024-01-05","2024-01-07 08:00:00","2024-01-07 17:00:00","9.0", "COMPLETED"),
    ("ASGN-016","WO-10021","TECH-019","LEAD",    "2024-01-04","2024-01-06 08:00:00","2024-01-06 16:00:00","8.5", "COMPLETED"),
    ("ASGN-017","WO-10022","TECH-020","SOLO",    "2024-01-03","2024-01-05 09:00:00","2024-01-05 13:00:00","4.0", "COMPLETED"),
    ("ASGN-018","WO-10023","TECH-017","SOLO",    "2024-01-02","2024-01-04 10:00:00","2024-01-04 12:00:00","2.0", "COMPLETED"),
    ("ASGN-019","WO-10024","TECH-016","LEAD",    "2024-01-01","2024-01-03 07:00:00","2024-01-03 15:00:00","9.0", "COMPLETED"),
    ("ASGN-020","WO-10025","TECH-018","SOLO",    "2023-12-30","2024-01-02 08:00:00","2024-01-02 11:00:00","3.0", "COMPLETED"),
    ("ASGN-021","WO-10027","TECH-001","LEAD",    "2024-01-09","2024-01-09 02:00:00","2024-01-09 08:00:00","6.0", "COMPLETED"),
    ("ASGN-022","WO-10028","TECH-003","LEAD",    "2024-01-06","2024-01-08 09:00:00","2024-01-08 13:00:00","4.0", "COMPLETED"),
    ("ASGN-023","WO-10045","TECH-002","LEAD",    "2024-01-09","2024-01-09 03:00:00","2024-01-09 10:00:00","7.0", "COMPLETED"),
    ("ASGN-024","WO-10041","TECH-017","LEAD",    "2024-01-04","2024-01-06 09:00:00","2024-01-06 13:00:00","4.0", "COMPLETED"),
    ("ASGN-025","WO-10042","TECH-018","SOLO",    "2024-01-03","2024-01-05 09:00:00","2024-01-05 11:00:00","2.0", "COMPLETED"),

    # ── PROBLEM: Orphaned work_order_id ───────────────────────────────────────────
    ("ASGN-004","WO-88888","TECH-003","LEAD",    "2024-01-05","2024-01-08 09:00:00","2024-01-08 15:00:00","6.0", "COMPLETED"),
    ("ASGN-026","WO-55555","TECH-019","SUPPORT", "2024-01-03","2024-01-05 10:00:00","2024-01-05 14:00:00","4.0", "COMPLETED"),

    # ── PROBLEM: Orphaned technician_id ───────────────────────────────────────────
    ("ASGN-005","WO-10004","TECH-999","SUPPORT", "2024-01-09",None,                 None,                 None,  "ACTIVE"),
    ("ASGN-027","WO-10026","TECH-888","LEAD",    "2024-01-10",None,                 None,                 None,  "SCHEDULED"),

    # ── PROBLEM: Terminated tech on open WO ───────────────────────────────────────
    ("ASGN-006","WO-10015","TECH-006","LEAD",    "2024-01-08","2024-01-10 08:00:00",None,                 None,  "ACTIVE"),
    ("ASGN-028","WO-10036","TECH-007","SOLO",    "2023-09-19","2023-09-20 08:00:00",None,                 None,  "ACTIVE"),

    # ── PROBLEM: Same tech assigned twice to same WO ──────────────────────────────
    ("ASGN-007","WO-10001","TECH-001","LEAD",    "2024-01-05","2024-01-08 08:00:00","2024-01-08 14:30:00","6.5", "COMPLETED"),
    ("ASGN-029","WO-10002","TECH-002","SUPPORT", "2024-01-07","2024-01-07 08:00:00","2024-01-07 18:00:00","10.0","COMPLETED"),

    # ── PROBLEM: Negative hours_logged ────────────────────────────────────────────
    ("ASGN-008","WO-10007","TECH-004","SOLO",    "2024-01-03","2024-01-05 08:00:00","2024-01-05 10:30:00","-2.5","COMPLETED"),
    ("ASGN-030","WO-10038","TECH-020","SOLO",    "2024-01-06","2024-01-08 11:00:00","2024-01-08 12:30:00","-1.5","COMPLETED"),

    # ── PROBLEM: Invalid assignment_status ────────────────────────────────────────
    ("ASGN-031","WO-10043","TECH-016","LEAD",    "2024-01-02","2024-01-04 07:00:00","2024-01-04 15:00:00","8.0", "Done"),
    ("ASGN-032","WO-10047","TECH-019","LEAD",    "2024-01-05","2024-01-07 08:00:00","2024-01-07 14:00:00","6.0", "Finished"),

    # ── PROBLEM: end_time before start_time ───────────────────────────────────────
    ("ASGN-033","WO-10048","TECH-020","SOLO",    "2024-01-04","2024-01-06 11:00:00","2024-01-06 07:00:00","4.0", "COMPLETED"),

    # ── PROBLEM: hours_logged does not match start/end duration ──────────────────
    ("ASGN-034","WO-10050","TECH-014","LEAD",    "2024-01-06","2024-01-08 06:00:00","2024-01-08 14:00:00","2.0", "COMPLETED"),

    # ── PROBLEM: assignment_role not in allowed domain ────────────────────────────
    ("ASGN-035","WO-10046","TECH-017","OBSERVER","2024-01-06","2024-01-08 09:00:00","2024-01-08 11:00:00","2.0", "COMPLETED"),
]

asgn_cols = [
    "assignment_id","work_order_id","technician_id","assignment_role",
    "assigned_date","start_time","end_time","hours_logged","assignment_status"
]
df_asgn = spark.createDataFrame(
    [Row(**dict(zip(asgn_cols, r)), notes=None, _extract_timestamp=now, _batch_id=BATCH_ID)
     for r in assignments]
)
df_asgn.write.format("delta").mode("overwrite").saveAsTable(t("work_order_assignments"))
print(f"Loaded {df_asgn.count()} assignment records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 55)
print("  DIRTY DATA LOAD COMPLETE")
print("=" * 55)
for tbl in ["customers","technicians","work_orders","invoices","work_order_assignments"]:
    n = spark.table(t(tbl)).count()
    print(f"  {t(tbl):<45} {n:>4} rows")
print("\nNext: Run notebook 03_validation_framework")
