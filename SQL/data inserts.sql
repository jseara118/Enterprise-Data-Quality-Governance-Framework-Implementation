-- ============================================================
-- E. DIRTY DATA INSERT SCRIPTS
-- FieldOps Pro — Intentionally Corrupt Source Data
-- Each block annotates the specific DQ problem being simulated
-- ============================================================

USE FieldOpsDQ;
GO

-- ============================================================
-- CUSTOMERS — 30 records with multiple DQ problems
-- ============================================================

INSERT INTO raw.customers
    (customer_id, customer_name, customer_type, account_status, credit_limit,
     primary_contact, email_address, phone_number,
     billing_city, billing_state, billing_zip, region_code,
     created_date, last_modified, created_by, source_system, _batch_id)
VALUES
-- GOOD records (baseline)
('CUST-001', 'Apex Power Corporation',         'ENTERPRISE', 'ACTIVE',   '250000.00', 'Sandra Okafor',       'sokafor@apexpower.com',       '(512) 555-0101', 'Austin',        'TX', '78701', 'SW', '2019-03-15', '2024-01-10 08:30:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-002', 'Valley Water District',          'GOVERNMENT', 'ACTIVE',   '500000.00', 'Marcus Chen',         'mchen@valleywater.gov',       '(916) 555-0202', 'Sacramento',    'CA', '95814', 'NW', '2018-07-22', '2024-01-08 14:15:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-003', 'Riverstone Gas & Electric',      'ENTERPRISE', 'ACTIVE',   '750000.00', 'Priya Nair',          'pnair@riverstone-ge.com',     '(713) 555-0303', 'Houston',       'TX', '77002', 'SW', '2017-11-01', '2024-01-09 11:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Duplicate customer — same company, slightly different name and status
('CUST-004', 'Apex Power Corporation',         'ENTERPRISE', 'ACTIVE',   '250000.00', 'Sandra Okafor',       'sokafor@apexpower.com',       '(512) 555-0101', 'Austin',        'TX', '78701', 'SW', '2019-03-15', '2024-01-11 09:00:00', 'mobile_sync', 'MOBILE_APP', 'BATCH-20240110'),
('CUST-005', 'APEX POWER CORP',                'Enterprise', 'Active',   '250000',    'S. Okafor',           'sokafor@apexpower.com',       '512-555-0101',   'Austin',        'TX', '78701', 'SW', '2019-03-15', '2024-01-11 09:01:00', 'legacy_import', 'LEGACY_DB', 'BATCH-20240110'),

-- PROBLEM: NULL in required fields (customer_name, account_status)
('CUST-006', NULL,                             'SMB',        'ACTIVE',   '15000.00',  'Tom Bradley',         'tbradley@company.com',        '(303) 555-0601', 'Denver',        'CO', '80203', 'CW', '2023-05-20', '2024-01-07 10:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-007', 'Pacific Northwest Utilities',   'ENTERPRISE', NULL,       '300000.00', 'Janet Wu',            'jwu@pnwutil.com',             '(503) 555-0701', 'Portland',      'OR', '97201', 'NW', '2020-08-14', '2024-01-06 15:30:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Invalid account_status (not in allowed domain)
('CUST-008', 'Summit Energy Partners',         'ENTERPRISE', 'PENDING_REVIEW',  '180000.00', 'Alex Torres',  'atorres@summitenergy.com',    '(720) 555-0801', 'Denver',        'CO', '80202', 'CW', '2022-12-01', '2024-01-05 08:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-009', 'Lakeside Municipal Water',       'GOVERNMENT', 'TEMP SUSPEND',    '100000.00', 'Rob Petersen',  'rpetersen@lakesidewater.gov', '(414) 555-0901', 'Milwaukee',     'WI', '53202', 'MW', '2021-06-10', '2024-01-04 12:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-010', 'Brightfield Solar LLC',          'SMB',        'activ',           '50000.00',  'Dana Kim',      'dkim@brightfieldsolar.com',   '(415) 555-1001', 'San Francisco', 'CA', '94102', 'NW', '2023-09-05', '2024-01-03 09:15:00', 'mobile_sync', 'MOBILE_APP', 'BATCH-20240110'),

-- PROBLEM: Malformed / future created_date
('CUST-011', 'Northern Grid Authority',        'GOVERNMENT', 'ACTIVE',   '400000.00', 'Christine Bell',      'cbell@norgrid.gov',           '(651) 555-1101', 'Minneapolis',   'MN', '55401', 'MW', '2027-01-01', '2024-01-02 07:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-012', 'MidWest Power Group',            'ENTERPRISE', 'ACTIVE',   '600000.00', 'James Porter',        'jporter@mwpowergroup.com',    '(312) 555-1201', 'Chicago',       'IL', '60601', 'MW', 'UNKNOWN',    '2024-01-01 14:00:00', 'legacy_import', 'LEGACY_DB', 'BATCH-20240110'),

-- PROBLEM: Negative credit_limit
('CUST-013', 'Clearwater Treatment Co',        'SMB',        'ACTIVE',   '-5000.00',  'Helen Park',          'hpark@clearwatertreat.com',   '(615) 555-1301', 'Nashville',     'TN', '37201', 'SE', '2022-03-18', '2023-12-28 10:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Invalid email format
('CUST-014', 'Desert Sun Energy',              'SMB',        'ACTIVE',   '75000.00',  'Miguel Reyes',        'not-an-email',                '(480) 555-1401', 'Phoenix',       'AZ', '85001', 'SW', '2023-01-15', '2023-12-30 11:00:00', 'mobile_sync', 'MOBILE_APP', 'BATCH-20240110'),
('CUST-015', 'Coastal Power Services',         'ENTERPRISE', 'ACTIVE',   '350000.00', 'Yuki Tanaka',         'ytanaka@coastalpwr',          '(858) 555-1501', 'San Diego',     'CA', '92101', 'NW', '2020-05-20', '2023-12-29 16:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Invalid state code
('CUST-016', 'Heartland Gas Distribution',     'ENTERPRISE', 'ACTIVE',   '220000.00', 'Brenda Walsh',        'bwalsh@heartlandgas.com',     '(913) 555-1601', 'Kansas City',   'KS ', '66101', 'MW', '2019-09-10', '2023-12-27 09:30:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-017', 'Blue Ridge Telecom',             'ENTERPRISE', 'ACTIVE',   '190000.00', 'Larry Stone',         'lstone@blueridgetelecom.com', '(704) 555-1701', 'Charlotte',     'NC', '28201', 'SE', '2021-04-22', '2023-12-26 13:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: customer_type not in allowed domain
('CUST-018', 'Granite Falls Mining Corp',      'INDUSTRIAL', 'ACTIVE',   '450000.00', 'Oscar Nielsen',       'onielsen@granitefalls.com',   '(907) 555-1801', 'Anchorage',     'AK', '99501', 'NW', '2018-02-14', '2023-12-25 10:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: credit_limit stored as text/garbage value
('CUST-019', 'Sunridge Residential HOA',       'SMB',        'ACTIVE',   'TBD',       'Fiona Bell',          'fbell@sunridgehoa.org',       '(858) 555-1901', 'Escondido',     'CA', '92025', 'NW', '2023-11-01', '2023-12-24 08:00:00', 'mobile_sync', 'MOBILE_APP', 'BATCH-20240110'),
('CUST-020', 'Eastern Corridor Rail',          'ENTERPRISE', 'ACTIVE',   'N/A',       'George Huang',        'ghuang@ecrail.com',           '(617) 555-2001', 'Boston',        'MA', '02101', 'NE', '2022-07-30', '2023-12-23 14:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Stale last_modified (over 2 years old — freshness issue)
('CUST-021', 'Pacific Gas Holdings',           'ENTERPRISE', 'ACTIVE',   '500000.00', 'Rachel Green',        'rgreen@pacgashold.com',       '(213) 555-2101', 'Los Angeles',   'CA', '90001', 'NW', '2015-03-01', '2021-06-30 12:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Completely missing primary_contact, email, phone
('CUST-022', 'Northgate Industrial Park',      'SMB',        'ACTIVE',   '30000.00',  NULL,                  NULL,                          NULL,             'Seattle',       'WA', '98101', 'NW', '2023-08-15', '2023-12-22 10:30:00', 'mobile_sync', 'MOBILE_APP', 'BATCH-20240110'),

-- Good records continued
('CUST-023', 'Sunbelt Power Cooperative',      'GOVERNMENT', 'ACTIVE',   '280000.00', 'Calvin Brooks',       'cbrooks@sunbeltcoop.gov',     '(404) 555-2301', 'Atlanta',       'GA', '30301', 'SE', '2016-11-08', '2024-01-09 07:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-024', 'Alpine Water Authority',         'GOVERNMENT', 'INACTIVE', '150000.00', 'Donna Fischer',       'dfischer@alpinewater.gov',    '(801) 555-2401', 'Salt Lake City','UT', '84101', 'NW', '2019-05-12', '2024-01-08 11:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-025', 'Metro Grid Solutions',           'ENTERPRISE', 'ACTIVE',   '320000.00', 'Barry White',         'bwhite@metrogrid.com',        '(212) 555-2501', 'New York',      'NY', '10001', 'NE', '2017-09-19', '2024-01-07 13:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Orphaned billing state (non-US code)
('CUST-026', 'TransCanada Operations',         'ENTERPRISE', 'ACTIVE',   '800000.00', 'Lena Marchetti',      'lmarchetti@transcanada.com',  '(403) 555-2601', 'Calgary',       'AB', 'T2P0S8', 'NW', '2020-01-15', '2024-01-06 09:30:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Exact duplicate of CUST-002
('CUST-027', 'Valley Water District',          'GOVERNMENT', 'ACTIVE',   '500000.00', 'Marcus Chen',         'mchen@valleywater.gov',       '(916) 555-0202', 'Sacramento',    'CA', '95814', 'NW', '2018-07-22', '2024-01-08 14:15:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- Good records
('CUST-028', 'SolarField Energy Inc',          'SMB',        'ACTIVE',   '65000.00',  'Natasha Rivera',      'nrivera@solarfield.com',      '(702) 555-2801', 'Las Vegas',     'NV', '89101', 'SW', '2023-02-28', '2024-01-05 10:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-029', 'Great Lakes Utilities',          'ENTERPRISE', 'ACTIVE',   '420000.00', 'Kenneth Shaw',        'kshaw@greatlakesutil.com',    '(313) 555-2901', 'Detroit',       'MI', '48201', 'MW', '2018-04-03', '2024-01-04 08:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),
('CUST-030', 'Gulf Shore Pipeline Co',         'ENTERPRISE', 'SUSPENDED','200000.00', 'Irene Castro',        'icastro@gulfshore.com',       '(504) 555-3001', 'New Orleans',   'LA', '70112', 'SE', '2016-08-22', '2024-01-03 14:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110');
GO

-- ============================================================
-- TECHNICIANS — Dirty data simulation
-- ============================================================

INSERT INTO raw.technicians
    (technician_id, employee_number, first_name, last_name, full_name,
     employment_status, skill_level, home_region, hire_date, termination_date,
     hourly_rate, manager_id, email, mobile_phone, created_date, last_modified, _batch_id)
VALUES
-- Good records
('TECH-001', 'EMP-10045', 'Carlos',  'Mendez',    'Carlos Mendez',    'ACTIVE',      'SENIOR',      'SW', '2015-06-01', NULL,         '65.00',  'MGR-001', 'cmendez@meridian.com',     '(512) 555-4001', '2015-06-01', '2024-01-10 08:00:00', 'BATCH-20240110'),
('TECH-002', 'EMP-10046', 'Amara',   'Osei',      'Amara Osei',       'ACTIVE',      'SENIOR',      'NW', '2016-03-15', NULL,         '68.00',  'MGR-002', 'aosei@meridian.com',       '(503) 555-4002', '2016-03-15', '2024-01-10 07:45:00', 'BATCH-20240110'),
('TECH-003', 'EMP-10047', 'Steven',  'Kowalski',  'Steven Kowalski',  'ACTIVE',      'MID',         'MW', '2018-09-10', NULL,         '52.00',  'MGR-003', 'skowalski@meridian.com',   '(312) 555-4003', '2018-09-10', '2024-01-10 09:00:00', 'BATCH-20240110'),
('TECH-004', 'EMP-10048', 'Leila',   'Ahmadi',    'Leila Ahmadi',     'ACTIVE',      'JUNIOR',      'SW', '2022-01-20', NULL,         '38.00',  'MGR-001', 'lahmadi@meridian.com',     '(713) 555-4004', '2022-01-20', '2024-01-09 10:00:00', 'BATCH-20240110'),

-- PROBLEM: Inconsistent employment_status values
('TECH-005', 'EMP-10049', 'Brian',   'Nguyen',    'Brian Nguyen',     'active',      'Senior',      'NW', '2014-11-03', NULL,         '70.00',  'MGR-002', 'bnguyen@meridian.com',     '(206) 555-4005', '2014-11-03', '2024-01-08 08:00:00', 'BATCH-20240110'),
('TECH-006', 'EMP-10050', 'Diana',   'Ross',      'Diana Ross',       'Terminated',  'SENIOR',      'SE', '2013-05-12', '2023-11-30', '72.00',  'MGR-004', 'dross@meridian.com',       '(404) 555-4006', '2013-05-12', '2023-12-01 00:00:00', 'BATCH-20240110'),
('TECH-007', 'EMP-10051', 'Ahmad',   'Hassan',    'Ahmad Hassan',     'TERMINATED',  'MID',         'MW', '2019-07-08', '2024-01-05', '50.00',  'MGR-003', 'ahassan@meridian.com',     '(312) 555-4007', '2019-07-08', '2024-01-06 00:00:00', 'BATCH-20240110'),
('TECH-008', 'EMP-10052', 'Patrice', 'Dubois',    'Patrice Dubois',   'ON LEAVE',    'SENIOR',      'NE', '2017-02-28', NULL,         '66.00',  'MGR-005', 'pdubois@meridian.com',     '(617) 555-4008', '2017-02-28', '2024-01-07 11:00:00', 'BATCH-20240110'),

-- PROBLEM: Negative hourly_rate
('TECH-009', 'EMP-10053', 'Jerome',  'Banks',     'Jerome Banks',     'ACTIVE',      'JUNIOR',      'SE', '2023-06-05', NULL,         '-38.50', 'MGR-004', 'jbanks@meridian.com',      '(813) 555-4009', '2023-06-05', '2024-01-05 09:00:00', 'BATCH-20240110'),

-- PROBLEM: hire_date after termination_date (logically invalid)
('TECH-010', 'EMP-10054', 'Sara',    'Lindqvist', 'Sara Lindqvist',   'TERMINATED',  'MID',         'NW', '2023-12-01', '2022-06-30', '55.00',  'MGR-002', 'slindqvist@meridian.com',  '(503) 555-4010', '2023-12-01', '2023-12-31 00:00:00', 'BATCH-20240110'),

-- PROBLEM: NULL employee_number, NULL skill_level
('TECH-011', NULL,         'Marcus',  'Cole',      'Marcus Cole',      'ACTIVE',      NULL,          'SW', '2021-03-14', NULL,         '48.00',  'MGR-001', 'mcole@meridian.com',       '(602) 555-4011', '2021-03-14', '2024-01-04 12:00:00', 'BATCH-20240110'),

-- PROBLEM: Future hire_date
('TECH-012', 'EMP-10056', 'Nina',    'Petrov',    'Nina Petrov',      'ACTIVE',      'JUNIOR',      'NE', '2026-01-01', NULL,         '40.00',  'MGR-005', 'npetrov@meridian.com',     '(212) 555-4012', '2026-01-01', '2024-01-10 08:00:00', 'BATCH-20240110'),

-- PROBLEM: Duplicate technician (same employee, two IDs)
('TECH-013', 'EMP-10045', 'Carlos',  'Mendez',    'Carlos Mendez',    'ACTIVE',      'SENIOR',      'SW', '2015-06-01', NULL,         '65.00',  'MGR-001', 'cmendez@meridian.com',     '(512) 555-4001', '2015-06-01', '2024-01-10 09:30:00', 'BATCH-20240110'),

-- Good additional records
('TECH-014', 'EMP-10057', 'Kofi',    'Antwi',     'Kofi Antwi',       'ACTIVE',      'SENIOR',      'SE', '2016-08-22', NULL,         '67.00',  'MGR-004', 'kantwi@meridian.com',      '(404) 555-4014', '2016-08-22', '2024-01-09 08:00:00', 'BATCH-20240110'),
('TECH-015', 'EMP-10058', 'Tanya',   'Morrison',  'Tanya Morrison',   'ACTIVE',      'MID',         'CW', '2020-04-06', NULL,         '54.00',  'MGR-006', 'tmorrison@meridian.com',   '(720) 555-4015', '2020-04-06', '2024-01-08 14:00:00', 'BATCH-20240110');
GO

-- ============================================================
-- WORK ORDERS — The richest table for DQ problems
-- ============================================================

INSERT INTO raw.work_orders
    (work_order_id, work_order_number, site_id, customer_id, work_order_type,
     priority, status, description, scheduled_date, actual_start, actual_end,
     estimated_hours, actual_hours, labor_cost, parts_cost, total_cost,
     sla_due_date, sla_met, resolution_code, created_date, last_modified,
     created_by, source_system, _batch_id)
VALUES
-- Good baseline records
('WO-10001', 'WO-2024-10001', 'SITE-001', 'CUST-001', 'PREVENTIVE_MAINTENANCE', 'HIGH',   'COMPLETED', 'Annual transformer inspection and maintenance', '2024-01-08', '2024-01-08 08:00:00', '2024-01-08 14:30:00', '6.0',  '6.5',  '422.50',  '185.00',  '607.50',  '2024-01-10 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-05', '2024-01-08 15:00:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),
('WO-10002', 'WO-2024-10002', 'SITE-002', 'CUST-002', 'CORRECTIVE_MAINTENANCE', 'CRITICAL','COMPLETED', 'Emergency pump failure at main station',       '2024-01-07', '2024-01-07 06:00:00', '2024-01-07 18:00:00', '10.0', '12.0', '960.00',  '1250.00', '2210.00', '2024-01-07 12:00:00', 'N', 'RESOLVED_PARTIAL',  '2024-01-07', '2024-01-07 18:30:00', 'dispatch02', 'FIELDOPS_APP', 'BATCH-20240110'),
('WO-10003', 'WO-2024-10003', 'SITE-003', 'CUST-003', 'INSPECTION',             'MEDIUM', 'COMPLETED', 'Pipeline pressure test — segment B7',          '2024-01-09', '2024-01-09 09:00:00', '2024-01-09 12:00:00', '3.0',  '3.0',  '195.00',  '0.00',    '195.00',  '2024-01-12 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-06', '2024-01-09 12:30:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: status not in allowed domain
('WO-10004', 'WO-2024-10004', 'SITE-004', 'CUST-004', 'CORRECTIVE_MAINTENANCE', 'HIGH',   'IN PROGRESS', 'Faulty circuit breaker — Building C', '2024-01-10', NULL, NULL, '4.0', NULL, NULL, NULL, NULL, '2024-01-12 17:00:00', NULL, NULL, '2024-01-09', '2024-01-10 11:00:00', 'dispatch03', 'FIELDOPS_APP', 'BATCH-20240110'),
('WO-10005', 'WO-2024-10005', 'SITE-005', 'CUST-005', 'INSPECTION',             'LOW',    'Completed',   'Meter calibration — customer site',   '2024-01-06', '2024-01-06 10:00:00', '2024-01-06 11:30:00', '2.0', '1.5', '97.50', '0.00', '97.50', '2024-01-09 17:00:00', 'y', 'RESOLVED_COMPLETE', '2024-01-04', '2024-01-06 12:00:00', 'mobile_user', 'MOBILE_APP', 'BATCH-20240110'),

-- PROBLEM: actual_end BEFORE actual_start (logically impossible)
('WO-10006', 'WO-2024-10006', 'SITE-006', 'CUST-003', 'EMERGENCY_REPAIR',       'CRITICAL','COMPLETED',  'Gas leak detected — isolation required', '2024-01-08', '2024-01-08 14:00:00', '2024-01-08 09:00:00', '5.0', '5.0', '325.00', '750.00', '1075.00', '2024-01-08 16:00:00', 'N', 'RESOLVED_COMPLETE', '2024-01-08', '2024-01-08 14:30:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: NULL site_id (orphan work order)
('WO-10007', 'WO-2024-10007', NULL,       'CUST-002', 'PREVENTIVE_MAINTENANCE', 'MEDIUM', 'COMPLETED',   'Valve inspection — sector 4',           '2024-01-05', '2024-01-05 08:00:00', '2024-01-05 10:30:00', '3.0', '2.5', '162.50', '0.00', '162.50', '2024-01-08 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-03', '2024-01-05 11:00:00', 'dispatch02', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Orphaned customer_id (does not exist in customers table)
('WO-10008', 'WO-2024-10008', 'SITE-008', 'CUST-999', 'CORRECTIVE_MAINTENANCE', 'HIGH',   'COMPLETED',   'Replace failed relay — panel 7',        '2024-01-06', '2024-01-06 07:30:00', '2024-01-06 14:00:00', '7.0', '6.5', '422.50', '320.00', '742.50', '2024-01-09 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-04', '2024-01-06 14:30:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Negative labor_cost and parts_cost
('WO-10009', 'WO-2024-10009', 'SITE-009', 'CUST-001', 'PREVENTIVE_MAINTENANCE', 'LOW',    'COMPLETED',   'Annual cooling tower service',          '2024-01-04', '2024-01-04 09:00:00', '2024-01-04 12:00:00', '3.0', '3.0', '-195.00', '-85.00', '-280.00', '2024-01-07 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-02', '2024-01-04 12:30:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Future scheduled_date combined with COMPLETED status (impossible)
('WO-10010', 'WO-2024-10010', 'SITE-010', 'CUST-023', 'INSPECTION',             'MEDIUM', 'COMPLETED',   'Fire suppression system check',         '2025-06-15', '2025-06-15 09:00:00', '2025-06-15 11:00:00', '2.0', '2.0', '130.00', '0.00', '130.00', '2025-06-18 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-09', '2024-01-10 08:00:00', 'admin', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: NULL description, NULL priority on non-draft WO
('WO-10011', 'WO-2024-10011', 'SITE-011', 'CUST-025', 'CORRECTIVE_MAINTENANCE', NULL,     'COMPLETED',   NULL,                                    '2024-01-03', '2024-01-03 10:00:00', '2024-01-03 13:00:00', '3.0', '3.0', '195.00', '120.00', '315.00', '2024-01-06 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-02', '2024-01-03 13:30:00', 'dispatch03', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: total_cost does not equal labor_cost + parts_cost
('WO-10012', 'WO-2024-10012', 'SITE-012', 'CUST-029', 'PREVENTIVE_MAINTENANCE', 'HIGH',   'COMPLETED',   'Substation battery bank replacement',   '2024-01-09', '2024-01-09 07:00:00', '2024-01-09 17:00:00', '10.0', '10.0', '650.00', '4200.00', '3500.00', '2024-01-12 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-06', '2024-01-09 17:30:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: actual_hours exceeds 24 in a single day (impossible for 1 tech)
('WO-10013', 'WO-2024-10013', 'SITE-013', 'CUST-017', 'CORRECTIVE_MAINTENANCE', 'HIGH',   'COMPLETED',   'Full switchgear overhaul',              '2024-01-07', '2024-01-07 08:00:00', '2024-01-07 17:00:00', '8.0', '31.0', '2015.00', '1200.00', '3215.00', '2024-01-10 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-05', '2024-01-07 17:30:00', 'dispatch02', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Exact duplicate WO (same WO_ID, different insert)
('WO-10001', 'WO-2024-10001', 'SITE-001', 'CUST-001', 'PREVENTIVE_MAINTENANCE', 'HIGH',   'COMPLETED',   'Annual transformer inspection and maintenance', '2024-01-08', '2024-01-08 08:00:00', '2024-01-08 14:30:00', '6.0', '6.5', '422.50', '185.00', '607.50', '2024-01-10 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-05', '2024-01-08 15:00:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- Good records continued
('WO-10014', 'WO-2024-10014', 'SITE-014', 'CUST-028', 'INSPECTION',             'LOW',    'SCHEDULED',   'Solar panel array efficiency test',     '2024-01-15', NULL, NULL, '4.0', NULL, NULL, NULL, NULL, '2024-01-18 17:00:00', NULL, NULL, '2024-01-10', '2024-01-10 09:00:00', 'dispatch03', 'FIELDOPS_APP', 'BATCH-20240110'),
('WO-10015', 'WO-2024-10015', 'SITE-015', 'CUST-023', 'PREVENTIVE_MAINTENANCE', 'MEDIUM', 'IN_PROGRESS', 'HVAC plant service — Building A and B', '2024-01-10', '2024-01-10 08:00:00', NULL, '6.0', NULL, NULL, NULL, NULL, '2024-01-13 17:00:00', NULL, NULL, '2024-01-08', '2024-01-10 10:00:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: SLA_MET = 'Y' but actual completion is AFTER sla_due_date (contradiction)
('WO-10016', 'WO-2024-10016', 'SITE-016', 'CUST-003', 'EMERGENCY_REPAIR',       'CRITICAL','COMPLETED',  'Pressure relief valve replacement',     '2024-01-06', '2024-01-06 06:00:00', '2024-01-06 20:00:00', '8.0', '14.0', '910.00', '680.00', '1590.00', '2024-01-06 12:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-06', '2024-01-06 20:30:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: work_order_type not in allowed domain
('WO-10017', 'WO-2024-10017', 'SITE-017', 'CUST-016', 'SITE_SURVEY',            'LOW',    'COMPLETED',   'New site assessment for expansion',     '2024-01-04', '2024-01-04 13:00:00', '2024-01-04 15:00:00', '2.0', '2.0', '130.00', '0.00', '130.00', '2024-01-07 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-03', '2024-01-04 15:30:00', 'dispatch02', 'FIELDOPS_APP', 'BATCH-20240110'),

-- PROBLEM: Stale WO — last_modified over 90 days ago while status = IN_PROGRESS
('WO-10018', 'WO-2024-10018', 'SITE-018', 'CUST-029', 'CORRECTIVE_MAINTENANCE', 'HIGH',   'IN_PROGRESS', 'Transformer oil replacement — Unit 3',  '2023-08-15', '2023-08-15 09:00:00', NULL, '6.0', NULL, NULL, NULL, NULL, '2023-08-18 17:00:00', NULL, NULL, '2023-08-14', '2023-08-15 10:00:00', 'dispatch01', 'FIELDOPS_APP', 'BATCH-20240110'),

-- Good records
('WO-10019', 'WO-2024-10019', 'SITE-019', 'CUST-002', 'PREVENTIVE_MAINTENANCE', 'MEDIUM', 'COMPLETED',   'Pump station annual service',           '2024-01-08', '2024-01-08 07:30:00', '2024-01-08 15:00:00', '7.0', '7.5', '487.50', '220.00', '707.50', '2024-01-11 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-05', '2024-01-08 15:30:00', 'dispatch02', 'FIELDOPS_APP', 'BATCH-20240110'),
('WO-10020', 'WO-2024-10020', 'SITE-020', 'CUST-025', 'INSPECTION',             'HIGH',   'COMPLETED',   'Electrical distribution board check',   '2024-01-09', '2024-01-09 08:00:00', '2024-01-09 10:30:00', '3.0', '2.5', '162.50', '50.00', '212.50', '2024-01-11 17:00:00', 'Y', 'RESOLVED_COMPLETE', '2024-01-07', '2024-01-09 11:00:00', 'dispatch03', 'FIELDOPS_APP', 'BATCH-20240110');
GO

-- ============================================================
-- INVOICES — Financial data with DQ problems
-- ============================================================

INSERT INTO raw.invoices
    (invoice_id, invoice_number, work_order_id, customer_id, invoice_date, due_date,
     invoice_status, subtotal, tax_amount, discount_amount, total_amount,
     payment_terms, payment_received, payment_date, created_date, last_modified, created_by, _batch_id)
VALUES
-- Good records
('INV-5001', 'INV-2024-5001', 'WO-10001', 'CUST-001', '2024-01-08', '2024-02-07', 'PAID',    '607.50',  '48.60',  '0.00',   '656.10',  'NET-30', '656.10',  '2024-01-25', '2024-01-08', '2024-01-25 10:00:00', 'billing01', 'BATCH-20240110'),
('INV-5002', 'INV-2024-5002', 'WO-10002', 'CUST-002', '2024-01-07', '2024-02-06', 'PAID',    '2210.00', '176.80', '0.00',   '2386.80', 'NET-30', '2386.80', '2024-01-20', '2024-01-07', '2024-01-20 09:00:00', 'billing01', 'BATCH-20240110'),
('INV-5003', 'INV-2024-5003', 'WO-10003', 'CUST-003', '2024-01-09', '2024-02-08', 'SENT',    '195.00',  '15.60',  '0.00',   '210.60',  'NET-30', NULL,      NULL,         '2024-01-09', '2024-01-09 13:00:00', 'billing02', 'BATCH-20240110'),

-- PROBLEM: invoice total_amount does not balance (subtotal + tax - discount != total)
('INV-5004', 'INV-2024-5004', 'WO-10012', 'CUST-029', '2024-01-09', '2024-02-08', 'SENT',    '3500.00', '280.00', '0.00',   '4200.00', 'NET-30', NULL,      NULL,         '2024-01-09', '2024-01-09 18:00:00', 'billing01', 'BATCH-20240110'),

-- PROBLEM: Orphaned invoice — work_order_id does not exist
('INV-5005', 'INV-2024-5005', 'WO-99999', 'CUST-001', '2024-01-06', '2024-02-05', 'PAID',    '450.00',  '36.00',  '0.00',   '486.00',  'NET-30', '486.00',  '2024-01-22', '2024-01-06', '2024-01-22 11:00:00', 'billing02', 'BATCH-20240110'),

-- PROBLEM: Duplicate invoice for same work order
('INV-5006', 'INV-2024-5006', 'WO-10001', 'CUST-001', '2024-01-09', '2024-02-08', 'SENT',    '607.50',  '48.60',  '0.00',   '656.10',  'NET-30', NULL,      NULL,         '2024-01-09', '2024-01-09 08:00:00', 'billing01', 'BATCH-20240110'),

-- PROBLEM: Negative subtotal
('INV-5007', 'INV-2024-5007', 'WO-10009', 'CUST-001', '2024-01-04', '2024-02-03', 'PAID',    '-280.00', '-22.40', '0.00',   '-302.40', 'NET-30', '-302.40', '2024-01-15', '2024-01-04', '2024-01-15 09:00:00', 'admin', 'BATCH-20240110'),

-- PROBLEM: payment_received > total_amount (overpayment not flagged)
('INV-5008', 'INV-2024-5008', 'WO-10019', 'CUST-002', '2024-01-08', '2024-02-07', 'PAID',    '707.50',  '56.60',  '0.00',   '764.10',  'NET-30', '1500.00', '2024-01-20', '2024-01-08', '2024-01-20 10:00:00', 'billing02', 'BATCH-20240110'),

-- PROBLEM: due_date before invoice_date
('INV-5009', 'INV-2024-5009', 'WO-10020', 'CUST-025', '2024-01-09', '2024-01-05', 'SENT',    '212.50',  '17.00',  '0.00',   '229.50',  'NET-30', NULL,      NULL,         '2024-01-09', '2024-01-09 11:30:00', 'billing01', 'BATCH-20240110'),

-- PROBLEM: invoice_status not in allowed domain
('INV-5010', 'INV-2024-5010', 'WO-10016', 'CUST-003', '2024-01-06', '2024-02-05', 'AWAITING_APPROVAL', '1590.00', '127.20', '0.00', '1717.20', 'NET-30', NULL, NULL, '2024-01-06', '2024-01-07 08:00:00', 'billing02', 'BATCH-20240110'),

-- Good records
('INV-5011', 'INV-2024-5011', 'WO-10014', 'CUST-028', '2024-01-10', '2024-02-09', 'DRAFT',   '0.00',    '0.00',   '0.00',   '0.00',    'NET-30', NULL,      NULL,         '2024-01-10', '2024-01-10 09:30:00', 'billing01', 'BATCH-20240110'),
('INV-5012', 'INV-2024-5012', 'WO-10015', 'CUST-023', '2024-01-10', '2024-02-09', 'DRAFT',   '0.00',    '0.00',   '0.00',   '0.00',    'NET-30', NULL,      NULL,         '2024-01-10', '2024-01-10 10:00:00', 'billing01', 'BATCH-20240110');
GO

-- ============================================================
-- WORK ORDER ASSIGNMENTS — Referential and logic problems
-- ============================================================

INSERT INTO raw.work_order_assignments
    (assignment_id, work_order_id, technician_id, assignment_role,
     assigned_date, start_time, end_time, hours_logged, assignment_status, _batch_id)
VALUES
-- Good records
('ASGN-001', 'WO-10001', 'TECH-001', 'LEAD',      '2024-01-05', '2024-01-08 08:00:00', '2024-01-08 14:30:00', '6.5',  'COMPLETED', 'BATCH-20240110'),
('ASGN-002', 'WO-10002', 'TECH-002', 'LEAD',      '2024-01-07', '2024-01-07 06:00:00', '2024-01-07 18:00:00', '12.0', 'COMPLETED', 'BATCH-20240110'),
('ASGN-003', 'WO-10003', 'TECH-001', 'SOLO',      '2024-01-06', '2024-01-09 09:00:00', '2024-01-09 12:00:00', '3.0',  'COMPLETED', 'BATCH-20240110'),

-- PROBLEM: Orphaned work_order_id (WO does not exist)
('ASGN-004', 'WO-88888', 'TECH-003',  'LEAD',      '2024-01-05', '2024-01-08 09:00:00', '2024-01-08 15:00:00', '6.0',  'COMPLETED', 'BATCH-20240110'),

-- PROBLEM: Orphaned technician_id (tech does not exist)
('ASGN-005', 'WO-10004', 'TECH-999',  'SUPPORT',   '2024-01-09', NULL, NULL, NULL, 'ACTIVE', 'BATCH-20240110'),

-- PROBLEM: Terminated technician assigned to open WO (business rule violation)
('ASGN-006', 'WO-10015', 'TECH-006',  'LEAD',      '2024-01-08', '2024-01-10 08:00:00', NULL, NULL, 'ACTIVE', 'BATCH-20240110'),

-- PROBLEM: Same technician assigned twice to same WO
('ASGN-007', 'WO-10001', 'TECH-001', 'LEAD',      '2024-01-05', '2024-01-08 08:00:00', '2024-01-08 14:30:00', '6.5',  'COMPLETED', 'BATCH-20240110'),

-- PROBLEM: hours_logged is negative
('ASGN-008', 'WO-10007', 'TECH-004',  'SOLO',      '2024-01-03', '2024-01-05 08:00:00', '2024-01-05 10:30:00', '-2.5', 'COMPLETED', 'BATCH-20240110'),

-- Good records
('ASGN-009', 'WO-10005', 'TECH-005',  'SOLO',      '2024-01-04', '2024-01-06 10:00:00', '2024-01-06 11:30:00', '1.5',  'COMPLETED', 'BATCH-20240110'),
('ASGN-010', 'WO-10019', 'TECH-002',  'LEAD',      '2024-01-05', '2024-01-08 07:30:00', '2024-01-08 15:00:00', '7.5',  'COMPLETED', 'BATCH-20240110'),
('ASGN-011', 'WO-10020', 'TECH-014',  'SOLO',      '2024-01-07', '2024-01-09 08:00:00', '2024-01-09 10:30:00', '2.5',  'COMPLETED', 'BATCH-20240110'),
('ASGN-012', 'WO-10016', 'TECH-014',  'LEAD',      '2024-01-06', '2024-01-06 06:00:00', '2024-01-06 20:00:00', '14.0', 'COMPLETED', 'BATCH-20240110'),
('ASGN-013', 'WO-10014', 'TECH-015',  'SOLO',      '2024-01-10', NULL, NULL, NULL, 'SCHEDULED', 'BATCH-20240110'),
('ASGN-014', 'WO-10015', 'TECH-003',  'LEAD',      '2024-01-08', '2024-01-10 08:00:00', NULL, NULL, 'ACTIVE', 'BATCH-20240110'),
('ASGN-015', 'WO-10013', 'TECH-001',  'LEAD',      '2024-01-05', '2024-01-07 08:00:00', '2024-01-07 17:00:00', '9.0',  'COMPLETED', 'BATCH-20240110');

GO