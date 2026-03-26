CREATE TABLE IF NOT EXISTS claims (
    claim_id        TEXT PRIMARY KEY,
    patient_id      TEXT,
    provider_id     TEXT,
    visit_date      TEXT,               -- stored as ISO-8601 string for flexibility
    discharge_date  TEXT,
    diagnosis_code  TEXT,
    procedure_code  TEXT,
    amount_billed   REAL,
    amount_paid     REAL,
    claim_status    TEXT,
    insurance_type  TEXT,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS icd10_reference (
    icd10_code      TEXT PRIMARY KEY,
    description     TEXT NOT NULL,
    category        TEXT
);



CREATE TABLE IF NOT EXISTS qa_rules (
    rule_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_code       TEXT UNIQUE NOT NULL,   -- e.g. QA-001
    rule_name       TEXT NOT NULL,
    description     TEXT,
    severity        TEXT CHECK(severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')),
    domain          TEXT,                   -- e.g. Completeness, Validity, Consistency
    is_enabled      INTEGER DEFAULT 1       -- 1 = active, 0 = disabled
);



CREATE TABLE IF NOT EXISTS qa_test_runs (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dataset_name    TEXT,
    total_records   INTEGER,
    total_issues    INTEGER,
    pass_count      INTEGER,
    fail_count      INTEGER,
    run_status      TEXT CHECK(run_status IN ('PASS', 'FAIL', 'PARTIAL')),
    run_duration_ms INTEGER
);



CREATE TABLE IF NOT EXISTS qa_results (
    result_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER REFERENCES qa_test_runs(run_id),
    rule_code       TEXT REFERENCES qa_rules(rule_code),
    claim_id        TEXT,
    patient_id      TEXT,
    field_name      TEXT,               -- which column triggered the issue
    field_value     TEXT,               -- the actual bad value found
    issue_detail    TEXT,               
    severity        TEXT,
    flagged_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



INSERT OR IGNORE INTO qa_rules (rule_code, rule_name, description, severity, domain) VALUES

-- COMPLETENESS: 
('QA-001', 'Missing Patient ID',
 'Every claim must be linked to a patient. A null patient_id means the claim cannot be attributed and will fail downstream member attribution.',
 'CRITICAL', 'Completeness'),

('QA-002', 'Missing Provider ID',
 'Claims must reference a valid provider. Missing provider_id breaks provider performance analytics and payment routing.',
 'HIGH', 'Completeness'),

('QA-003', 'Missing Visit Date',
 'visit_date is required for episode grouping and timeline analysis. Claims without a visit date cannot be included in care pathway reports.',
 'CRITICAL', 'Completeness'),

('QA-004', 'Missing Discharge Date',
 'Inpatient claims require a discharge_date to calculate length of stay and episode cost. Outpatient claims may be exempt.',
 'MEDIUM', 'Completeness'),

('QA-005', 'Missing Diagnosis Code',
 'A diagnosis code is mandatory for clinical classification. Claims without diagnosis codes cannot be risk-stratified.',
 'CRITICAL', 'Completeness'),

-- VALIDITY: 
('QA-006', 'Invalid ICD-10 Diagnosis Code',
 'Diagnosis code does not exist in the ICD-10-CM reference table. Invalid codes corrupt clinical analytics and quality measure calculations.',
 'HIGH', 'Validity'),

('QA-007', 'Future Visit Date',
 'Visit date is in the future, which is logically impossible for a completed claim. Likely a data entry error or system clock issue.',
 'HIGH', 'Validity'),

('QA-008', 'Negative Amount Paid',
 'amount_paid cannot be negative. Negative values indicate a data transformation error. Adjustments should use a separate claim with type ADJUSTMENT.',
 'CRITICAL', 'Validity'),

('QA-009', 'Invalid Claim Status',
 'claim_status must be one of: APPROVED, DENIED, PENDING. Unexpected values break downstream workflow routing.',
 'MEDIUM', 'Validity'),

-- CONSISTENCY:
('QA-010', 'Discharge Date Before Visit Date',
 'discharge_date cannot precede visit_date. This indicates a date transposition error in the source system.',
 'HIGH', 'Consistency'),

('QA-011', 'Amount Paid Exceeds Amount Billed',
 'amount_paid should not exceed amount_billed except in rare contractual adjustment scenarios. Flags potential overpayment.',
 'HIGH', 'Consistency'),

-- UNIQUENESS: Duplicate records inflate cost and utilisation metrics
('QA-012', 'Duplicate Claim Record',
 'An exact duplicate exists with the same claim_id, patient_id, visit_date, diagnosis_code, and amount. Duplicates cause double-counting in financial reports and analytics dashboards.',
 'CRITICAL', 'Uniqueness');
