# Healthcare Claims Data QA System

A SQL-driven data quality assurance pipeline for healthcare claims data — built to simulate the kind of validation work done in real-world healthcare data pipelines at companies like Cedar Gate Technologies.



About this Project:

Healthcare data is complex. A single insurance company processes millions of claims per year — each claim representing a patient visit, a diagnosis, a procedure, and a payment. Before any of that data can feed into analytics dashboards, risk models, or payment systems, it needs to be validated.

Bad data in healthcare isn't just an inconvenience:
- A missing patient ID means a claim can't be attributed to a member — breaking care management workflows.
- An invalid diagnosis code corrupts clinical quality measures and risk adjustment scores.
- A duplicate claim causes a double payment — a direct financial loss.
- A future-dated visit date indicates a broken ETL process that will silently poison downstream reports.

This project implements a complete structured QA pipeline that catches these problems systematically, using SQL as the validation engine — because that's where healthcare data actually lives.



# What It Does:

The pipeline loads a batch of healthcare claims, runs 12 validation rules across four QA dimensions, and produces a detailed report of every issue found — along with the severity, affected field, and a plain-English explanation of why it matters.

[1/5] Initialise database schema and reference tables
[2/5] Load ICD-10-CM diagnosis code reference data
[3/5] Load claims batch from CSV
[4/5] Execute 12 SQL validation rules
[5/5] Generate QA report → output/qa_report_<timestamp>.txt


##Example Output

Running the pipeline against the included sample dataset produces:

========================================================================
  HEALTHCARE CLAIMS DATA — QA VALIDATION REPORT
========================================================================
  Run ID       : 1
  Dataset      : sample_claims.csv
  Status       : FAIL

  SUMMARY
  ----------------------------------------
  Total records loaded  : 40
  Clean records         : 25  (62.5%)
  Records with issues   : 15
  Total issues found    : 15

  ISSUES BY SEVERITY
  ----------------------------------------
  [!!!] CRITICAL :    5  (data must NOT be promoted)
  [!! ]     HIGH :    9  (investigate before next run)
  [ ! ]   MEDIUM :    1  (flag for analyst review)

  ISSUES BY RULE
  ----------------------------------------
  QA-001  :  2  Missing Patient ID
  QA-006  :  3  Invalid ICD-10 Diagnosis Code
  QA-007  :  2  Future Visit Date
  QA-010  :  3  Discharge Date Before Visit Date
  QA-012  :  2  Duplicate Claim Record
  ...

DETAILED FINDINGS (excerpt):

  [!!!] QA-012 — Duplicate Claim Record
  Domain: Uniqueness | Severity: CRITICAL
  -----------------------------------------------------------------------
  Claim     : C1001
  Patient   : P2001
  Field     : claim_id  =  "C1001"
  Detail    : Duplicate claim detected. Another claim [C1010] exists for
              the same patient [P2001] on visit date [2024-01-10] with
              diagnosis [A09.0] and billed amount [350.0]. Duplicates
              inflate financial totals and must be investigated before
              data promotion.




# Validation Rules

Rules are organised by QA domain, mirroring how real data quality frameworks categorise checks.

| Rule   | Severity | Domain       | What It Checks |
|--------|----------|--------------|----------------|
| QA-001 | CRITICAL | Completeness | Patient ID is not null |
| QA-002 | HIGH     | Completeness | Provider ID is not null |
| QA-003 | CRITICAL | Completeness | Visit date is not null |
| QA-004 | MEDIUM   | Completeness | Discharge date is not null |
| QA-005 | CRITICAL | Completeness | Diagnosis code is not null |
| QA-006 | HIGH     | Validity     | Diagnosis code exists in ICD-10-CM reference |
| QA-007 | HIGH     | Validity     | Visit date is not in the future |
| QA-008 | CRITICAL | Validity     | Amount paid is not negative |
| QA-009 | MEDIUM   | Validity     | Claim status is one of APPROVED / DENIED / PENDING |
| QA-010 | HIGH     | Consistency  | Discharge date is not before visit date |
| QA-011 | HIGH     | Consistency  | Amount paid does not exceed amount billed |
| QA-012 | CRITICAL | Uniqueness   | No duplicate claims (same patient, provider, date, diagnosis, amount) |

All rules are defined as SQL queries in `sql/validation_rules.sql` — not embedded in Python. This means a data analyst who knows SQL but not Python can read, audit, and propose changes to any rule without touching application code.





#Database Design

The system uses 5 relational tables designed to support full auditability of every QA run.


claims              Raw ingested claims data
icd10_reference     Reference table of valid ICD-10-CM codes
qa_rules            Catalogue of all validation rules with severity and domain
qa_test_runs        Audit log — one record per pipeline execution
qa_results          Issue log — one record per claim-per-rule violation


Every issue in `qa_results` links back to the `qa_test_runs` row that generated it, so you can always trace exactly when a problem was first detected.

This structure is intentionally similar to how data quality frameworks work in production environments — where rules are version-controlled, runs are auditable, and issues have a full lineage back to their source.



# Author

Hriddhu — [github.com/Hriddhu](https://github.com/Hriddhu)

Built as a learning project to explore healthcare data quality concepts and SQL-driven validation pipelines.
