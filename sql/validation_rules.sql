-- QA-001
SELECT
    claim_id,
    NULL              AS patient_id,
    'patient_id'      AS field_name,
    COALESCE(patient_id, 'NULL') AS field_value,
    'Patient ID is missing. Claim cannot be attributed to a member.' AS issue_detail
FROM claims
WHERE patient_id IS NULL OR TRIM(patient_id) = '';



-- QA-002
SELECT
    claim_id,
    patient_id,
    'provider_id'     AS field_name,
    COALESCE(provider_id, 'NULL') AS field_value,
    'Provider ID is missing. Claim cannot be attributed to a provider.' AS issue_detail
FROM claims
WHERE provider_id IS NULL OR TRIM(provider_id) = '';



-- QA-003
SELECT
    claim_id,
    patient_id,
    'visit_date'      AS field_name,
    COALESCE(visit_date, 'NULL') AS field_value,
    'Visit date is missing. Claim cannot be episode-grouped or timeline-sorted.' AS issue_detail
FROM claims
WHERE visit_date IS NULL OR TRIM(visit_date) = '';



-- QA-004
SELECT
    claim_id,
    patient_id,
    'discharge_date'  AS field_name,
    COALESCE(discharge_date, 'NULL') AS field_value,
    'Discharge date is missing. Required for inpatient length-of-stay calculation.' AS issue_detail
FROM claims
WHERE discharge_date IS NULL OR TRIM(discharge_date) = '';



-- QA-005
SELECT
    claim_id,
    patient_id,
    'diagnosis_code'  AS field_name,
    COALESCE(diagnosis_code, 'NULL') AS field_value,
    'Diagnosis code is missing. Claim cannot be clinically classified.' AS issue_detail
FROM claims
WHERE diagnosis_code IS NULL OR TRIM(diagnosis_code) = '';


-- QA-006
SELECT
    c.claim_id,
    c.patient_id,
    'diagnosis_code'  AS field_name,
    c.diagnosis_code  AS field_value,
    'Diagnosis code [' || c.diagnosis_code || '] is not a recognised ICD-10-CM code. '
    || 'Will be rejected by clinical analytics and risk adjustment modules.' AS issue_detail
FROM claims c
LEFT JOIN icd10_reference r ON c.diagnosis_code = r.icd10_code
WHERE r.icd10_code IS NULL
  AND c.diagnosis_code IS NOT NULL
  AND TRIM(c.diagnosis_code) != '';



-- QA-007
SELECT
    claim_id,
    patient_id,
    'visit_date'      AS field_name,
    visit_date        AS field_value,
    'Visit date [' || visit_date || '] is in the future. '
    || 'Completed claims cannot have future visit dates. Likely a data entry or ETL error.' AS issue_detail
FROM claims
WHERE visit_date IS NOT NULL
  AND DATE(visit_date) > DATE('now');



-- QA-008
SELECT
    claim_id,
    patient_id,
    'amount_paid'     AS field_name,
    CAST(amount_paid AS TEXT) AS field_value,
    'Amount paid [' || amount_paid || '] is negative. '
    || 'Adjustments must be submitted as separate adjustment claims, not negative values.' AS issue_detail
FROM claims
WHERE amount_paid IS NOT NULL
  AND amount_paid < 0;



-- QA-009
SELECT
    claim_id,
    patient_id,
    'claim_status'    AS field_name,
    claim_status      AS field_value,
    'Claim status [' || COALESCE(claim_status, 'NULL') || '] is not a valid status. '
    || 'Accepted values: APPROVED, DENIED, PENDING.' AS issue_detail
FROM claims
WHERE claim_status IS NULL
   OR TRIM(claim_status) NOT IN ('APPROVED', 'DENIED', 'PENDING');



-- QA-010
SELECT
    claim_id,
    patient_id,
    'discharge_date'  AS field_name,
    discharge_date || ' (visit: ' || visit_date || ')' AS field_value,
    'Discharge date [' || discharge_date || '] is before visit date [' || visit_date || ']. '
    || 'Dates appear to be transposed in the source system.' AS issue_detail
FROM claims
WHERE visit_date IS NOT NULL
  AND discharge_date IS NOT NULL
  AND DATE(discharge_date) < DATE(visit_date);



-- QA-011
SELECT
    claim_id,
    patient_id,
    'amount_paid'     AS field_name,
    'billed=' || amount_billed || ', paid=' || amount_paid AS field_value,
    'Amount paid [' || amount_paid || '] exceeds amount billed [' || amount_billed || ']. '
    || 'Potential overpayment. Requires financial review.' AS issue_detail
FROM claims
WHERE amount_paid IS NOT NULL
  AND amount_billed IS NOT NULL
  AND amount_paid > amount_billed;


-- QA-012
SELECT
    c.claim_id,
    c.patient_id,
    'claim_id'        AS field_name,
    c.claim_id        AS field_value,
    'Duplicate claim detected. Another claim [' || d.claim_id || '] exists for the same '
    || 'patient [' || c.patient_id || '] on visit date [' || c.visit_date || '] '
    || 'with diagnosis [' || c.diagnosis_code || '] and billed amount [' || c.amount_billed || ']. '
    || 'Duplicates inflate financial totals and must be investigated before promotion.' AS issue_detail
FROM claims c
JOIN claims d
    ON  c.patient_id      = d.patient_id
    AND c.provider_id     = d.provider_id
    AND c.visit_date      = d.visit_date
    AND c.diagnosis_code  = d.diagnosis_code
    AND c.amount_billed   = d.amount_billed
    AND c.claim_id        != d.claim_id
    AND c.claim_id        < d.claim_id;   -- it ensures each pair is reported once, not twice..
