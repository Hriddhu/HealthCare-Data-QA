import os
import sys
import sqlite3
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.db import initialise_database, reset_database, get_connection
from src.loader import load_icd10_reference
from src.validator import run_all_validations

SQL_RULES = os.path.join(os.path.dirname(__file__), "..", "sql", "validation_rules.sql")


def _insert_claim(conn, **kwargs):
    """Helper to insert a single claim with sensible defaults."""
    defaults = {
        "claim_id":       "TC001",
        "patient_id":     "P9001",
        "provider_id":    "PR901",
        "visit_date":     "2024-06-01",
        "discharge_date": "2024-06-01",
        "diagnosis_code": "I10",
        "procedure_code": "99213",
        "amount_billed":  300.00,
        "amount_paid":    240.00,
        "claim_status":   "APPROVED",
        "insurance_type": "MEDICARE",
    }
    defaults.update(kwargs)
    conn.execute(
        """INSERT INTO claims (claim_id, patient_id, provider_id, visit_date, discharge_date,
                               diagnosis_code, procedure_code, amount_billed, amount_paid,
                               claim_status, insurance_type)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        list(defaults.values())
    )


def _create_run(conn):
    cursor = conn.execute(
        "INSERT INTO qa_test_runs (dataset_name, total_records, total_issues, pass_count, fail_count, run_status, run_duration_ms)"
        " VALUES ('test', 1, 0, 0, 0, 'PARTIAL', 0)"
    )
    return cursor.lastrowid


class TestCompletenessRules(unittest.TestCase):

    def setUp(self):
        reset_database()
        load_icd10_reference()
        self.conn = get_connection()
        self.conn.execute("DELETE FROM claims")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _run(self):
        run_id = _create_run(self.conn)
        self.conn.commit()
        return run_id, run_all_validations(run_id, SQL_RULES)

    def test_qa001_missing_patient_id(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC001", patient_id=None)
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-001", 0), 0,
                           "QA-001 should flag a claim with null patient_id")

    def test_qa001_passes_when_patient_id_present(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC002", patient_id="P9999")
        run_id, summary = self._run()
        self.assertEqual(summary["issues_by_rule"].get("QA-001", 0), 0,
                         "QA-001 should NOT flag a claim with a valid patient_id")

    def test_qa002_missing_provider_id(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC003", provider_id=None)
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-002", 0), 0)

    def test_qa004_missing_discharge_date(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC004", discharge_date=None)
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-004", 0), 0)


class TestValidityRules(unittest.TestCase):

    def setUp(self):
        reset_database()
        load_icd10_reference()
        self.conn = get_connection()
        self.conn.execute("DELETE FROM claims")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _run(self):
        run_id = _create_run(self.conn)
        self.conn.commit()
        return run_id, run_all_validations(run_id, SQL_RULES)

    def test_qa006_invalid_diagnosis_code(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC005", diagnosis_code="XXXXXX")
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-006", 0), 0,
                           "QA-006 should flag an unrecognised ICD-10 code")

    def test_qa006_valid_diagnosis_code_passes(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC006", diagnosis_code="I10")
        run_id, summary = self._run()
        self.assertEqual(summary["issues_by_rule"].get("QA-006", 0), 0,
                         "QA-006 should NOT flag a valid ICD-10 code")

    def test_qa007_future_visit_date(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC007", visit_date="2099-01-01", discharge_date="2099-01-01")
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-007", 0), 0)

    def test_qa008_negative_amount_paid(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC008", amount_paid=-100.00)
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-008", 0), 0)


class TestConsistencyRules(unittest.TestCase):

    def setUp(self):
        reset_database()
        load_icd10_reference()
        self.conn = get_connection()
        self.conn.execute("DELETE FROM claims")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _run(self):
        run_id = _create_run(self.conn)
        self.conn.commit()
        return run_id, run_all_validations(run_id, SQL_RULES)

    def test_qa010_discharge_before_visit(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC009",
                          visit_date="2024-06-10", discharge_date="2024-06-05")
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-010", 0), 0)

    def test_qa011_paid_exceeds_billed(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC010", amount_billed=200.00, amount_paid=500.00)
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-011", 0), 0)

    def test_qa012_duplicate_claim(self):
        with self.conn:
            _insert_claim(self.conn, claim_id="TC011",
                          patient_id="P100", provider_id="PR100",
                          visit_date="2024-06-01", diagnosis_code="I10", amount_billed=300.0)
            _insert_claim(self.conn, claim_id="TC012",
                          patient_id="P100", provider_id="PR100",
                          visit_date="2024-06-01", diagnosis_code="I10", amount_billed=300.0)
        run_id, summary = self._run()
        self.assertGreater(summary["issues_by_rule"].get("QA-012", 0), 0,
                           "QA-012 should detect duplicate claim pairs")


if __name__ == "__main__":
    unittest.main(verbosity=2)
