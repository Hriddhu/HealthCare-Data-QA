import csv
import os
import sqlite3
from src.db import get_connection

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def load_icd10_reference() -> int:
    """
    Load valid ICD-10-CM codes from reference CSV into icd10_reference table.
    Returns the number of codes loaded.
    """
    conn = get_connection()
    path = os.path.join(DATA_DIR, "valid_icd10_codes.csv")
    count = 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        with conn:
            for row in reader:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO icd10_reference (icd10_code, description, category)
                    VALUES (?, ?, ?)
                    """,
                    (row["icd10_code"], row["description"], row["category"])
                )
                count += 1

    conn.close()
    return count


def load_claims(csv_path: str = None) -> tuple[int, str]:
    """
    Load claims from a CSV file into the claims table.
    Clears existing claims before loading — simulates a fresh daily batch.

    Returns (records_loaded, dataset_name).
    """
    if csv_path is None:
        csv_path = os.path.join(DATA_DIR, "sample_claims.csv")

    dataset_name = os.path.basename(csv_path)
    conn = get_connection()
    count = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        with conn:
            conn.execute("DELETE FROM claims")   # fresh load each run
            for row in reader:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO claims (
                        claim_id, patient_id, provider_id, visit_date, discharge_date,
                        diagnosis_code, procedure_code, amount_billed, amount_paid,
                        claim_status, insurance_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["claim_id"]      or None,
                        row["patient_id"]    or None,
                        row["provider_id"]   or None,
                        row["visit_date"]    or None,
                        row["discharge_date"]or None,
                        row["diagnosis_code"]or None,
                        row["procedure_code"]or None,
                        float(row["amount_billed"]) if row["amount_billed"] else None,
                        float(row["amount_paid"])   if row["amount_paid"]   else None,
                        row["claim_status"]  or None,
                        row["insurance_type"]or None,
                    )
                )
                count += 1

    conn.close()
    return count, dataset_name
