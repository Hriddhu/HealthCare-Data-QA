import os
import sys
import time
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from src.db import initialise_database, get_connection
from src.loader import load_claims, load_icd10_reference
from src.validator import run_all_validations
from src.report import generate_report

SQL_RULES_PATH = os.path.join(os.path.dirname(__file__), "sql", "validation_rules.sql")


def main(claims_csv: str = None):
    print("\n" + "=" * 60)
    print("  Healthcare Claims Data QA System")
    print("  Cedar Gate Technologies — Data Quality Pipeline")
    print("=" * 60)

    # first step; initialize
    print("\n[1/5] Initialising database...")
    initialise_database()
    print("      Schema and views ready.")

    # 2nd : loading reference data
    print("\n[2/5] Loading ICD-10 reference codes...")
    icd_count = load_icd10_reference()
    print(f"      {icd_count} ICD-10-CM codes loaded into reference table.")

    # 3rd: loading batch
    print("\n[3/5] Loading claims data...")
    total_records, dataset_name = load_claims(claims_csv)
    print(f"      {total_records} claims loaded from [{dataset_name}].")

    # 4: creating new record
    print("\n[4/5] Starting QA validation run...")
    conn = get_connection()
    start_time = time.time()

    with conn:
        cursor = conn.execute(
            """
            INSERT INTO qa_test_runs (dataset_name, total_records, total_issues,
                                      pass_count, fail_count, run_status, run_duration_ms)
            VALUES (?, ?, 0, 0, 0, 'PARTIAL', 0)
            """,
            (dataset_name, total_records)
        )
        run_id = cursor.lastrowid
    conn.close()

    # 5: validation run
    summary = run_all_validations(run_id, SQL_RULES_PATH)
    elapsed_ms = int((time.time() - start_time) * 1000)

    # Compute affected claims
    conn = get_connection()
    affected_claims = conn.execute(
        "SELECT COUNT(DISTINCT claim_id) FROM qa_results WHERE run_id = ?", (run_id,)
    ).fetchone()[0]
    conn.close()

    total_issues = summary["total_issues"]
    fail_count   = affected_claims
    pass_count   = total_records - fail_count
    run_status   = "PASS" if total_issues == 0 else (
                   "FAIL" if summary["severity_counts"].get("CRITICAL", 0) > 0 else "PARTIAL"
                  )

    # 6: finalized run record
    conn = get_connection()
    with conn:
        conn.execute(
            """
            UPDATE qa_test_runs
            SET total_issues   = ?,
                pass_count     = ?,
                fail_count     = ?,
                run_status     = ?,
                run_duration_ms = ?
            WHERE run_id = ?
            """,
            (total_issues, pass_count, fail_count, run_status, elapsed_ms, run_id)
        )
    conn.close()

    print(f"      {summary['rules_executed']} rules executed in {elapsed_ms}ms.")
    print(f"      {total_issues} total issues found across {fail_count} claims.")

    # report generating:
    print("\n[5/5] Generating QA report...")
    report_path, report_text = generate_report(run_id, summary, dataset_name)

    print("\n" + report_text)
    print(f"\nReport saved to: {report_path}")

    # Return non-zero exit code if CRITICAL issues: 
    if run_status == "FAIL":
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Claims Data QA Pipeline"
    )
    parser.add_argument(
        "--claims",
        type=str,
        default=None,
        help="Path to claims CSV file (default: data/sample_claims.csv)"
    )
    args = parser.parse_args()
    main(args.claims)
