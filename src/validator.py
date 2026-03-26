import sqlite3
from src.db import get_connection


def _parse_rule_blocks(sql_path: str) -> dict[str, str]:
    """
    Parse validation_rules.sql into a dict of {rule_code: sql_query}.

    The file uses comment markers like:  -- QA-001
    to delimit each rule block, making them individually executable.
    """
    with open(sql_path, "r", encoding="utf-8") as f:
        content = f.read()

    rules: dict[str, str] = {}
    current_code: str | None = None
    current_lines: list[str] = []

    for line in content.splitlines():
        stripped = line.strip()

        if stripped.startswith("-- QA-") and len(stripped.split()) == 2:
            if current_code and current_lines:
                rules[current_code] = "\n".join(current_lines).strip()
            current_code = stripped.replace("-- ", "")
            current_lines = []
        elif current_code is not None:
            current_lines.append(line)

    # Save the last rule
    if current_code and current_lines:
        rules[current_code] = "\n".join(current_lines).strip()

    return rules


def run_all_validations(run_id: int, sql_path: str) -> dict:
    """
    Execute every enabled QA rule and store results in qa_results.

    Returns a summary dict:
    {
        "rules_executed": int,
        "total_issues": int,
        "issues_by_rule": { "QA-001": n, ... },
        "severity_counts": { "CRITICAL": n, ... }
    }
    """
    conn = get_connection()
    rule_blocks = _parse_rule_blocks(sql_path)

    # Fetch enabled rules and their metadata from the catalogue
    cursor = conn.execute(
        "SELECT rule_code, severity FROM qa_rules WHERE is_enabled = 1"
    )
    rule_meta = {row["rule_code"]: dict(row) for row in cursor.fetchall()}

    issues_by_rule: dict[str, int] = {}
    severity_counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    total_issues = 0

    for rule_code, sql_query in rule_blocks.items():
        if rule_code not in rule_meta:
            continue 

        severity = rule_meta[rule_code]["severity"]

        try:
            rows = conn.execute(sql_query).fetchall()
        except sqlite3.OperationalError as e:
            print(f"  [ERROR] Rule {rule_code} failed to execute: {e}")
            continue

        issues_found = len(rows)
        issues_by_rule[rule_code] = issues_found
        total_issues += issues_found

        if issues_found > 0:
            severity_counts[severity] = severity_counts.get(severity, 0) + issues_found

        # Writing each flagged row 
        with conn:
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO qa_results
                        (run_id, rule_code, claim_id, patient_id,
                         field_name, field_value, issue_detail, severity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        rule_code,
                        row["claim_id"]     if "claim_id"    in row.keys() else None,
                        row["patient_id"]   if "patient_id"  in row.keys() else None,
                        row["field_name"]   if "field_name"  in row.keys() else None,
                        row["field_value"]  if "field_value" in row.keys() else None,
                        row["issue_detail"] if "issue_detail"in row.keys() else None,
                        severity,
                    )
                )

    conn.close()

    return {
        "rules_executed": len(rule_blocks),
        "total_issues": total_issues,
        "issues_by_rule": issues_by_rule,
        "severity_counts": severity_counts,
    }
