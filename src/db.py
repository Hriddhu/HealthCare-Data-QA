

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "qa_database.db")
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def get_connection() -> sqlite3.Connection:
    """Return a database connection with row_factory set for dict-like access."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrent read performance
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialise_database() -> None:
    """
    Apply schema.sql and reports.sql on first run (CREATE IF NOT EXISTS is safe
    to call multiple times). Idempotent — safe to run on every startup.
    """
    conn = get_connection()
    with conn:
        for sql_file in ["schema.sql", "reports.sql"]:
            path = os.path.join(SQL_DIR, sql_file)
            with open(path, "r") as f:
                sql = f.read()
            
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for stmt in statements:
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError as e:
                    
                    if "already exists" not in str(e):
                        raise
    conn.close()


def reset_database() -> None:
    """
    Drop and recreate the database file. Used in testing.
    WARNING: destroys all data.
    """
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    initialise_database()
