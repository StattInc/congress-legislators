#!/usr/bin/env python3
"""Apply Postgres DDL for legislator history tables and views."""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DDL_PATH = Path(__file__).resolve().parents[1] / "infra" / "sql" / "civic_us_federal_legislator_history.sql"


def main() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    if not DDL_PATH.exists():
        raise FileNotFoundError(f"DDL file not found: {DDL_PATH}")

    ddl_sql = DDL_PATH.read_text(encoding="utf-8")
    engine = create_engine(DATABASE_URL)
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute(ddl_sql)
        raw_conn.commit()
    finally:
        raw_conn.close()

    print(f"✓ Applied legislator history DDL from {DDL_PATH}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"FATAL ERROR: {exc}")
        sys.exit(1)
