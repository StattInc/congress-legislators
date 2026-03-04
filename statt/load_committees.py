#!/usr/bin/env python3
"""
Committee Data Loader for Congress Committees

This script reads current committee data from a local YAML snapshot and
performs temporal SCD updates in civic.us_federal_committees.

Behavior:
- Never truncates or deletes rows.
- Inserts new rows for new committees.
- Expires old current rows and inserts new current rows when data changes.
- Expires rows that disappear from the source snapshot.

Usage:
    python load_committees.py
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_COMMITTEES_CURRENT_YAML_PATH = Path(__file__).resolve().parents[1] / "committees-current.yaml"
_committees_yaml_env = os.getenv("COMMITTEES_CURRENT_YAML_PATH")
COMMITTEES_CURRENT_YAML_PATH = (
    _committees_yaml_env.strip()
    if _committees_yaml_env and _committees_yaml_env.strip()
    else str(DEFAULT_COMMITTEES_CURRENT_YAML_PATH)
)

# Business fields tracked for change detection
COMMITTEE_FIELDS = [
    "type",
    "name",
    "url",
    "senate_committee_id",
    "house_committee_id",
    "jurisdiction",
    "jurisdiction_source",
    "address",
    "phone",
    "rss_url",
    "minority_rss_url",
    "youtube_id",
    "parent_committee_id",
    "is_subcommittee",
]


def fetch_committees_data(local_path: str) -> List[Dict[str, Any]]:
    """Load committee data from a local YAML file."""
    source_path = Path(local_path).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(
            f"Local committees YAML not found: {source_path}. "
            "Set COMMITTEES_CURRENT_YAML_PATH to a valid file path."
        )

    print(f"Loading committees from local file: {source_path}")
    with source_path.open("r", encoding="utf-8") as file:
        committees = yaml.safe_load(file)

    if not isinstance(committees, list):
        raise ValueError(
            f"Expected a YAML list in {source_path}, got {type(committees).__name__}."
        )

    print(f"✓ Loaded {len(committees)} committees")
    return committees


def flatten_committees(committees: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten committee data including subcommittees."""
    flattened: List[Dict[str, Any]] = []

    for committee in committees:
        parent_thomas_id = committee.get("thomas_id")

        parent_data = {
            "thomas_id": parent_thomas_id,
            "type": committee.get("type"),
            "name": committee.get("name"),
            "url": committee.get("url"),
            "senate_committee_id": committee.get("senate_committee_id"),
            "house_committee_id": committee.get("house_committee_id"),
            "jurisdiction": committee.get("jurisdiction"),
            "jurisdiction_source": committee.get("jurisdiction_source"),
            "address": committee.get("address"),
            "phone": committee.get("phone"),
            "rss_url": committee.get("rss_url"),
            "minority_rss_url": committee.get("minority_rss_url"),
            "youtube_id": committee.get("youtube_id"),
            "parent_committee_id": None,
            "is_subcommittee": False,
        }
        flattened.append(parent_data)

        for subcommittee in committee.get("subcommittees", []):
            sub_thomas_id = subcommittee.get("thomas_id")
            combined_thomas_id = f"{parent_thomas_id}{sub_thomas_id}"

            sub_data = {
                "thomas_id": combined_thomas_id,
                "type": committee.get("type"),
                "name": subcommittee.get("name"),
                "url": subcommittee.get("url"),
                "senate_committee_id": None,
                "house_committee_id": None,
                "jurisdiction": None,
                "jurisdiction_source": None,
                "address": subcommittee.get("address"),
                "phone": subcommittee.get("phone"),
                "rss_url": None,
                "minority_rss_url": None,
                "youtube_id": None,
                "parent_committee_id": parent_thomas_id,
                "is_subcommittee": True,
            }
            flattened.append(sub_data)

    return flattened


def committee_payload_changed(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    """Return True when any tracked committee field changed."""
    for field in COMMITTEE_FIELDS:
        if existing.get(field) != incoming.get(field):
            return True
    return False


def sync_committees(database_url: str, committees: List[Dict[str, Any]], as_of_date: date) -> Dict[str, int]:
    """Perform temporal synchronization without deleting any rows."""
    engine = create_engine(database_url)

    select_current_sql = text(
        """
        SELECT
            thomas_id,
            type,
            name,
            url,
            senate_committee_id,
            house_committee_id,
            jurisdiction,
            jurisdiction_source,
            address,
            phone,
            rss_url,
            minority_rss_url,
            youtube_id,
            parent_committee_id,
            is_subcommittee,
            effective_date
        FROM civic.us_federal_committees
        WHERE is_current = TRUE;
        """
    )

    insert_sql = text(
        """
        INSERT INTO civic.us_federal_committees (
            thomas_id,
            type,
            name,
            url,
            senate_committee_id,
            house_committee_id,
            jurisdiction,
            jurisdiction_source,
            address,
            phone,
            rss_url,
            minority_rss_url,
            youtube_id,
            parent_committee_id,
            is_subcommittee,
            effective_date,
            expiration_date,
            is_current,
            updated_at
        ) VALUES (
            :thomas_id,
            :type,
            :name,
            :url,
            :senate_committee_id,
            :house_committee_id,
            :jurisdiction,
            :jurisdiction_source,
            :address,
            :phone,
            :rss_url,
            :minority_rss_url,
            :youtube_id,
            :parent_committee_id,
            :is_subcommittee,
            :effective_date,
            NULL,
            TRUE,
            CURRENT_TIMESTAMP
        );
        """
    )

    update_in_place_sql = text(
        """
        UPDATE civic.us_federal_committees
        SET
            type = :type,
            name = :name,
            url = :url,
            senate_committee_id = :senate_committee_id,
            house_committee_id = :house_committee_id,
            jurisdiction = :jurisdiction,
            jurisdiction_source = :jurisdiction_source,
            address = :address,
            phone = :phone,
            rss_url = :rss_url,
            minority_rss_url = :minority_rss_url,
            youtube_id = :youtube_id,
            parent_committee_id = :parent_committee_id,
            is_subcommittee = :is_subcommittee,
            updated_at = CURRENT_TIMESTAMP
        WHERE thomas_id = :thomas_id
          AND is_current = TRUE;
        """
    )

    expire_sql = text(
        """
        UPDATE civic.us_federal_committees
        SET
            is_current = FALSE,
            expiration_date = :expiration_date,
            updated_at = CURRENT_TIMESTAMP
        WHERE thomas_id = :thomas_id
          AND is_current = TRUE;
        """
    )

    stats = {
        "inserted": 0,
        "changed": 0,
        "expired": 0,
        "unchanged": 0,
        "skipped": 0,
    }

    # Last-write-wins dedupe for duplicate IDs in source payload.
    incoming_by_id: Dict[str, Dict[str, Any]] = {}
    for committee in committees:
        thomas_id = committee.get("thomas_id")
        if not thomas_id:
            stats["skipped"] += 1
            print(f"⚠ Skipping committee without thomas_id: {committee.get('name')}")
            continue
        incoming_by_id[thomas_id] = committee

    with engine.begin() as conn:
        current_rows = conn.execute(select_current_sql).mappings().all()
        current_by_id: Dict[str, Mapping[str, Any]] = {
            row["thomas_id"]: row for row in current_rows
        }

        seen_ids = set()

        for thomas_id, incoming in incoming_by_id.items():
            seen_ids.add(thomas_id)
            existing = current_by_id.get(thomas_id)

            if existing is None:
                payload = dict(incoming)
                payload["effective_date"] = as_of_date
                conn.execute(insert_sql, payload)
                stats["inserted"] += 1
                continue

            if committee_payload_changed(existing, incoming):
                existing_effective = existing.get("effective_date")

                if existing_effective == as_of_date:
                    conn.execute(update_in_place_sql, incoming)
                else:
                    expiration_date = as_of_date - timedelta(days=1)
                    conn.execute(
                        expire_sql,
                        {
                            "thomas_id": thomas_id,
                            "expiration_date": expiration_date,
                        },
                    )
                    payload = dict(incoming)
                    payload["effective_date"] = as_of_date
                    conn.execute(insert_sql, payload)

                stats["changed"] += 1
            else:
                stats["unchanged"] += 1

        for thomas_id, existing in current_by_id.items():
            if thomas_id in seen_ids:
                continue

            existing_effective = existing.get("effective_date")
            expiration_date = as_of_date if existing_effective == as_of_date else as_of_date - timedelta(days=1)

            conn.execute(
                expire_sql,
                {
                    "thomas_id": thomas_id,
                    "expiration_date": expiration_date,
                },
            )
            stats["expired"] += 1

    return stats


def load_committees() -> None:
    """Main function to load committee data."""
    if not DATABASE_URL:
        print("ERROR: No database URL found in environment variables")
        print("Please set DATABASE_URL")
        sys.exit(1)

    masked_url = DATABASE_URL
    if "@" in DATABASE_URL:
        parts = DATABASE_URL.split("@")
        if "://" in parts[0]:
            protocol_and_creds = parts[0].split("://")
            if ":" in protocol_and_creds[1]:
                user = protocol_and_creds[1].split(":")[0]
                masked_url = f"{protocol_and_creds[0]}://{user}:****@{parts[1]}"

    as_of_date = date.today()

    print("\n=== Congress Committees Temporal Loader ===\n")
    print(f"Database: {masked_url}")
    print(f"As-of date: {as_of_date}\n")

    print("Step 1: Loading committees from local YAML...")
    current_committees = fetch_committees_data(COMMITTEES_CURRENT_YAML_PATH)

    print("\nStep 2: Flattening committee records...")
    flattened_current = flatten_committees(current_committees)
    print(f"✓ Processed {len(flattened_current)} committee/subcommittee rows")

    print("\nStep 3: Applying temporal updates...")
    stats = sync_committees(DATABASE_URL, flattened_current, as_of_date)

    print("\n=== Summary ===")
    print(f"Inserted:  {stats['inserted']}")
    print(f"Changed:   {stats['changed']}")
    print(f"Expired:   {stats['expired']}")
    print(f"Unchanged: {stats['unchanged']}")
    print(f"Skipped:   {stats['skipped']}")
    print("\n✓ Committee temporal load complete!")


if __name__ == "__main__":
    try:
        load_committees()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"\n\nFATAL ERROR: {exc}")
        sys.exit(1)
