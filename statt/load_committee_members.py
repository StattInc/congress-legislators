#!/usr/bin/env python3
"""
Committee Membership Data Loader for Congress

This script reads current committee membership data from a local YAML snapshot
and performs temporal SCD updates in civic.us_federal_committee_members.

Behavior:
- Never truncates or deletes rows.
- Inserts new rows for new memberships.
- Expires old current rows and inserts new current rows when data changes.
- Expires rows that disappear from the source snapshot.

Usage:
    python load_committee_members.py
"""

import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_COMMITTEE_MEMBERSHIP_YAML_PATH = Path(__file__).resolve().parents[1] / "committee-membership-current.yaml"
_committee_membership_yaml_env = os.getenv("COMMITTEE_MEMBERSHIP_YAML_PATH")
COMMITTEE_MEMBERSHIP_YAML_PATH = (
    _committee_membership_yaml_env.strip()
    if _committee_membership_yaml_env and _committee_membership_yaml_env.strip()
    else str(DEFAULT_COMMITTEE_MEMBERSHIP_YAML_PATH)
)

# Business fields tracked for change detection
MEMBER_FIELDS = [
    "name",
    "party",
    "rank",
    "title",
    "chamber",
]


def canonicalize_title(title: Optional[str]) -> Optional[str]:
    """
    Normalize non-substantive committee title variants so temporal history
    reflects real role changes.
    """
    if title is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(title)).strip()
    if not cleaned:
        return None

    # Normalize punctuation/hyphenation before matching.
    token = cleaned.lower().replace(".", "")
    token = re.sub(r"[-_/]", " ", token)
    token = re.sub(r"\s+", " ", token).strip()

    canonical_map = {
        "member": None,
        "chair": "Chair",
        "chairman": "Chair",
        "chairwoman": "Chair",
        "chairperson": "Chair",
        "vice chair": "Vice Chair",
        "vice chairman": "Vice Chair",
        "vice chairwoman": "Vice Chair",
        "vice chairperson": "Vice Chair",
        "co chair": "Co-Chair",
        "co chairman": "Co-Chair",
        "co chairwoman": "Co-Chair",
        "co chairperson": "Co-Chair",
        "ranking": "Ranking Member",
        "ranking member": "Ranking Member",
        "ranking minority": "Ranking Member",
        "ranking minority member": "Ranking Member",
        "ex officio": "Ex Officio",
    }

    if token in canonical_map:
        return canonical_map[token]

    return cleaned


def fetch_committee_membership_data(local_path: str) -> Dict[str, Any]:
    """Load committee membership data from a local YAML file."""
    source_path = Path(local_path).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(
            f"Local committee membership YAML not found: {source_path}. "
            "Set COMMITTEE_MEMBERSHIP_YAML_PATH to a valid file path."
        )

    print(f"Loading committee membership from local file: {source_path}")
    with source_path.open("r", encoding="utf-8") as file:
        membership_data = yaml.safe_load(file)

    if not isinstance(membership_data, dict):
        raise ValueError(
            f"Expected a YAML mapping in {source_path}, got {type(membership_data).__name__}."
        )

    total_members = sum(len(members) for members in membership_data.values())
    print(f"✓ Loaded {len(membership_data)} committees with {total_members} total members")
    return membership_data


def flatten_membership_data(membership_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Flatten committee membership data into individual member records."""
    flattened: List[Dict[str, Any]] = []

    for committee_id, members in membership_data.items():
        if not members:
            continue

        for member in members:
            bioguide_id = None
            if isinstance(member.get("bioguide"), str):
                bioguide_id = member["bioguide"]
            elif isinstance(member.get("id"), dict):
                bioguide_id = member["id"].get("bioguide")

            if not bioguide_id:
                print(f"⚠ Skipping member without bioguide ID: {member.get('name')}")
                continue

            member_record = {
                "committee_id": committee_id,
                "bioguide_id": bioguide_id,
                "name": member.get("name"),
                "party": member.get("party"),
                "rank": member.get("rank"),
                "title": canonicalize_title(member.get("title")),
                "chamber": member.get("chamber"),
            }
            flattened.append(member_record)

    return flattened


def member_payload_changed(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    """Return True when any tracked committee-member field changed."""
    for field in MEMBER_FIELDS:
        if field == "title":
            if canonicalize_title(existing.get(field)) != canonicalize_title(incoming.get(field)):
                return True
            continue
        if existing.get(field) != incoming.get(field):
            return True
    return False


def sync_committee_members(database_url: str, members: List[Dict[str, Any]], as_of_date: date) -> Dict[str, int]:
    """Perform temporal synchronization without deleting any rows."""
    engine = create_engine(database_url)

    select_current_sql = text(
        """
        SELECT
            id,
            committee_id,
            bioguide_id,
            name,
            party,
            rank,
            title,
            chamber,
            effective_date
        FROM civic.us_federal_committee_members
        WHERE is_current = TRUE;
        """
    )

    insert_sql = text(
        """
        INSERT INTO civic.us_federal_committee_members (
            committee_id,
            bioguide_id,
            name,
            party,
            rank,
            title,
            chamber,
            effective_date,
            expiration_date,
            is_current,
            updated_at
        ) VALUES (
            :committee_id,
            :bioguide_id,
            :name,
            :party,
            :rank,
            :title,
            :chamber,
            :effective_date,
            NULL,
            TRUE,
            CURRENT_TIMESTAMP
        );
        """
    )

    update_in_place_sql = text(
        """
        UPDATE civic.us_federal_committee_members
        SET
            name = :name,
            party = :party,
            rank = :rank,
            title = :title,
            chamber = :chamber,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
          AND is_current = TRUE;
        """
    )

    expire_sql = text(
        """
        UPDATE civic.us_federal_committee_members
        SET
            is_current = FALSE,
            expiration_date = :expiration_date,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = :id
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

    # Last-write-wins dedupe for duplicate committee/member pairs.
    incoming_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for member in members:
        committee_id = member.get("committee_id")
        bioguide_id = member.get("bioguide_id")

        if not committee_id or not bioguide_id:
            stats["skipped"] += 1
            continue

        incoming_by_key[(committee_id, bioguide_id)] = member

    with engine.begin() as conn:
        current_rows = conn.execute(select_current_sql).mappings().all()
        current_by_key: Dict[Tuple[str, str], Mapping[str, Any]] = {
            (row["committee_id"], row["bioguide_id"]): row for row in current_rows
        }

        seen_keys = set()

        for key, incoming in incoming_by_key.items():
            seen_keys.add(key)
            existing = current_by_key.get(key)

            if existing is None:
                payload = dict(incoming)
                payload["effective_date"] = as_of_date
                conn.execute(insert_sql, payload)
                stats["inserted"] += 1
                continue

            if member_payload_changed(existing, incoming):
                existing_effective = existing.get("effective_date")

                if existing_effective == as_of_date:
                    payload = dict(incoming)
                    payload["id"] = existing["id"]
                    conn.execute(update_in_place_sql, payload)
                else:
                    expiration_date = as_of_date - timedelta(days=1)
                    conn.execute(
                        expire_sql,
                        {
                            "id": existing["id"],
                            "expiration_date": expiration_date,
                        },
                    )
                    payload = dict(incoming)
                    payload["effective_date"] = as_of_date
                    conn.execute(insert_sql, payload)

                stats["changed"] += 1
            else:
                stats["unchanged"] += 1

        for key, existing in current_by_key.items():
            if key in seen_keys:
                continue

            existing_effective = existing.get("effective_date")
            expiration_date = as_of_date if existing_effective == as_of_date else as_of_date - timedelta(days=1)

            conn.execute(
                expire_sql,
                {
                    "id": existing["id"],
                    "expiration_date": expiration_date,
                },
            )
            stats["expired"] += 1

    return stats


def load_committee_members() -> None:
    """Main function to load committee membership data."""
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

    print("\n=== Congress Committee Membership Temporal Loader ===\n")
    print(f"Database: {masked_url}")
    print(f"As-of date: {as_of_date}\n")

    print("Step 1: Loading committee membership from local YAML...")
    membership_data = fetch_committee_membership_data(COMMITTEE_MEMBERSHIP_YAML_PATH)

    print("\nStep 2: Processing membership data...")
    flattened_members = flatten_membership_data(membership_data)
    print(f"✓ Processed {len(flattened_members)} member records")

    print("\nStep 3: Applying temporal updates...")
    stats = sync_committee_members(DATABASE_URL, flattened_members, as_of_date)

    print("\n=== Summary ===")
    print(f"Inserted:  {stats['inserted']}")
    print(f"Changed:   {stats['changed']}")
    print(f"Expired:   {stats['expired']}")
    print(f"Unchanged: {stats['unchanged']}")
    print(f"Skipped:   {stats['skipped']}")
    print(f"Committees in source: {len(membership_data)}")
    print("\n✓ Committee membership temporal load complete!")


if __name__ == "__main__":
    try:
        load_committee_members()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"\n\nFATAL ERROR: {exc}")
        sys.exit(1)
