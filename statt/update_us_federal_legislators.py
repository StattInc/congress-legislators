#!/usr/bin/env python3
"""
Incremental updater for us_federal_legislators.

Behavior:
- Fetches current members from Congress.gov API.
- Upserts current members by bioguide_id.
- Sets active = false for existing rows no longer in current Congress.
- Never truncates or deletes rows.
- Never updates columns outside the Congress.gov source allowlist.

Manual fields such as biography/about_page_url are preserved because they are not
included in the update allowlist.
"""

import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
TABLE_NAME = os.getenv("LEGISLATORS_TABLE_NAME", "us_federal_legislators")
TABLE_SCHEMA_OVERRIDE = os.getenv("LEGISLATORS_TABLE_SCHEMA")
REQUEST_DELAY = float(os.getenv("CONGRESS_REQUEST_DELAY", "0.0"))
DEFAULT_SCHEMA = "civic"

# These are the only fields this script will write.
CONGRESS_SOURCE_COLUMNS = [
    "bioguide_id",
    "first_name",
    "middle_name",
    "last_name",
    "state_code",
    "party",
    "congress_history",
    "image_url",
    "website",
    "phone",
    "address",
    "chamber",
    "district",
    "active",
]


def fetch_current_members(api_key: str) -> List[Dict[str, Any]]:
    """Fetch all current members using pagination."""
    all_members: List[Dict[str, Any]] = []
    offset = 0
    limit = 250

    while True:
        url = "https://api.congress.gov/v3/member"
        params = {
            "format": "json",
            "limit": limit,
            "offset": offset,
            "currentMember": "true",
            "api_key": api_key,
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        members = data.get("members", [])

        if not members:
            break

        all_members.extend(members)
        print(f"Fetched {len(members)} members at offset {offset} (total: {len(all_members)})")

        pagination = data.get("pagination", {})
        if offset + limit >= pagination.get("count", 0):
            break

        offset += limit
        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

    print(f"Total current members fetched: {len(all_members)}")
    return all_members


def fetch_member_details(bioguide_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch detailed information for one member."""
    url = f"https://api.congress.gov/v3/member/{bioguide_id}"
    params = {"format": "json", "api_key": api_key}

    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            print(f"  Warning: details fetch failed for {bioguide_id} ({response.status_code})")
            return None

        data = response.json()
        return data.get("member", {})
    except Exception as exc:
        print(f"  Warning: details fetch exception for {bioguide_id}: {exc}")
        return None


def extract_state_code(member_detail: Dict[str, Any]) -> Optional[str]:
    terms = member_detail.get("terms", [])
    if terms:
        state_code = terms[-1].get("stateCode")
        if state_code:
            return str(state_code).upper()
    return None


def extract_chamber(member_detail: Dict[str, Any]) -> Optional[str]:
    terms = member_detail.get("terms", [])
    if terms:
        return terms[-1].get("chamber")
    return None


def extract_congress_history(member_detail: Dict[str, Any]) -> Optional[str]:
    terms = member_detail.get("terms", [])
    if not terms:
        return None

    parts: List[str] = []
    for term in terms:
        congress = term.get("congress")
        chamber = term.get("chamber")
        if not congress or not chamber:
            continue
        chamber_abbr = "H" if "House" in str(chamber) else "S"
        parts.append(f"{congress}{chamber_abbr}")

    return ", ".join(parts) if parts else None


def transform_member_to_record(basic_info: Dict[str, Any], detailed_info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Map API payloads into DB columns supported by this updater."""
    bioguide_id = basic_info.get("bioguideId")

    name = basic_info.get("name", "")
    name_parts = name.split(", ")
    last_name = name_parts[0] if len(name_parts) > 0 else None

    first_middle = name_parts[1] if len(name_parts) > 1 else ""
    first_middle_parts = first_middle.split(" ")
    first_name = first_middle_parts[0] if len(first_middle_parts) > 0 else None
    middle_name = " ".join(first_middle_parts[1:]).rstrip(".") if len(first_middle_parts) > 1 else None

    party_name = basic_info.get("partyName")
    party = party_name[0] if party_name else None

    chamber = None
    district = basic_info.get("district")
    terms = basic_info.get("terms", {}).get("item", [])
    if terms:
        chamber = terms[-1].get("chamber")

    image_url = basic_info.get("depiction", {}).get("imageUrl")

    website = None
    phone = None
    address = None
    congress_history = None
    state_code = None

    if detailed_info:
        first_name = detailed_info.get("firstName", first_name)
        middle_name = detailed_info.get("middleName", middle_name)
        last_name = detailed_info.get("lastName", last_name)

        website = detailed_info.get("officialWebsiteUrl")

        address_info = detailed_info.get("addressInformation", {})
        if address_info:
            phone = address_info.get("phoneNumber")
            office = address_info.get("officeAddress", "")
            city = address_info.get("city", "")
            district_addr = address_info.get("district", "")
            zip_code = address_info.get("zipCode", "")
            address_parts = [p for p in [office, city, district_addr, str(zip_code) if zip_code else ""] if p]
            address = ", ".join(address_parts) if address_parts else None

        detailed_image = detailed_info.get("depiction", {}).get("imageUrl")
        if detailed_image:
            image_url = detailed_image

        if not chamber:
            chamber = extract_chamber(detailed_info)

        state_code = extract_state_code(detailed_info)

        if district is None:
            district = detailed_info.get("district")

        congress_history = extract_congress_history(detailed_info)

    return {
        "id": str(uuid.uuid4()),  # Used only when table requires explicit id on insert.
        "bioguide_id": bioguide_id,
        "first_name": first_name,
        "middle_name": middle_name,
        "last_name": last_name,
        "state_code": state_code,
        "party": party,
        "congress_history": congress_history,
        "image_url": image_url,
        "website": website,
        "phone": phone,
        "address": address,
        "chamber": chamber,
        "district": str(district) if district is not None else None,
        "active": True,
    }


def resolve_target_table(conn, table_name: str, schema_override: Optional[str]) -> Tuple[str, str]:
    """Resolve target table schema. Defaults to civic."""
    target_schema = schema_override or DEFAULT_SCHEMA
    exists = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :table_schema
              AND table_name = :table_name
            LIMIT 1
            """
        ),
        {"table_schema": target_schema, "table_name": table_name},
    ).scalar()

    if not exists:
        raise RuntimeError(
            f"Table {target_schema}.{table_name} not found. "
            "Set LEGISLATORS_TABLE_SCHEMA / LEGISLATORS_TABLE_NAME if needed."
        )

    return target_schema, table_name


def get_table_columns(conn, schema: str, table: str) -> Dict[str, Dict[str, Any]]:
    """Get column metadata keyed by column name."""
    rows = conn.execute(
        text(
            """
            SELECT column_name, is_nullable, column_default, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = :table_schema
              AND table_name = :table_name
            """
        ),
        {"table_schema": schema, "table_name": table},
    ).mappings().all()

    return {row["column_name"]: dict(row) for row in rows}


def sync_members(engine, schema: str, table: str, records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Incrementally sync records with no delete/truncate."""
    stats = {
        "inserted": 0,
        "updated": 0,
        "deactivated": 0,
        "skipped": 0,
    }

    quoted_table = f'"{schema}"."{table}"'

    with engine.begin() as conn:
        column_info = get_table_columns(conn, schema, table)
        table_columns = set(column_info.keys())

        if "bioguide_id" not in table_columns:
            raise RuntimeError(f"{schema}.{table} must have a bioguide_id column")

        # Only update fields we consider Congress.gov sourced and that exist on this table.
        updatable_columns = [
            col for col in CONGRESS_SOURCE_COLUMNS if col in table_columns and col != "bioguide_id"
        ]

        should_update_timestamp = "updated_at" in table_columns

        # Insert columns are similarly restricted, plus bioguide_id and optional id.
        insert_columns = ["bioguide_id"] + [col for col in updatable_columns if col != "active"]
        if "active" in updatable_columns:
            insert_columns.append("active")

        id_info = column_info.get("id")
        include_id_on_insert = False
        if id_info and id_info.get("column_default") is None:
            if id_info.get("udt_name") == "uuid":
                include_id_on_insert = True
                insert_columns = ["id"] + insert_columns

        # Preload existing bioguide IDs.
        existing_ids = {
            row[0]
            for row in conn.execute(text(f"SELECT bioguide_id FROM {quoted_table} WHERE bioguide_id IS NOT NULL"))
        }

        update_assignments: List[str] = []
        for col in updatable_columns:
            if col == "active":
                update_assignments.append('"active" = TRUE')
            else:
                # Preserve existing value when incoming value is NULL.
                update_assignments.append(f'"{col}" = COALESCE(:{col}, "{col}")')

        if should_update_timestamp:
            update_assignments.append('"updated_at" = CURRENT_TIMESTAMP')

        if not update_assignments:
            raise RuntimeError(
                f"{schema}.{table} has no updatable Congress.gov columns. "
                "Check your column names or update CONGRESS_SOURCE_COLUMNS."
            )

        update_sql = text(
            f"UPDATE {quoted_table} SET {', '.join(update_assignments)} WHERE bioguide_id = :bioguide_id"
        )

        insert_cols_sql = ", ".join(f'"{col}"' for col in insert_columns)
        insert_vals_sql = ", ".join(f":{col}" for col in insert_columns)
        insert_sql = text(f"INSERT INTO {quoted_table} ({insert_cols_sql}) VALUES ({insert_vals_sql})")

        incoming_ids: Set[str] = set()

        for record in records:
            bioguide_id = record.get("bioguide_id")
            if not bioguide_id:
                stats["skipped"] += 1
                continue

            incoming_ids.add(bioguide_id)

            payload = {col: record.get(col) for col in CONGRESS_SOURCE_COLUMNS if col in table_columns}
            payload["bioguide_id"] = bioguide_id

            if bioguide_id in existing_ids:
                conn.execute(update_sql, payload)
                stats["updated"] += 1
            else:
                if include_id_on_insert:
                    payload["id"] = record["id"]
                insert_payload = {col: payload.get(col) for col in insert_columns}
                conn.execute(insert_sql, insert_payload)
                stats["inserted"] += 1

        # Mark no-longer-current members inactive, but keep rows.
        if "active" in table_columns:
            deactivate_assignment = '"active" = FALSE'
            if should_update_timestamp:
                deactivate_assignment += ', "updated_at" = CURRENT_TIMESTAMP'

            currently_active = conn.execute(
                text(f"SELECT bioguide_id FROM {quoted_table} WHERE bioguide_id IS NOT NULL AND COALESCE(active, FALSE) = TRUE")
            ).fetchall()

            deactivate_sql = text(
                f"UPDATE {quoted_table} SET {deactivate_assignment} WHERE bioguide_id = :bioguide_id"
            )

            for row in currently_active:
                bioguide_id = row[0]
                if bioguide_id in incoming_ids:
                    continue
                conn.execute(deactivate_sql, {"bioguide_id": bioguide_id})
                stats["deactivated"] += 1

    return stats


def run_update() -> None:
    if not CONGRESS_API_KEY:
        print("ERROR: CONGRESS_API_KEY is not set")
        sys.exit(1)

    if not DATABASE_URL:
        print("ERROR: DATABASE_URL is not set")
        sys.exit(1)

    print("=== US Federal Legislators Incremental Update ===")

    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        schema, table = resolve_target_table(conn, TABLE_NAME, TABLE_SCHEMA_OVERRIDE)

    print(f"Target table: {schema}.{table}")
    print("\nStep 1: Fetch current members list...")
    current_members = fetch_current_members(CONGRESS_API_KEY)

    print(f"\nStep 2: Fetch details and transform ({len(current_members)} members)...")
    records: List[Dict[str, Any]] = []

    for idx, member in enumerate(current_members, 1):
        bioguide_id = member.get("bioguideId", "")
        details = fetch_member_details(bioguide_id, CONGRESS_API_KEY) if bioguide_id else None
        record = transform_member_to_record(member, details)
        records.append(record)

        if idx % 25 == 0 or idx == len(current_members):
            print(f"  Processed {idx}/{len(current_members)}")

        if REQUEST_DELAY > 0 and idx < len(current_members):
            time.sleep(REQUEST_DELAY)

    print("\nStep 3: Apply incremental DB sync...")
    stats = sync_members(engine, schema, table, records)

    print("\n=== Summary ===")
    print(f"Fetched current members: {len(current_members)}")
    print(f"Inserted: {stats['inserted']}")
    print(f"Updated: {stats['updated']}")
    print(f"Deactivated: {stats['deactivated']}")
    print(f"Skipped (missing bioguide_id): {stats['skipped']}")
    print("\nNo rows were deleted or truncated.")


if __name__ == "__main__":
    try:
        run_update()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"\nFATAL ERROR: {exc}")
        sys.exit(1)
