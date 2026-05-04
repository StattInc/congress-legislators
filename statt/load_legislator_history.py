#!/usr/bin/env python3
"""
Load legislator profile and service history from congress-legislators YAML files.

Behavior:
- Loads both legislators-current.yaml and legislators-historical.yaml.
- Upserts person-level profile fields keyed by bioguide_id.
- Preserves manually managed profile fields such as about_page_url and biography.
- Fully refreshes source-managed tables for IDs, terms, party affiliations,
  and leadership roles from the latest repository snapshot.

Usage:
    python statt/load_legislator_history.py
"""

import json
import os
import sys
import csv
import io
from datetime import date
from pathlib import Path
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DB_BATCH_SIZE = max(1, int(os.getenv("LEGISLATOR_SYNC_BATCH_SIZE", "5000")))
COPY_NULL_TOKEN = "__CODEX_NULL__"
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CURRENT_YAML_PATH = ROOT / "legislators-current.yaml"
DEFAULT_HISTORICAL_YAML_PATH = ROOT / "legislators-historical.yaml"
_current_yaml_env = os.getenv("LEGISLATORS_CURRENT_YAML_PATH")
_historical_yaml_env = os.getenv("LEGISLATORS_HISTORICAL_YAML_PATH")
LEGISLATORS_CURRENT_YAML_PATH = (
    _current_yaml_env.strip()
    if _current_yaml_env and _current_yaml_env.strip()
    else str(DEFAULT_CURRENT_YAML_PATH)
)
LEGISLATORS_HISTORICAL_YAML_PATH = (
    _historical_yaml_env.strip()
    if _historical_yaml_env and _historical_yaml_env.strip()
    else str(DEFAULT_HISTORICAL_YAML_PATH)
)


def load_yaml_records(path: str) -> List[Dict[str, Any]]:
    source_path = Path(path).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(
            f"Legislator YAML not found: {source_path}. "
            "Set LEGISLATORS_CURRENT_YAML_PATH / LEGISLATORS_HISTORICAL_YAML_PATH if needed."
        )

    print(f"Loading legislators from local file: {source_path}")
    with source_path.open("r", encoding="utf-8") as file:
        records = yaml.safe_load(file)

    if not isinstance(records, list):
        raise ValueError(f"Expected a YAML list in {source_path}, got {type(records).__name__}.")

    print(f"✓ Loaded {len(records)} legislator records")
    return records


def parse_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def log_phase(message: str) -> None:
    print(message, flush=True)


def chunk_rows(rows: Sequence[Dict[str, Any]], chunk_size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for start in range(0, len(rows), chunk_size):
        yield rows[start : start + chunk_size]


def run_logged_statement(conn, sql, params, start_message: str, done_message: str) -> None:
    log_phase(start_message)
    phase_timer = time.monotonic()
    if params is None:
        conn.execute(sql)
    elif isinstance(params, list) and len(params) > DB_BATCH_SIZE:
        total_rows = len(params)
        total_chunks = (total_rows + DB_BATCH_SIZE - 1) // DB_BATCH_SIZE
        for chunk_index, chunk in enumerate(chunk_rows(params, DB_BATCH_SIZE), start=1):
            chunk_timer = time.monotonic()
            conn.execute(sql, chunk)
            log_phase(
                f"  chunk {chunk_index}/{total_chunks}: {len(chunk)} rows in "
                f"{time.monotonic() - chunk_timer:.2f}s"
            )
    else:
        conn.execute(sql, params)
    log_phase(f"{done_message} in {time.monotonic() - phase_timer:.2f}s")


def format_copy_value(value: Any) -> str:
    if value is None:
        return COPY_NULL_TOKEN
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def copy_rows_to_table(conn, copy_sql: str, columns: Sequence[str], rows: Sequence[Dict[str, Any]], label: str) -> None:
    if not rows:
        log_phase(f"DB phase: no rows to stage for {label}")
        return

    total_rows = len(rows)
    total_chunks = (total_rows + DB_BATCH_SIZE - 1) // DB_BATCH_SIZE
    dbapi_conn = conn.connection.driver_connection

    with dbapi_conn.cursor() as cursor:
        overall_timer = time.monotonic()
        for chunk_index, chunk in enumerate(chunk_rows(rows, DB_BATCH_SIZE), start=1):
            buffer = io.StringIO()
            writer = csv.writer(buffer, delimiter="\t", quotechar='"', lineterminator="\n")
            for row in chunk:
                writer.writerow([format_copy_value(row.get(column)) for column in columns])
            buffer.seek(0)

            chunk_timer = time.monotonic()
            cursor.copy_expert(copy_sql, buffer)
            log_phase(
                f"  COPY chunk {chunk_index}/{total_chunks} for {label}: {len(chunk)} rows in "
                f"{time.monotonic() - chunk_timer:.2f}s"
            )

    log_phase(f"DB phase complete: staged {total_rows} {label} rows via COPY in {time.monotonic() - overall_timer:.2f}s")


def chamber_for_term(term_type: str) -> str:
    if term_type == "sen":
        return "Senate"
    return "House"


def normalize_chamber(value: Any) -> Optional[str]:
    chamber = normalize_string(value)
    if not chamber:
        return None
    lowered = chamber.lower()
    if lowered == "senate":
        return "Senate"
    if lowered == "house":
        return "House"
    return chamber


def default_display_name(
    first_name: Optional[str],
    middle_name: Optional[str],
    last_name: Optional[str],
    nickname: Optional[str],
) -> Optional[str]:
    if nickname:
        name_parts = [part for part in [nickname, last_name] if part]
        return " ".join(name_parts) if name_parts else None

    fallback_first = first_name
    if fallback_first and "." in fallback_first:
        fallback_first = middle_name or fallback_first

    name_parts = [part for part in [fallback_first, last_name] if part]
    if not name_parts:
        return None
    return " ".join(name_parts)


def build_term_key(bioguide_id: str, term: Mapping[str, Any]) -> str:
    term_type = normalize_string(term.get("type")) or "unknown"
    start_token = normalize_string(term.get("start")) or "unknown"
    state_token = normalize_string(term.get("state")) or "unknown"
    district_token = "na" if term.get("district") is None else str(term.get("district"))
    class_token = "na" if term.get("class") is None else str(term.get("class"))
    return f"{bioguide_id}:{term_type}:{start_token}:{state_token}:{district_token}:{class_token}"


def build_seat_key(term: Mapping[str, Any]) -> Optional[str]:
    term_type = normalize_string(term.get("type"))
    state_code = normalize_string(term.get("state"))
    if not term_type or not state_code:
        return None

    if term_type == "sen":
        senate_class = term.get("class")
        if senate_class is None:
            return None
        return f"sen:{state_code}:{senate_class}"

    district = term.get("district")
    if district is None:
        return None
    return f"rep:{state_code}:{district}"


def combine_other_names(existing: Any, incoming: Any) -> Any:
    if not existing:
        return incoming
    if not incoming:
        return existing
    if not isinstance(existing, list) or not isinstance(incoming, list):
        return incoming

    seen: set[str] = set()
    combined: List[Any] = []
    for item in [*existing, *incoming]:
        fingerprint = json.dumps(item, sort_keys=True, ensure_ascii=False)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        combined.append(item)
    return combined


def merge_profile(existing: Dict[str, Any], incoming: Dict[str, Any], incoming_priority: int) -> Dict[str, Any]:
    merged = dict(existing)
    if incoming_priority > merged["_priority"]:
        merged["_priority"] = incoming_priority

    for field, value in incoming.items():
        if field == "_priority":
            continue
        if field == "other_names":
            merged[field] = combine_other_names(merged.get(field), value)
            continue

        if incoming_priority >= merged["_priority"]:
            merged[field] = value if value is not None else merged.get(field)
        elif merged.get(field) is None and value is not None:
            merged[field] = value

    return merged


def build_profile_row(record: Mapping[str, Any], source_priority: int) -> Dict[str, Any]:
    identity = record.get("id", {}) or {}
    name = record.get("name", {}) or {}
    bio = record.get("bio", {}) or {}
    return {
        "bioguide_id": normalize_string(identity.get("bioguide")),
        "first_name": normalize_string(name.get("first")),
        "middle_name": normalize_string(name.get("middle")),
        "last_name": normalize_string(name.get("last")),
        "suffix": normalize_string(name.get("suffix")),
        "nickname": normalize_string(name.get("nickname")),
        "official_full": normalize_string(name.get("official_full")),
        "display_name": default_display_name(
            normalize_string(name.get("first")),
            normalize_string(name.get("middle")),
            normalize_string(name.get("last")),
            normalize_string(name.get("nickname")),
        ),
        "birthday": parse_date(bio.get("birthday")),
        "gender": normalize_string(bio.get("gender")),
        "other_names": record.get("other_names"),
        "_priority": source_priority,
    }


def iter_id_rows(bioguide_id: str, identity: Mapping[str, Any]) -> Iterable[Dict[str, Any]]:
    for id_type, raw_value in identity.items():
        if raw_value in (None, ""):
            continue

        if isinstance(raw_value, list):
            for sort_order, list_value in enumerate(raw_value, start=1):
                normalized = normalize_string(list_value)
                if not normalized:
                    continue
                yield {
                    "bioguide_id": bioguide_id,
                    "id_type": id_type,
                    "id_value": normalized,
                    "is_previous": id_type == "bioguide_previous",
                    "sort_order": sort_order,
                }
            continue

        normalized = normalize_string(raw_value)
        if not normalized:
            continue

        yield {
            "bioguide_id": bioguide_id,
            "id_type": id_type,
            "id_value": normalized,
            "is_previous": id_type == "bioguide_previous",
            "sort_order": None,
        }


def build_staged_rows(
    historical_records: List[Dict[str, Any]],
    current_records: List[Dict[str, Any]],
    as_of_date: date,
) -> Dict[str, List[Dict[str, Any]]]:
    profiles_by_bioguide: Dict[str, Dict[str, Any]] = {}
    ids_by_key: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    terms_by_key: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    affiliations_by_key: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    roles_by_key: Dict[str, Tuple[int, Dict[str, Any]]] = {}

    sources = [
        ("legislators-historical.yaml", 0, historical_records),
        ("legislators-current.yaml", 1, current_records),
    ]

    for source_file, source_priority, records in sources:
        for record in records:
            identity = record.get("id", {}) or {}
            bioguide_id = normalize_string(identity.get("bioguide"))
            if not bioguide_id:
                continue

            incoming_profile = build_profile_row(record, source_priority)
            existing_profile = profiles_by_bioguide.get(bioguide_id)
            if existing_profile:
                profiles_by_bioguide[bioguide_id] = merge_profile(existing_profile, incoming_profile, source_priority)
            else:
                profiles_by_bioguide[bioguide_id] = incoming_profile

            for id_row in iter_id_rows(bioguide_id, identity):
                ids_by_key[(id_row["bioguide_id"], id_row["id_type"], id_row["id_value"])] = id_row

            for term_ordinal, term in enumerate(record.get("terms", []) or [], start=1):
                term_type = normalize_string(term.get("type"))
                start_date = parse_date(term.get("start"))
                end_date = parse_date(term.get("end"))
                state_code = normalize_string(term.get("state"))
                if term_type not in {"sen", "rep"} or not start_date or not end_date or not state_code:
                    continue

                term_key = build_term_key(bioguide_id, term)
                term_row = {
                    "term_key": term_key,
                    "bioguide_id": bioguide_id,
                    "term_ordinal": term_ordinal,
                    "term_type": term_type,
                    "chamber": chamber_for_term(term_type),
                    "start_date": start_date,
                    "end_date": end_date,
                    "state_code": state_code,
                    "district": term.get("district"),
                    "senate_class": term.get("class"),
                    "state_rank": normalize_string(term.get("state_rank")),
                    "party": normalize_string(term.get("party")),
                    "caucus": normalize_string(term.get("caucus")),
                    "how": normalize_string(term.get("how")),
                    "end_type": normalize_string(term.get("end-type")),
                    "url": normalize_string(term.get("url")),
                    "address": normalize_string(term.get("address")),
                    "phone": normalize_string(term.get("phone")),
                    "fax": normalize_string(term.get("fax")),
                    "contact_form": normalize_string(term.get("contact_form")),
                    "office": normalize_string(term.get("office")),
                    "rss_url": normalize_string(term.get("rss_url")),
                    "seat_key": build_seat_key(term),
                    "is_current": start_date <= as_of_date <= end_date,
                    "source_file": source_file,
                }
                current_term = terms_by_key.get(term_key)
                if current_term is None or source_priority >= current_term[0]:
                    terms_by_key[term_key] = (source_priority, term_row)

                for affiliation_index, affiliation in enumerate(term.get("party_affiliations", []) or [], start=1):
                    affiliation_start = parse_date(affiliation.get("start"))
                    affiliation_end = parse_date(affiliation.get("end"))
                    if not affiliation_start or not affiliation_end:
                        continue

                    affiliation_key = f"{term_key}:party:{affiliation_index}"
                    affiliation_row = {
                        "affiliation_key": affiliation_key,
                        "term_key": term_key,
                        "bioguide_id": bioguide_id,
                        "start_date": affiliation_start,
                        "end_date": affiliation_end,
                        "party": normalize_string(affiliation.get("party")),
                        "caucus": normalize_string(affiliation.get("caucus")),
                        "is_current": affiliation_start <= as_of_date <= affiliation_end,
                    }
                    current_affiliation = affiliations_by_key.get(affiliation_key)
                    if current_affiliation is None or source_priority >= current_affiliation[0]:
                        affiliations_by_key[affiliation_key] = (source_priority, affiliation_row)

            for role_index, role in enumerate(record.get("leadership_roles", []) or [], start=1):
                role_start = parse_date(role.get("start"))
                if not role_start:
                    continue
                role_end = parse_date(role.get("end"))
                title = normalize_string(role.get("title"))
                chamber = normalize_chamber(role.get("chamber"))
                if not title:
                    continue

                role_key = f"{bioguide_id}:leadership:{role_start.isoformat()}:{chamber or 'na'}:{title}:{role_index}"
                role_row = {
                    "role_key": role_key,
                    "bioguide_id": bioguide_id,
                    "title": title,
                    "chamber": chamber,
                    "start_date": role_start,
                    "end_date": role_end,
                    "is_current": role_start <= as_of_date and (role_end is None or as_of_date <= role_end),
                }
                current_role = roles_by_key.get(role_key)
                if current_role is None or source_priority >= current_role[0]:
                    roles_by_key[role_key] = (source_priority, role_row)

    profiles = []
    for profile in profiles_by_bioguide.values():
        cleaned = dict(profile)
        cleaned.pop("_priority", None)
        profiles.append(cleaned)

    terms = [row for _, row in terms_by_key.values()]
    affiliations = [row for _, row in affiliations_by_key.values()]
    roles = [row for _, row in roles_by_key.values()]

    return {
        "profiles": profiles,
        "ids": list(ids_by_key.values()),
        "terms": terms,
        "party_affiliations": affiliations,
        "leadership_roles": roles,
    }


def sync_legislator_history(database_url: str, staged_rows: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    engine = create_engine(database_url)

    create_temp_profiles_sql = text(
        """
        CREATE TEMP TABLE temp_us_federal_legislator_profiles (
            bioguide_id TEXT NOT NULL,
            first_name TEXT,
            middle_name TEXT,
            last_name TEXT,
            suffix TEXT,
            nickname TEXT,
            official_full TEXT,
            display_name TEXT,
            birthday DATE,
            gender TEXT,
            other_names TEXT
        ) ON COMMIT DROP
        """
    )
    copy_profiles_sql = (
        f"COPY temp_us_federal_legislator_profiles "
        f"(bioguide_id, first_name, middle_name, last_name, suffix, nickname, official_full, display_name, birthday, gender, other_names) "
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '{COPY_NULL_TOKEN}')"
    )
    upsert_profiles_sql = text(
        """
        INSERT INTO civic.us_federal_legislator_profiles (
            bioguide_id,
            first_name,
            middle_name,
            last_name,
            suffix,
            nickname,
            official_full,
            display_name,
            birthday,
            gender,
            other_names
        )
        SELECT
            s.bioguide_id,
            s.first_name,
            s.middle_name,
            s.last_name,
            s.suffix,
            s.nickname,
            s.official_full,
            s.display_name,
            s.birthday,
            s.gender,
            CAST(s.other_names AS JSONB)
        FROM temp_us_federal_legislator_profiles s
        ON CONFLICT (bioguide_id) DO UPDATE
        SET
            first_name = EXCLUDED.first_name,
            middle_name = EXCLUDED.middle_name,
            last_name = EXCLUDED.last_name,
            suffix = EXCLUDED.suffix,
            nickname = EXCLUDED.nickname,
            official_full = EXCLUDED.official_full,
            display_name = COALESCE(
                civic.us_federal_legislator_profiles.display_name,
                EXCLUDED.display_name
            ),
            birthday = EXCLUDED.birthday,
            gender = EXCLUDED.gender,
            other_names = EXCLUDED.other_names,
            updated_at = CURRENT_TIMESTAMP
        """
    )

    backfill_display_name_sql = text(
        """
        UPDATE civic.us_federal_legislator_profiles
        SET
            display_name = CASE
                WHEN nickname IS NOT NULL AND nickname != ''
                    THEN CONCAT_WS(' ', nickname, last_name)
                WHEN first_name LIKE '%%.%%'
                    THEN CONCAT_WS(' ', middle_name, last_name)
                ELSE CONCAT_WS(' ', first_name, last_name)
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE display_name IS NULL
          AND (
              first_name IS NOT NULL OR
              middle_name IS NOT NULL OR
              last_name IS NOT NULL
          )
        """
    )

    create_temp_ids_sql = text(
        """
        CREATE TEMP TABLE temp_us_federal_legislator_ids (
            bioguide_id TEXT NOT NULL,
            id_type TEXT NOT NULL,
            id_value TEXT NOT NULL,
            is_previous BOOLEAN NOT NULL,
            sort_order INTEGER
        ) ON COMMIT DROP
        """
    )
    copy_ids_sql = (
        f"COPY temp_us_federal_legislator_ids "
        f"(bioguide_id, id_type, id_value, is_previous, sort_order) "
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '{COPY_NULL_TOKEN}')"
    )
    sync_ids_sql = text(
        """
        INSERT INTO civic.us_federal_legislator_ids (
            bioguide_id,
            id_type,
            id_value,
            is_previous,
            sort_order,
            updated_at
        )
        SELECT
            s.bioguide_id,
            s.id_type,
            s.id_value,
            s.is_previous,
            s.sort_order,
            CURRENT_TIMESTAMP
        FROM temp_us_federal_legislator_ids s
        ON CONFLICT (bioguide_id, id_type, id_value) DO UPDATE
        SET
            is_previous = EXCLUDED.is_previous,
            sort_order = EXCLUDED.sort_order,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    delete_stale_ids_sql = text(
        """
        DELETE FROM civic.us_federal_legislator_ids t
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_us_federal_legislator_ids s
            WHERE s.bioguide_id = t.bioguide_id
              AND s.id_type = t.id_type
              AND s.id_value = t.id_value
        )
        """
    )

    create_temp_terms_sql = text(
        """
        CREATE TEMP TABLE temp_us_federal_legislator_terms (
            term_key TEXT NOT NULL,
            bioguide_id TEXT NOT NULL,
            term_ordinal INTEGER NOT NULL,
            term_type TEXT NOT NULL,
            chamber TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            state_code TEXT NOT NULL,
            district INTEGER,
            senate_class INTEGER,
            state_rank TEXT,
            party TEXT,
            caucus TEXT,
            how TEXT,
            end_type TEXT,
            url TEXT,
            address TEXT,
            phone TEXT,
            fax TEXT,
            contact_form TEXT,
            office TEXT,
            rss_url TEXT,
            seat_key TEXT,
            is_current BOOLEAN NOT NULL,
            source_file TEXT NOT NULL
        ) ON COMMIT DROP
        """
    )
    copy_terms_sql = (
        f"COPY temp_us_federal_legislator_terms "
        f"(term_key, bioguide_id, term_ordinal, term_type, chamber, start_date, end_date, state_code, district, senate_class, state_rank, party, caucus, how, end_type, url, address, phone, fax, contact_form, office, rss_url, seat_key, is_current, source_file) "
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '{COPY_NULL_TOKEN}')"
    )
    sync_terms_sql = text(
        """
        INSERT INTO civic.us_federal_legislator_terms (
            term_key,
            bioguide_id,
            term_ordinal,
            term_type,
            chamber,
            start_date,
            end_date,
            state_code,
            district,
            senate_class,
            state_rank,
            party,
            caucus,
            how,
            end_type,
            url,
            address,
            phone,
            fax,
            contact_form,
            office,
            rss_url,
            seat_key,
            is_current,
            source_file,
            updated_at
        )
        SELECT
            s.term_key,
            s.bioguide_id,
            s.term_ordinal,
            s.term_type,
            s.chamber,
            s.start_date,
            s.end_date,
            s.state_code,
            s.district,
            s.senate_class,
            s.state_rank,
            s.party,
            s.caucus,
            s.how,
            s.end_type,
            s.url,
            s.address,
            s.phone,
            s.fax,
            s.contact_form,
            s.office,
            s.rss_url,
            s.seat_key,
            s.is_current,
            s.source_file,
            CURRENT_TIMESTAMP
        FROM temp_us_federal_legislator_terms s
        ON CONFLICT (term_key) DO UPDATE
        SET
            bioguide_id = EXCLUDED.bioguide_id,
            term_ordinal = EXCLUDED.term_ordinal,
            term_type = EXCLUDED.term_type,
            chamber = EXCLUDED.chamber,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            state_code = EXCLUDED.state_code,
            district = EXCLUDED.district,
            senate_class = EXCLUDED.senate_class,
            state_rank = EXCLUDED.state_rank,
            party = EXCLUDED.party,
            caucus = EXCLUDED.caucus,
            how = EXCLUDED.how,
            end_type = EXCLUDED.end_type,
            url = EXCLUDED.url,
            address = EXCLUDED.address,
            phone = EXCLUDED.phone,
            fax = EXCLUDED.fax,
            contact_form = EXCLUDED.contact_form,
            office = EXCLUDED.office,
            rss_url = EXCLUDED.rss_url,
            seat_key = EXCLUDED.seat_key,
            is_current = EXCLUDED.is_current,
            source_file = EXCLUDED.source_file,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    delete_stale_terms_sql = text(
        """
        DELETE FROM civic.us_federal_legislator_terms t
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_us_federal_legislator_terms s
            WHERE s.term_key = t.term_key
        )
        """
    )

    create_temp_party_affiliations_sql = text(
        """
        CREATE TEMP TABLE temp_us_federal_legislator_party_affiliations (
            affiliation_key TEXT NOT NULL,
            term_key TEXT NOT NULL,
            bioguide_id TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            party TEXT,
            caucus TEXT,
            is_current BOOLEAN NOT NULL
        ) ON COMMIT DROP
        """
    )
    copy_party_affiliations_sql = (
        f"COPY temp_us_federal_legislator_party_affiliations "
        f"(affiliation_key, term_key, bioguide_id, start_date, end_date, party, caucus, is_current) "
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '{COPY_NULL_TOKEN}')"
    )
    sync_party_affiliations_sql = text(
        """
        INSERT INTO civic.us_federal_legislator_party_affiliations (
            affiliation_key,
            term_key,
            bioguide_id,
            start_date,
            end_date,
            party,
            caucus,
            is_current,
            updated_at
        )
        SELECT
            s.affiliation_key,
            s.term_key,
            s.bioguide_id,
            s.start_date,
            s.end_date,
            s.party,
            s.caucus,
            s.is_current,
            CURRENT_TIMESTAMP
        FROM temp_us_federal_legislator_party_affiliations s
        ON CONFLICT (affiliation_key) DO UPDATE
        SET
            term_key = EXCLUDED.term_key,
            bioguide_id = EXCLUDED.bioguide_id,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            party = EXCLUDED.party,
            caucus = EXCLUDED.caucus,
            is_current = EXCLUDED.is_current,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    delete_stale_party_affiliations_sql = text(
        """
        DELETE FROM civic.us_federal_legislator_party_affiliations t
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_us_federal_legislator_party_affiliations s
            WHERE s.affiliation_key = t.affiliation_key
        )
        """
    )

    create_temp_leadership_roles_sql = text(
        """
        CREATE TEMP TABLE temp_us_federal_legislator_leadership_roles (
            role_key TEXT NOT NULL,
            bioguide_id TEXT NOT NULL,
            title TEXT NOT NULL,
            chamber TEXT,
            start_date DATE NOT NULL,
            end_date DATE,
            is_current BOOLEAN NOT NULL
        ) ON COMMIT DROP
        """
    )
    copy_leadership_roles_sql = (
        f"COPY temp_us_federal_legislator_leadership_roles "
        f"(role_key, bioguide_id, title, chamber, start_date, end_date, is_current) "
        f"FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '{COPY_NULL_TOKEN}')"
    )
    sync_leadership_roles_sql = text(
        """
        INSERT INTO civic.us_federal_legislator_leadership_roles (
            role_key,
            bioguide_id,
            title,
            chamber,
            start_date,
            end_date,
            is_current,
            updated_at
        )
        SELECT
            s.role_key,
            s.bioguide_id,
            s.title,
            s.chamber,
            s.start_date,
            s.end_date,
            s.is_current,
            CURRENT_TIMESTAMP
        FROM temp_us_federal_legislator_leadership_roles s
        ON CONFLICT (role_key) DO UPDATE
        SET
            bioguide_id = EXCLUDED.bioguide_id,
            title = EXCLUDED.title,
            chamber = EXCLUDED.chamber,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            is_current = EXCLUDED.is_current,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    delete_stale_leadership_roles_sql = text(
        """
        DELETE FROM civic.us_federal_legislator_leadership_roles t
        WHERE NOT EXISTS (
            SELECT 1
            FROM temp_us_federal_legislator_leadership_roles s
            WHERE s.role_key = t.role_key
        )
        """
    )

    profile_rows = [
        {
            **row,
            "other_names": json.dumps(row["other_names"], ensure_ascii=False) if row.get("other_names") is not None else None,
        }
        for row in staged_rows["profiles"]
    ]

    phase_started = time.monotonic()

    with engine.begin() as conn:
        run_logged_statement(conn, create_temp_profiles_sql, None, "DB phase: creating temp profiles stage table", "DB phase complete: temp profiles stage table")
        copy_rows_to_table(
            conn,
            copy_profiles_sql,
            [
                "bioguide_id",
                "first_name",
                "middle_name",
                "last_name",
                "suffix",
                "nickname",
                "official_full",
                "display_name",
                "birthday",
                "gender",
                "other_names",
            ],
            profile_rows,
            "profiles",
        )
        run_logged_statement(conn, upsert_profiles_sql, None, "DB phase: upserting profiles from staged snapshot", "DB phase complete: profiles")

        run_logged_statement(
            conn,
            backfill_display_name_sql,
            None,
            "DB phase: backfilling missing display_name values from profile name fields",
            "DB phase complete: display_name backfill",
        )

        run_logged_statement(conn, create_temp_ids_sql, None, "DB phase: creating temp ids stage table", "DB phase complete: temp ids stage table")
        copy_rows_to_table(
            conn,
            copy_ids_sql,
            ["bioguide_id", "id_type", "id_value", "is_previous", "sort_order"],
            staged_rows["ids"],
            "ids",
        )
        run_logged_statement(conn, sync_ids_sql, None, "DB phase: upserting ids from staged snapshot", "DB phase complete: synced ids")

        run_logged_statement(conn, create_temp_terms_sql, None, "DB phase: creating temp terms stage table", "DB phase complete: temp terms stage table")
        copy_rows_to_table(
            conn,
            copy_terms_sql,
            [
                "term_key",
                "bioguide_id",
                "term_ordinal",
                "term_type",
                "chamber",
                "start_date",
                "end_date",
                "state_code",
                "district",
                "senate_class",
                "state_rank",
                "party",
                "caucus",
                "how",
                "end_type",
                "url",
                "address",
                "phone",
                "fax",
                "contact_form",
                "office",
                "rss_url",
                "seat_key",
                "is_current",
                "source_file",
            ],
            staged_rows["terms"],
            "terms",
        )
        run_logged_statement(conn, sync_terms_sql, None, "DB phase: upserting terms from staged snapshot", "DB phase complete: synced terms")

        run_logged_statement(
            conn,
            create_temp_party_affiliations_sql,
            None,
            "DB phase: creating temp party affiliations stage table",
            "DB phase complete: temp party affiliations stage table",
        )
        copy_rows_to_table(
            conn,
            copy_party_affiliations_sql,
            ["affiliation_key", "term_key", "bioguide_id", "start_date", "end_date", "party", "caucus", "is_current"],
            staged_rows["party_affiliations"],
            "party affiliations",
        )
        run_logged_statement(
            conn,
            sync_party_affiliations_sql,
            None,
            "DB phase: upserting party affiliations from staged snapshot",
            "DB phase complete: synced party affiliations",
        )

        run_logged_statement(
            conn,
            create_temp_leadership_roles_sql,
            None,
            "DB phase: creating temp leadership roles stage table",
            "DB phase complete: temp leadership roles stage table",
        )
        copy_rows_to_table(
            conn,
            copy_leadership_roles_sql,
            ["role_key", "bioguide_id", "title", "chamber", "start_date", "end_date", "is_current"],
            staged_rows["leadership_roles"],
            "leadership roles",
        )
        run_logged_statement(
            conn,
            sync_leadership_roles_sql,
            None,
            "DB phase: upserting leadership roles from staged snapshot",
            "DB phase complete: synced leadership roles",
        )

        run_logged_statement(
            conn,
            delete_stale_party_affiliations_sql,
            None,
            "DB phase: deleting stale party affiliations",
            "DB phase complete: deleted stale party affiliations",
        )
        run_logged_statement(
            conn,
            delete_stale_leadership_roles_sql,
            None,
            "DB phase: deleting stale leadership roles",
            "DB phase complete: deleted stale leadership roles",
        )
        run_logged_statement(conn, delete_stale_terms_sql, None, "DB phase: deleting stale terms", "DB phase complete: deleted stale terms")
        run_logged_statement(conn, delete_stale_ids_sql, None, "DB phase: deleting stale ids", "DB phase complete: deleted stale ids")

    log_phase(f"DB sync complete in {time.monotonic() - phase_started:.2f}s")

    return {
        "profiles_upserted": len(profile_rows),
        "ids_refreshed": len(staged_rows["ids"]),
        "terms_refreshed": len(staged_rows["terms"]),
        "party_affiliations_refreshed": len(staged_rows["party_affiliations"]),
        "leadership_roles_refreshed": len(staged_rows["leadership_roles"]),
    }


def main() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")

    current_records = load_yaml_records(LEGISLATORS_CURRENT_YAML_PATH)
    historical_records = load_yaml_records(LEGISLATORS_HISTORICAL_YAML_PATH)
    staged_rows = build_staged_rows(historical_records, current_records, as_of_date=date.today())

    print(
        "Prepared staged rows: "
        f"{len(staged_rows['profiles'])} profiles, "
        f"{len(staged_rows['ids'])} ids, "
        f"{len(staged_rows['terms'])} terms, "
        f"{len(staged_rows['party_affiliations'])} party affiliations, "
        f"{len(staged_rows['leadership_roles'])} leadership roles"
    )

    stats = sync_legislator_history(DATABASE_URL, staged_rows)
    print("✓ Legislator history sync complete")
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"FATAL ERROR: {exc}")
        sys.exit(1)
