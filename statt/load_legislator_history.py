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
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
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


def default_display_name(first_name: Optional[str], middle_name: Optional[str], last_name: Optional[str]) -> Optional[str]:
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
        ) VALUES (
            :bioguide_id,
            :first_name,
            :middle_name,
            :last_name,
            :suffix,
            :nickname,
            :official_full,
            :display_name,
            :birthday,
            :gender,
            CAST(:other_names AS JSONB)
        )
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

    legacy_profiles_exist_sql = text(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'civic'
              AND table_name = 'us_federal_legislators'
        )
        """
    )

    backfill_from_legacy_sql = text(
        """
        UPDATE civic.us_federal_legislator_profiles AS p
        SET
            about_page_url = COALESCE(p.about_page_url, legacy.about_page_url),
            biography = COALESCE(p.biography, legacy.biography),
            display_name = COALESCE(
                p.display_name,
                legacy.display_name,
                CASE
                    WHEN legacy.first_name LIKE '%%.%%'
                        THEN CONCAT_WS(' ', legacy.middle_name, legacy.last_name)
                    ELSE CONCAT_WS(' ', legacy.first_name, legacy.last_name)
                END
            ),
            updated_at = CASE
                WHEN p.about_page_url IS NULL AND legacy.about_page_url IS NOT NULL THEN CURRENT_TIMESTAMP
                WHEN p.biography IS NULL AND legacy.biography IS NOT NULL THEN CURRENT_TIMESTAMP
                WHEN p.display_name IS NULL AND (
                    legacy.display_name IS NOT NULL OR
                    legacy.first_name IS NOT NULL OR
                    legacy.middle_name IS NOT NULL OR
                    legacy.last_name IS NOT NULL
                ) THEN CURRENT_TIMESTAMP
                ELSE p.updated_at
            END
        FROM civic.us_federal_legislators AS legacy
        WHERE legacy.bioguide_id = p.bioguide_id
          AND (
              p.about_page_url IS NULL OR
              p.biography IS NULL OR
              p.display_name IS NULL
          )
        """
    )

    backfill_display_name_sql = text(
        """
        UPDATE civic.us_federal_legislator_profiles
        SET
            display_name = CASE
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

    clear_party_affiliations_sql = text("DELETE FROM civic.us_federal_legislator_party_affiliations")
    clear_leadership_roles_sql = text("DELETE FROM civic.us_federal_legislator_leadership_roles")
    clear_terms_sql = text("DELETE FROM civic.us_federal_legislator_terms")
    clear_ids_sql = text("DELETE FROM civic.us_federal_legislator_ids")

    insert_ids_sql = text(
        """
        INSERT INTO civic.us_federal_legislator_ids (
            bioguide_id,
            id_type,
            id_value,
            is_previous,
            sort_order,
            updated_at
        ) VALUES (
            :bioguide_id,
            :id_type,
            :id_value,
            :is_previous,
            :sort_order,
            CURRENT_TIMESTAMP
        )
        """
    )

    insert_terms_sql = text(
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
        ) VALUES (
            :term_key,
            :bioguide_id,
            :term_ordinal,
            :term_type,
            :chamber,
            :start_date,
            :end_date,
            :state_code,
            :district,
            :senate_class,
            :state_rank,
            :party,
            :caucus,
            :how,
            :end_type,
            :url,
            :address,
            :phone,
            :fax,
            :contact_form,
            :office,
            :rss_url,
            :seat_key,
            :is_current,
            :source_file,
            CURRENT_TIMESTAMP
        )
        """
    )

    insert_party_affiliations_sql = text(
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
        ) VALUES (
            :affiliation_key,
            :term_key,
            :bioguide_id,
            :start_date,
            :end_date,
            :party,
            :caucus,
            :is_current,
            CURRENT_TIMESTAMP
        )
        """
    )

    insert_leadership_roles_sql = text(
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
        ) VALUES (
            :role_key,
            :bioguide_id,
            :title,
            :chamber,
            :start_date,
            :end_date,
            :is_current,
            CURRENT_TIMESTAMP
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

    with engine.begin() as conn:
        if profile_rows:
            conn.execute(upsert_profiles_sql, profile_rows)

        legacy_profiles_exist = bool(conn.execute(legacy_profiles_exist_sql).scalar())
        if legacy_profiles_exist:
            conn.execute(backfill_from_legacy_sql)
        conn.execute(backfill_display_name_sql)

        conn.execute(clear_party_affiliations_sql)
        conn.execute(clear_leadership_roles_sql)
        conn.execute(clear_terms_sql)
        conn.execute(clear_ids_sql)

        if staged_rows["ids"]:
            conn.execute(insert_ids_sql, staged_rows["ids"])
        if staged_rows["terms"]:
            conn.execute(insert_terms_sql, staged_rows["terms"])
        if staged_rows["party_affiliations"]:
            conn.execute(insert_party_affiliations_sql, staged_rows["party_affiliations"])
        if staged_rows["leadership_roles"]:
            conn.execute(insert_leadership_roles_sql, staged_rows["leadership_roles"])

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
