#!/usr/bin/env python3
"""
Load legislator image URLs from the Congress.gov API into legislator profiles.

Behavior:
- Fetches every member (current and former) from the Congress.gov member list.
- Sets image_url on civic.us_federal_legislator_profiles by bioguide_id.
- Only updates rows whose image_url changed.
- Never clears an existing image_url when Congress.gov has no image for a member.
- Never inserts profiles; run statt/load_legislator_history.py first.

Usage:
    python statt/load_legislator_images.py
"""

import os
import sys
import time
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
REQUEST_DELAY = float(os.getenv("CONGRESS_REQUEST_DELAY", "0.0"))
PAGE_SIZE = 250


def normalize_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def fetch_member_image_urls(api_key: str) -> Dict[str, str]:
    """Return {bioguide_id: image_url} for every Congress.gov member with a depiction."""
    image_urls: Dict[str, str] = {}
    session = requests.Session()
    offset = 0

    while True:
        response = session.get(
            "https://api.congress.gov/v3/member",
            params={"format": "json", "limit": PAGE_SIZE, "offset": offset, "api_key": api_key},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        members = data.get("members", [])
        if not members:
            break

        for member in members:
            bioguide_id = normalize_string(member.get("bioguideId"))
            image_url = normalize_string((member.get("depiction") or {}).get("imageUrl"))
            if bioguide_id and image_url:
                image_urls[bioguide_id] = image_url

        print(f"Fetched {len(members)} members at offset {offset} ({len(image_urls)} with images so far)")

        offset += PAGE_SIZE
        if offset >= data.get("pagination", {}).get("count", 0):
            break
        if REQUEST_DELAY > 0:
            time.sleep(REQUEST_DELAY)

    return image_urls


def sync_image_urls(database_url: str, image_urls: Dict[str, str]) -> Dict[str, int]:
    engine = create_engine(database_url)
    bioguide_ids = list(image_urls)

    unmatched_sql = text(
        """
        SELECT s.bioguide_id
        FROM unnest(CAST(:bioguide_ids AS TEXT[])) AS s(bioguide_id)
        WHERE NOT EXISTS (
            SELECT 1
            FROM civic.us_federal_legislator_profiles p
            WHERE p.bioguide_id = s.bioguide_id
        )
        ORDER BY s.bioguide_id
        """
    )
    update_sql = text(
        """
        UPDATE civic.us_federal_legislator_profiles p
        SET
            image_url = s.image_url,
            updated_at = CURRENT_TIMESTAMP
        FROM unnest(CAST(:bioguide_ids AS TEXT[]), CAST(:image_urls AS TEXT[])) AS s(bioguide_id, image_url)
        WHERE p.bioguide_id = s.bioguide_id
          AND p.image_url IS DISTINCT FROM s.image_url
        """
    )

    with engine.begin() as conn:
        unmatched = [row[0] for row in conn.execute(unmatched_sql, {"bioguide_ids": bioguide_ids})]
        result = conn.execute(
            update_sql,
            {"bioguide_ids": bioguide_ids, "image_urls": [image_urls[b] for b in bioguide_ids]},
        )

    if unmatched:
        print(f"  Warning: {len(unmatched)} Congress.gov members have no profile yet: {', '.join(unmatched)}")

    return {
        "images_fetched": len(image_urls),
        "profiles_updated": result.rowcount,
        "members_without_profile": len(unmatched),
    }


def main() -> None:
    if not CONGRESS_API_KEY:
        raise RuntimeError("CONGRESS_API_KEY is required")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")

    image_urls = fetch_member_image_urls(CONGRESS_API_KEY)
    stats = sync_image_urls(DATABASE_URL, image_urls)
    print("✓ Legislator image sync complete")
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
