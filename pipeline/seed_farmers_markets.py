#!/usr/bin/env python3
"""
Seed recurring farmers market events into the Supabase community_events table.
Generates the next 52 occurrences from today for each market.

Usage:
    python pipeline/seed_farmers_markets.py

Requires:
    - SUPABASE_URL env var
    - SUPABASE_KEY env var (service role key)
    - pip install supabase
"""

import os
import sys
from datetime import date, timedelta
from supabase import create_client

# ── Config ──────────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Set SUPABASE_URL and SUPABASE_KEY environment variables.")
    sys.exit(1)

TODAY = date.today()

# Day-of-week constants (date.weekday(): Mon=0 … Sun=6)
DOW = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}


# ── Date generation helpers ─────────────────────────────────────────────────

def next_weekday(start: date, weekday: int) -> date:
    """Return the first date >= start that falls on the given weekday."""
    days_ahead = (weekday - start.weekday()) % 7
    return start + timedelta(days=days_ahead)


def generate_dates_year_round(day_of_week: str, count: int = 52) -> list[date]:
    """Generate the next `count` occurrences of a weekday from today."""
    wd = DOW[day_of_week.lower()]
    d = next_weekday(TODAY, wd)
    dates = []
    for _ in range(count):
        dates.append(d)
        d += timedelta(weeks=1)
    return dates


def generate_dates_seasonal(
    day_of_week: str,
    season_start: date,
    season_end: date,
    count: int = 52,
) -> list[date]:
    """Generate up to `count` occurrences within a seasonal window."""
    wd = DOW[day_of_week.lower()]
    start = max(TODAY, season_start)
    d = next_weekday(start, wd)
    dates = []
    while len(dates) < count and d <= season_end:
        dates.append(d)
        d += timedelta(weeks=1)
    return dates


# ── Market definitions ──────────────────────────────────────────────────────

MARKETS = [
    {
        "title": "Napa Farmers Market",
        "description": (
            "Year-round Saturday market. Fresh produce, local food, "
            "specialty goods. CalFresh EBT accepted with dollar-for-dollar match."
        ),
        "start_time": "08:00",
        "end_time": "12:00",
        "venue_name": "Napa Farmers Market",
        "address": "1100 West Street",
        "town": "napa",
        "category": "food",
        "source": "NapaServe",
        "source_url": "https://napafarmersmarket.org",
        "dates_fn": lambda: generate_dates_year_round("saturday"),
    },
    {
        "title": "Napa Farmers Market (Tuesday)",
        "description": (
            "Year-round Saturday market. Fresh produce, local food, "
            "specialty goods. CalFresh EBT accepted with dollar-for-dollar match."
        ),
        "start_time": "08:00",
        "end_time": "12:00",
        "venue_name": "Napa Farmers Market",
        "address": "1100 West Street",
        "town": "napa",
        "category": "food",
        "source": "NapaServe",
        "source_url": "https://napafarmersmarket.org",
        "dates_fn": lambda: generate_dates_seasonal(
            "tuesday", date(2026, 4, 7), date(2026, 12, 22)
        ),
    },
    {
        "title": "Calistoga Farmers Market",
        "description": (
            "Year-round Saturday market at Sharpsteen Plaza. Fresh produce, "
            "flowers, gourmet food, live music, and local crafts."
        ),
        "start_time": "09:00",
        "end_time": "13:00",
        "venue_name": "Sharpsteen Plaza",
        "address": "1311 Washington Street",
        "town": "calistoga",
        "category": "food",
        "source": "NapaServe",
        "source_url": "https://visitcalistoga.com/listing/calistoga-farmers-market/",
        "dates_fn": lambda: generate_dates_year_round("saturday"),
    },
    {
        "title": "St. Helena Farmers Market",
        "description": (
            "Friday morning tradition at Crane Park since 1986. Fresh produce, "
            "live music, chef demos, kids programs, and nonprofit booths."
        ),
        "start_time": "07:30",
        "end_time": "12:00",
        "venue_name": "Crane Park",
        "address": "360 Crane Avenue",
        "town": "st-helena",
        "category": "food",
        "source": "NapaServe",
        "source_url": "https://www.sthelenafarmersmkt.org",
        "dates_fn": lambda: generate_dates_seasonal(
            "friday", date(2026, 5, 1), date(2026, 10, 31)
        ),
    },
    {
        "title": "Yountville Certified Farmers Market",
        "description": (
            "Year-round certified farmers market in downtown Yountville. "
            "Fresh produce, local goods, year-round rain or shine."
        ),
        "start_time": "09:00",
        "end_time": "13:00",
        "venue_name": "Yountville Farmers Market",
        "address": "6498 Washington Street",
        "town": "yountville",
        "category": "food",
        "source": "NapaServe",
        "source_url": "https://www.visitnapavalley.com/listing/yountville-certified-farmers-market/2183/",
        "dates_fn": lambda: generate_dates_year_round("sunday"),
    },
    {
        "title": "Yountville Certified Farmers Market (Thursday)",
        "description": (
            "Year-round certified farmers market in downtown Yountville. "
            "Fresh produce, local goods, year-round rain or shine."
        ),
        "start_time": "09:00",
        "end_time": "13:00",
        "venue_name": "Yountville Farmers Market",
        "address": "6498 Washington Street",
        "town": "yountville",
        "category": "food",
        "source": "NapaServe",
        "source_url": "https://www.visitnapavalley.com/listing/yountville-certified-farmers-market/2183/",
        "dates_fn": lambda: generate_dates_year_round("thursday"),
    },
]


# ── Build rows ──────────────────────────────────────────────────────────────

def build_rows() -> list[dict]:
    rows = []
    for market in MARKETS:
        event_dates = market["dates_fn"]()
        for d in event_dates:
            rows.append({
                "title": market["title"],
                "description": market["description"],
                "event_date": d.isoformat(),
                "start_time": market["start_time"],
                "end_time": market["end_time"],
                "venue_name": market["venue_name"],
                "address": market["address"],
                "town": market["town"],
                "category": market["category"],
                "source": market["source"],
                "source_url": market["source_url"],
                "status": "approved",
                "include_in_email": True,
            })
    return rows


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    rows = build_rows()

    # ── Dry-run summary ─────────────────────────────────────────────────
    print(f"Today: {TODAY}")
    print(f"Total rows to insert: {len(rows)}\n")

    by_title: dict[str, list[dict]] = {}
    for r in rows:
        by_title.setdefault(r["title"], []).append(r)

    for title, group in by_title.items():
        dates = [r["event_date"] for r in group]
        print(f"  {title}")
        print(f"    Count: {len(dates)}")
        print(f"    First: {dates[0]}  Last: {dates[-1]}")
        print(f"    Time:  {group[0]['start_time']}–{group[0]['end_time']}")
        print(f"    Town:  {group[0]['town']}")
        print()

    print("─" * 60)
    print("Sample rows (first 3):\n")
    for r in rows[:3]:
        for k, v in r.items():
            print(f"  {k}: {v}")
        print()

    # ── Prompt before insert ────────────────────────────────────────────
    answer = input("Insert these rows into Supabase? (y/n): ").strip().lower()
    if answer != "y":
        print("Aborted.")
        sys.exit(0)

    # ── Connect to Supabase ─────────────────────────────────────────────
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Check for existing rows and insert ──────────────────────────────
    inserted = 0
    skipped = 0
    errors = 0

    for r in rows:
        # Check if this title+event_date already exists
        existing = (
            supabase.table("community_events")
            .select("id")
            .eq("title", r["title"])
            .eq("event_date", r["event_date"])
            .execute()
        )

        if existing.data:
            skipped += 1
            continue

        try:
            supabase.table("community_events").insert(r).execute()
            inserted += 1
        except Exception as e:
            errors += 1
            print(f"  ERROR inserting {r['title']} on {r['event_date']}: {e}")

    print(f"\nDone. Inserted: {inserted}, Skipped (already exist): {skipped}, Errors: {errors}")


if __name__ == "__main__":
    main()
