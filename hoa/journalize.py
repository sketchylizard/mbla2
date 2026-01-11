#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List
import json
import sys

from hoa import config
from hoa.journal import JournalEntry, Journal
from hoa.models import FinancialEvent, Posting

# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def read_events_from_stream(stream: Iterable[str]) -> List[FinancialEvent]:
    """
    Read FinancialEvents from a text stream.

    Supports:
      - JSON array
      - JSON object per line
    """
    text = "".join(stream).strip()
    if not text:
        return []

    events: List[FinancialEvent] = []

    if text.startswith("["):
        raw = json.loads(text)
        for obj in raw:
            events.append(FinancialEvent.from_dict(obj))
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            events.append(FinancialEvent.from_dict(obj))

    return events


def read_events_from_files(paths: List[str]) -> List[FinancialEvent]:
    events: List[FinancialEvent] = []

    for path in paths:
        if path == "-":
            events.extend(read_events_from_stream(sys.stdin))
        else:
            with open(path, "r", encoding="utf-8") as f:
                events.extend(read_events_from_stream(f))

    return events


import json


def generate_name_variations(full_name: str) -> List[str]:
    """
    Generate variations of a name by removing middle names/initials.

    Examples:
        "John R. Brading" -> {"John R. Brading", "John Brading"}
        "Kelly Anne Blair" -> {"Kelly Anne Blair", "Kelly Blair"}
        "Jason Stewart" -> {"Jason Stewart"}
    """
    variations = []

    # Add the original name
    variations.append(full_name)

    # Split into parts
    parts = full_name.split()

    if len(parts) <= 2:
        # Already first + last only
        return variations

    # Generate variation with just first and last name
    # (removing all middle names/initials)
    first_last = f"{parts[0]} {parts[-1]}"
    variations.append(first_last)

    return variations


def build_lot_lookup(address_file: Path) -> dict[str, list[int]]:
    """
    Build a dictionary mapping name variations to lot numbers.
    Returns: dict where keys are name variations and values are lists of lot numbers
    """
    with open(address_file, "r") as f:
        addresses = json.load(f)

    lot_lookup = {}

    for lot_key, lot_data in addresses.items():
        # Skip entries that reference another lot (billing_lot)
        if "billing_lot" in lot_data:
            continue

        # Get the lots this entry covers
        lots = lot_data.get("lots", [int(lot_key)])

        # Process each owner name
        for name in lot_data.get("name", []):
            # Generate all variations of this name
            variations = generate_name_variations(name)

            # Add each variation to the lookup
            for variation in variations:
                lot_lookup[variation] = lots

    return lot_lookup


def is_applicable(event: FinancialEvent) -> bool:
    """
    Determine if the given FinancialEvent should be journalized.
    """

    # If the from or to account are two "assets:truist" accounts, then we can journalize it.
    if event.from_account and event.from_account.startswith("assets:truist"):
        return True

    if event.to_account and event.to_account.startswith("assets:truist"):
        return True

    if event.event_id.startswith("venmo"):
        # check for keywords
        keywords = [
            "mbla",
            "miles branch",
            "mbloa",
            "dues",
            "lot",
            "lonna",
            "carson",
            "lauren",
            "hoa",
            "mbhoa",
        ]
        if event.memo is None:
            return False

        for keyword in keywords:
            if keyword in event.memo.lower():
                return True
        return False

    return True


def print_journal_entry(
    entry: FinancialEvent, lot_lookup: dict[str, list[int]]
) -> None:
    lot = lot_lookup.get(entry.description, [])

    print(f"  Date: {entry.posted_date}")
    print(f"  Description: {entry.description}")
    if entry.memo:
        print(f"  Memo: {entry.memo}")
    print(f"  Amount: {entry.amount}")
    if lot:
        print(f"  Lot(s): {', '.join(str(l) for l in lot)}")
    print("  Accounts:")

    print(f"    - {entry.from_account}: {-entry.amount}")
    print(f"    - {entry.to_account}: {-entry.amount}")
    print()


def create_journal_entries(journal: Journal, events: List[FinancialEvent]) -> None:
    # Usage
    lot_lookup = build_lot_lookup(config.PROJECT_ROOT / "MilesBranch.json")

    skipped = []

    for event in events:
        if not is_applicable(event):
            skipped.append(event)
            continue

        print_journal_entry(event, lot_lookup)

    print("\nSkipped events:")
    for event in skipped:
        print(
            f"  - {event.event_id}: {event.description}, {event.memo}, ({event.amount}), from: {event.from_account}, to: {event.to_account}    "
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: List[str]) -> int:
    if not argv:
        # No args means stdin
        events = read_events_from_stream(sys.stdin)
    else:
        events = read_events_from_files(argv)

    journal = Journal(config.DATABASE)
    create_journal_entries(journal, events)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
