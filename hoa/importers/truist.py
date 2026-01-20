#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, DefaultDict, Dict, Tuple
import csv
import re
import sys
import yaml

from hoa import config
from hoa.annotation import Annotation
from hoa.models import Invoice, Transaction, Source, TxType
from hoa import accounts
from hoa.members import MemberDirectory, Lot

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_amount(value: str) -> Decimal:
    value = value.replace("$", "").replace(",", "").replace(" ", "").strip()
    if not value:
        return Decimal("0")

    # if the value is surrounded by parentheses, it's negative
    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1].strip()
    return Decimal(value)


def parse_date(value: str) -> str:
    # Truist format is usually MM/DD/YYYY
    date = datetime.strptime(value.strip(), "%m/%d/%Y").date()
    return date


def parse_reference(check_number: str, description: str) -> str | None:
    check_number = check_number.strip()
    if check_number:
        return check_number

    # Try to extract from description
    match = re.search(r"Check\s*#\s*(\d+)", description, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def transaction_from_csv_row(
    row: dict,
    account: str,
    event_id: str,
    path: Path,
    line_no: int,
) -> Transaction:
    """
    Create a Transaction from a CSV row.

    Normalizes bank terminology into semantic transaction types.
    """

    posted_date = parse_date(row["Posted Date"])
    amount = parse_amount(row["Amount"])
    description = row.get("Full description", "").strip().capitalize()
    check_number = row.get("Check/Serial #", "").strip()
    category_name = row.get("Category name", "").strip().lower()

    reference = parse_reference(check_number, description)

    # Determine semantic type and accounts based on amount and description

    # Handle known transfer patterns first
    match = re.match(r"Online (from|to) \**(\d+)", description)
    if match:
        direction = match.group(1)
        other_account = accounts.normalize(match.group(2))

        if direction == "from":
            assert amount > 0, f"Expected positive amount for {description}"
            return Transaction(
                event_id=event_id,
                posted_date=posted_date,
                amount=amount,
                type=TxType.transfer,
                from_account=other_account,
                to_account=account,
                reference=reference,
                description=f"Transfer from {other_account}",
                memo=None,
                source=Source(str(path), line_no),
            )

        if direction == "to":
            assert amount < 0, f"Expected negative amount for {description}"
            return Transaction(
                event_id=event_id,
                posted_date=posted_date,
                amount=abs(amount),
                type=TxType.transfer,
                from_account=account,
                to_account=other_account,
                reference=reference or None,
                description=f"Transfer to {other_account}",
                memo=None,
                source=Source(str(path), line_no),
            )

    match = re.match(r"Cashout venmo (\d+)", description)
    if match:
        return Transaction(
            event_id=event_id,
            posted_date=posted_date,
            amount=amount,
            type=TxType.transfer,
            from_account="assets:venmo",
            to_account=account,
            reference=reference,
            description=f"Transfer from Venmo {match.group(1)}",
            memo=None,
            source=Source(str(path), line_no),
        )

    # Money leaving (negative amount)
    if amount < 0:
        # Determine semantic type based on description
        if reference:
            tx_type = TxType.check
        elif "FEE" in description.upper():
            tx_type = TxType.fee
        else:
            tx_type = TxType.debit

        return Transaction(
            event_id=event_id,
            posted_date=posted_date,
            amount=abs(amount),
            type=tx_type,
            from_account=account,
            to_account=None,
            reference=reference,
            description=description,
            memo=None,
            source=Source(str(path), line_no),
        )

    # Money arriving (positive amount)
    assert amount > 0, f"Expected positive amount for {description}"

    if category_name == "deposits" and description.lower() in (
        "mobile deposit",
        "deposit",
        "counter deposit",
    ):
        assert reference is None, f"Expected no reference for deposit: {description}"
        tx_type = TxType.deposit
    else:
        tx_type = TxType.credit

    return Transaction(
        event_id=event_id,
        posted_date=posted_date,
        amount=amount,
        type=tx_type,
        from_account=None,
        to_account=account,
        reference=reference,
        description=description,
        memo=None,
        source=Source(str(path), line_no),
    )


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

Key = Tuple[str, date]


def extract_one_account(
    f: __file__,
    path: Path,
    line_no: int,
) -> Tuple[str, List[Transaction], int] | None:
    # An account section starts with the account name, followed by a CSV header, then the transactions.
    events: List[Transaction] = []

    counters: dict[Key, int] = DefaultDict(int)

    # Skip empty lines until we find the account name
    while True:
        line = f.readline()
        line_no += 1
        if not line:
            return None
        if line.strip():
            break

    if not line:
        return None

    account = accounts.normalize(line)
    # Add the chart of accounts prefix to the account name
    id_prefix = account.replace("assets:", "")

    headers = f.readline().strip().split(",")
    line_no += 1

    for line in f:
        line_no += 1

        if not line.strip():
            break

        row = csv.DictReader([line], fieldnames=headers).__next__()
        posted_date = parse_date(row["Posted Date"])

        source_type = row.get("Transaction Type", "").strip().lower()
        counters[(posted_date, source_type)] += 1
        ordinal = counters[(posted_date, source_type)]

        check_number = row.get("Check/Serial #", "").strip()
        event_id = f"{id_prefix}:{posted_date}:{str(source_type)}:{ordinal:02}"
        if check_number:
            event_id += f":{check_number}"

        event = transaction_from_csv_row(row, account, event_id, path, line_no)

        events.append(event)

    return (account, events, line_no)


def can_merge_transfers(a, b):
    return (
        a.transfer_source is None
        and a.type == b.type == "transfer"
        and a.posted_date == b.posted_date
        and a.amount == b.amount
        and a.from_account == b.from_account
        and a.to_account == b.to_account
    )


def merge_intra_bank_transfers(events_a, events_b):
    output = []

    i = j = 0
    a_len = len(events_a)
    b_len = len(events_b)

    while i < a_len and j < b_len:
        # Drain non-transfers from A
        while i < a_len and events_a[i].type != "transfer":
            output.append(events_a[i])
            i += 1

        # Drain non-transfers from B
        while j < b_len and events_b[j].type != "transfer":
            output.append(events_b[j])
            j += 1

        if i >= a_len or j >= b_len:
            break

        a = events_a[i]
        b = events_b[j]

        if can_merge_transfers(a, b):
            output.append(a.with_transfer_source(b.source))
        else:
            output.append(a)
            output.append(b)
        i += 1
        j += 1

    # Append leftovers
    output.extend(events_a[i:])
    output.extend(events_b[j:])

    return output


def extract_events(path: Path) -> List[Transaction]:
    events: List[Transaction] = []

    with path.open(encoding="utf-8-sig") as f:
        # There should be two sections, one for each of the accounts (checking & savings). We should be sitting on the
        # first line of an account section, which is the account name. Keep reading until we EOF.

        line_no = 0

        while f:
            results = extract_one_account(f, path, line_no)
            if results is None:
                # EOF reached
                break
            account, new_events, line_no = results
            events = merge_intra_bank_transfers(events, new_events)

    return events


def load_yaml(path: Path) -> Any:
    """Load YAML file, returns empty dict if file doesn't exist"""
    if not path.exists():
        return {}

    with path.open("r") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: Path, data: Any) -> None:
    """Save data to YAML file"""
    with path.open("w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def normalize_references(
    events: List[Transaction], counter_file: Path
) -> List[Transaction]:
    """Ensure that deposits and electronic checks have unique references."""

    counters = load_yaml(counter_file) if counter_file.exists() else {}

    events.sort(key=lambda e: (e.posted_date, e.source.line if e.source else 0))

    annotated_events = []
    for event in events:
        if event.type == TxType.deposit:
            year = event.posted_date.year
            count = counters.get(year, 0) + 1
            counters[year] = count
            event = replace(event, reference=f"dep:{year}-{count:03d}")
        elif event.reference is not None and event.reference.startswith("975"):
            # Electronic checks and ACH transfers from the HOA's bank account are assigned a reference number
            # starting with 975. This number seems to be unique within a year, but we want it to be unique across
            # all years. So we prefix it with the year.
            year = event.posted_date.year
            event = replace(event, reference=f"chk:{year}-{event.reference[3:]}")

        annotated_events.append(event)

    save_yaml(counter_file, counters)
    return annotated_events


def apply_annotations(
    events: List[Transaction], annotation_root: Path
) -> List[Transaction]:
    """Apply annotations to the given events, returning a new list of events with annotations applied."""
    annotations = Annotation.load_all(annotation_root)

    annotations.sort(key=lambda a: a.reference)

    events.sort(key=lambda e: (e.reference or "~~~~~~~~~~"))

    for annotation in annotations:
        found = False
        for txn_index, event in enumerate(events):
            if annotation.matches(event):
                events[txn_index] = annotation.apply(event)
                # annotation should only match one transaction
                found = True
                break

        if not found:
            # If we didn't find a match, we can log a warning or raise an error
            print(
                f"Warning: No match found for annotation {annotation.reference} in events",
                file=sys.stderr,
            )

    return events


def process(truist_root: Path) -> List[Transaction]:
    # Stage 1: Extract raw transactions from CSV
    events: List[Transaction] = []

    statements_path = truist_root / "statements"
    if not statements_path.is_dir():
        raise FileNotFoundError(f"Expected directory: {statements_path}")

    for path in sorted(statements_path.glob("*.csv")):
        file_events = extract_events(path)
        events.extend(file_events)

    events = normalize_references(events, truist_root / "deposit_counter.yaml")

    # Stage 2: Load all annotations
    events = apply_annotations(events, truist_root / "annotations")

    # Stage 3: Apply categorization rules
    #    events = apply_categorization_rules(events, rules)

    # Stage 4: Apply deposit annotations
    #    events = apply_deposit_annotations(events, deposits)

    return events


# ----------------------------
# CLI
# ----------------------------


def main(argv: list[str]) -> int:
    if not argv:
        print(
            "Usage: truist.py <file.csv | directory> [...]",
            file=sys.stderr,
        )
        return 2

    all_events: list[Transaction] = []

    all_events = process(config.SOURCES / "truist")

    Transaction.write_ndjson(all_events, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
