#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import List, DefaultDict, Tuple
import csv
import re
import sys
import yaml

from hoa import accounts
from hoa import config
from hoa.annotation import Annotation
from hoa.models import Invoice, merge_transfers, Transaction, Source, TxType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class Counter:
    def __init__(self):
        self.values = defaultdict(int)

    def increment(self, key: str) -> str:
        self.values[key] += 1
        return f"{key}-{self.values[key]:02d}"


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
    path: Path,
    line_no: int,
    deposit_counter: Counter,
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
    type: TxType = None

    # Handle known transfer patterns first
    match = re.match("Payment venmo", description)
    if match:
        assert amount < 0, f"Expected negative amount for {description}"
        return Transaction(
            posted_date=posted_date,
            amount=abs(amount),
            type=TxType.transfer,
            bank="truist",
            from_account=account,
            to_account="expenses:venmo:payments",
            reference=reference or None,
            description=f"Transfer from {account}",
            memo=None,
            source=Source(str(path), line_no),
        )

    match = re.match(r"Online (from|to) \**(\d+)", description)
    if match:
        direction = match.group(1)
        other_account = accounts.normalize(match.group(2))

        if direction == "from":
            assert amount > 0, f"Expected positive amount for {description}"

            return Transaction(
                posted_date=posted_date,
                amount=amount,
                type=TxType.transfer,
                bank="truist",
                from_account=other_account,
                to_account=account,
                reference=reference or None,
                description=f"Transfer from {other_account}",
                memo=None,
                source=Source(str(path), line_no),
            )

        if direction == "to":
            assert amount < 0, f"Expected negative amount for {description}"
            return Transaction(
                posted_date=posted_date,
                amount=abs(amount),
                type=TxType.transfer,
                bank="truist",
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
            posted_date=posted_date,
            amount=amount,
            type=TxType.transfer,
            bank="truist",
            from_account="assets:venmo",
            to_account=account,
            reference=reference,
            description=f"Transfer from Venmo {match.group(1)}",
            memo=None,
            source=Source(str(path), line_no),
        )

    match = re.match(r"Addfunds venmo", description, re.IGNORECASE)
    if match:
        return Transaction(
            posted_date=posted_date,
            amount=abs(amount),  # Make positive
            type=TxType.transfer,
            bank="truist",
            from_account=account,  # truist checking
            to_account="assets:venmo",
            reference=reference,
            description="Transfer to Venmo",
            memo=None,
            source=Source(str(path), line_no),
        )

    # Money leaving (negative amount)
    if amount < 0:
        # Determine semantic type based on description
        if reference:
            type = TxType.check
        elif "FEE" in description.upper():
            type = TxType.fee
        else:
            type = TxType.debit

        if reference is not None:
            match = re.match(r"9750(\d+)", reference)
            if match:
                # Truist bank check
                reference = f"chk-{posted_date.year}-{match.group(1)}"

        return Transaction(
            posted_date=posted_date,
            amount=abs(amount),
            type=type,
            bank="truist",
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
        year = posted_date.year
        reference = deposit_counter.increment(f"dep-{year}")
        type = TxType.deposit
    else:
        type = TxType.credit

    return Transaction(
        posted_date=posted_date,
        amount=amount,
        type=type,
        bank="truist",
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
    f: __file__, path: Path, line_no: int, deposit_counter: Counter
) -> Tuple[str, List[Transaction], int] | None:
    # An account section starts with the account name, followed by a CSV header, then the transactions.
    events: List[Transaction] = []

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
        event = transaction_from_csv_row(row, account, path, line_no, deposit_counter)

        events.append(event)

    return (account, events, line_no)


def extract_events(path: Path, deposit_counter: Counter) -> List[Transaction]:
    events: List[Transaction] = []

    with path.open(encoding="utf-8-sig") as f:
        # There should be two sections, one for each of the accounts (checking & savings). We should be sitting on the
        # first line of an account section, which is the account name. Keep reading until we EOF.

        line_no = 0

        while f:
            results = extract_one_account(f, path, line_no, deposit_counter)
            if results is None:
                # EOF reached
                break
            account, new_events, line_no = results
            events = merge_transfers(events, new_events)

    return events


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
                print(f"Applied annotation {annotation.reference} to event")
                break

        if not found:
            # If we didn't find a match, we can log a warning or raise an error
            print(
                f"Warning: No match found for annotation {annotation.reference} in events",
                file=sys.stderr,
            )

    return events


def process() -> List[Transaction]:
    # Stage 1: Extract raw transactions from CSV
    events: List[Transaction] = []

    deposit_counter = Counter()

    truist_root = config.SOURCES / "truist"

    counter_file = truist_root / "counters.yaml"
    if counter_file.is_file():
        with open(truist_root / "counters.yaml") as f:
            data = yaml.safe_load(f) or {}
            deposit_counter.values = defaultdict(int, data.get("deposit_counter", {}))

    statements_path = truist_root / "statements"
    if not statements_path.is_dir():
        raise FileNotFoundError(f"Expected directory: {statements_path}")

    for path in sorted(statements_path.glob("*.csv")):
        file_events = extract_events(path, deposit_counter)
        events.extend(file_events)

    # Stage 2: Load all annotations
    events = apply_annotations(events, truist_root / "annotations")

    # Stage 3: Apply categorization rules
    #    events = apply_categorization_rules(events, rules)

    # Stage 4: Apply deposit annotations
    #    events = apply_deposit_annotations(events, deposits)

    with open(counter_file, "w") as f:
        yaml.safe_dump({"deposit_counter": dict(deposit_counter.values)}, f)

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
