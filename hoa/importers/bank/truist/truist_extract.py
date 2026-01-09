#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import List, DefaultDict, Tuple
import csv
import re
import sys

from hoa.models import FinancialEvent


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


def normalize_account(name: str) -> str:
    """
    Normalize Truist account names into canonical account paths.
    """
    name = name.lower()
    if "checking" in name or "0947" in name:
        return "truist:checking"
    if "savings" in name or "9625" in name:
        return "truist:savings"
    return "truist:unknown"


def _fix_accounts(
    active_account: str, description: str, type: str, amount: Decimal
) -> dict:
    """
    If the description matches a known transfer pattern, return the from_account, to_account, type, and amount.
    Otherwise, deduce the from/to account from the sign of the amount.
    """
    match = re.match(r"ONLINE (FROM|TO) \**(\d+)", description)
    if match:
        direction = match.group(1)
        other_account = "assets:" + normalize_account(match.group(2))

        if direction == "FROM":
            assert amount > 0, f"Expected positive amount for {description}"
            return {
                "from": other_account,
                "to": active_account,
                "description": f"Transfer from {other_account}",
                "type": "transfer",
                "amount": amount,
            }

        if direction == "TO":
            assert amount < 0, f"Expected negative amount for {description}"
            return {
                "from": active_account,
                "to": other_account,
                "description": f"Transfer to {other_account}",
                "type": "transfer",
                "amount": abs(amount),
            }

        raise ValueError(
            f"Unexpected direction {direction} in description: {description}"
        )

    match = re.match(r"CASHOUT VENMO (\d+) JASON STEWART ACH CREDIT", description)
    if match:
        other_account = "assets:venmo"
        return {
            "from": "assets:venmo",
            "to": active_account,
            "description": f"Transfer from Venmo {match.group(1)}",
            "type": "transfer",
            "amount": amount,
        }

    if amount < 0:
        return {
            "from": active_account,
            "to": None,
            "description": description,
            "type": type,
            "amount": abs(amount),
        }

    return {
        "from": None,
        "to": active_account,
        "description": description,
        "amount": amount,
    }


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

Key = Tuple[str, date]


def extract_one_account(
    f: __file__,
    path: Path,
    line_no: int,
) -> List[FinancialEvent]:
    # An account section starts with the account name, followed by a CSV header, then the transactions.
    events: List[FinancialEvent] = []

    counters: dict[Key, int] = DefaultDict(int)

    # Skip empty lines until we find the account name
    while True:
        line = f.readline()
        line_no += 1
        if not line:
            return (events, line_no)
        if line.strip():
            break

    if not line:
        return (events, line_no)

    id_prefix = normalize_account(line)
    # Add the chart of accounts prefix to the account name
    account = f"assets:{id_prefix}"

    headers = f.readline().strip().split(",")
    line_no += 1

    for line in f:
        line_no += 1

        if not line.strip():
            break

        row = csv.DictReader([line], fieldnames=headers).__next__()
        posted_date = parse_date(row["Posted Date"])

        source_type = row.get("Transaction Type", "").strip().lower()
        counters[(posted_date, type)] += 1
        ordinal = counters[(posted_date, type)]
        event_id = f"{id_prefix}:{posted_date}:{type}:{ordinal:02}"

        check_number = row.get("Check/Serial #", "").strip()
        if check_number:
            event_id += f":{check_number}"

        # Amount sign convention:
        amount = parse_amount(row["Amount"])

        description = row.get("Full description", "").strip()

        values = _fix_accounts(account, description, source_type, amount)

        event = FinancialEvent(
            event_id=event_id,
            posted_date=posted_date,
            amount=amount,
            type=values.get("type") or source_type,
            from_account=values["from"],
            to_account=values["to"],
            reference=check_number or None,
            description=values["description"],
            memo=None,
            source_file=str(path),
            source_line=line_no,
            source_type=source_type,
        )

        events.append(event)

    return (events, line_no)


def extract_events(path: Path) -> List[FinancialEvent]:
    events: List[FinancialEvent] = []

    with path.open(encoding="utf-8-sig") as f:
        # There should be two sections, one for each of the accounts (checking & savings). We should be sitting on the
        # first line of an account section, which is the account name. Keep reading until we EOF.

        line_no = 0

        while f:
            new_events, line_no = extract_one_account(f, path, line_no)
            if not new_events:
                break
            events.extend(new_events)

    return events


# ----------------------------
# CLI
# ----------------------------


def iter_csv_files(args: list[str]) -> list[Path]:
    files: list[Path] = []

    for arg in args:
        path = Path(arg)

        if path.is_dir():
            files.extend(sorted(path.glob("*.csv")))
        elif path.is_file():
            files.append(path)
        else:
            raise FileNotFoundError(f"No such file or directory: {arg}")

    return files


def main(argv: list[str]) -> int:
    if not argv:
        print(
            "Usage: truist.py <file.csv | directory> [...]",
            file=sys.stderr,
        )
        return 2

    all_events: list[FinancialEvent] = []

    for csv_file in iter_csv_files(argv):
        events = extract_events(csv_file)
        all_events.extend(events)

    FinancialEvent.write_ndjson(all_events, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
