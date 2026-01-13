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
import yaml

from hoa import config
from hoa.models import DepositAnnotation, CheckDetail, Transaction, Source
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
        other_account = accounts.normalize(match.group(2))

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

    assert amount > 0, f"Expected non-negative amount for {description}"

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
        event_id = f"{id_prefix}:{posted_date}:{str(source_type)}:{ordinal:02}"

        check_number = row.get("Check/Serial #", "").strip()
        if check_number:
            event_id += f":{check_number}"

        # Amount sign convention:
        amount = parse_amount(row["Amount"])

        description = row.get("Full description", "").strip()

        values = _fix_accounts(account, description, source_type, amount)

        event = Transaction(
            event_id=event_id,
            posted_date=posted_date,
            amount=values["amount"],
            type=values.get("type") or source_type,
            from_account=values["from"],
            to_account=values["to"],
            reference=check_number or None,
            description=values["description"],
            memo=None,
            source=Source(str(path), line_no),
        )

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


def extract_deposits(yaml_file: Path) -> List[DepositAnnotation]:
    deposits = []

    with yaml_file.open(encoding="utf-8-sig") as f:
        data = yaml.safe_load(f)

        for dep in data["deposits"]:

            date = dep["date"]
            checks = []

            for check in dep["checks"]:
                checks.append(
                    CheckDetail(
                        check_number=check.get("check"),  # None for cash
                        payer_name=check["name"],
                        amount=Decimal(check["amount"]),
                        lot=check.get("lot") or None,
                        invoice=check.get("invoice") or None,
                    )
                )
            deposits.append(DepositAnnotation(date=date, checks=checks))

    return deposits


def apply_deposit_annotations(
    transactions: List[Transaction],
    annotations_path: Path,
) -> List[Transaction]:
    """Apply deposit annotations to transactions"""

    # Load all deposit annotations
    all_deposits = []
    if annotations_path.is_dir():
        for path in sorted(annotations_path.glob("deposits*.yaml")):
            all_deposits.extend(extract_deposits(path))

    # Sort by date for predictable matching
    all_deposits.sort(key=lambda d: d.date)

    # Match and annotate
    annotated = []
    for txn in transactions:
        if (
            txn.type in ("deposit", "credit")
            and txn.to_account == "assets:truist:checking"
        ):

            for idx, deposit in enumerate(all_deposits):
                if (
                    deposit.total_amount == txn.amount
                    and deposit.date <= txn.posted_date
                ):
                    deposit = all_deposits.pop(idx)
                    txn = txn.with_updates(
                        description=deposit.description,
                        annotation=deposit,  # Just attach it
                    )
                    break

        annotated.append(txn)

    # Report unmatched deposits
    if all_deposits:
        print(f"\nWarning: {len(all_deposits)} unmatched deposit annotations:")
        for d in all_deposits:
            print(f"  {d.date}: ${d.total_amount} - {d.description}")

    return annotated


def process(truist_root: Path) -> List[Transaction]:
    events: List[Transaction] = []

    statements_path = truist_root / "statements"
    if not statements_path.is_dir():
        raise FileNotFoundError(f"Expected directory: {statements_path}")

    for path in sorted(statements_path.glob("*.csv")):
        events.extend(extract_events(path))

    deposits_path = truist_root / "annotations"

    events = apply_deposit_annotations(events, deposits_path)

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
