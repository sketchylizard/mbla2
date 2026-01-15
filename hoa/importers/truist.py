#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, DefaultDict, Tuple
import csv
import re
import sys
import yaml

from hoa import config
from hoa.models import DepositAnnotation, CheckDetail, Invoice, Transaction, Source
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
    from hoa.models import TxType

    posted_date = parse_date(row["Posted Date"])
    amount = parse_amount(row["Amount"])
    description = row.get("Full description", "").strip()
    source_type = row.get("Transaction Type", "").strip().lower()
    check_number = row.get("Check/Serial #", "").strip()

    # Determine semantic type and accounts based on amount and description

    # Handle known transfer patterns first
    match = re.match(r"ONLINE (FROM|TO) \**(\d+)", description)
    if match:
        direction = match.group(1)
        other_account = accounts.normalize(match.group(2))

        if direction == "FROM":
            assert amount > 0, f"Expected positive amount for {description}"
            return Transaction(
                event_id=event_id,
                posted_date=posted_date,
                amount=amount,
                type=TxType.transfer,
                from_account=other_account,
                to_account=account,
                reference=check_number or None,
                description=f"Transfer from {other_account}",
                memo=None,
                source=Source(str(path), line_no),
            )

        if direction == "TO":
            assert amount < 0, f"Expected negative amount for {description}"
            return Transaction(
                event_id=event_id,
                posted_date=posted_date,
                amount=abs(amount),
                type=TxType.transfer,
                from_account=account,
                to_account=other_account,
                reference=check_number or None,
                description=f"Transfer to {other_account}",
                memo=None,
                source=Source(str(path), line_no),
            )

    match = re.match(r"CASHOUT VENMO (\d+)", description)
    if match:
        return Transaction(
            event_id=event_id,
            posted_date=posted_date,
            amount=amount,
            type=TxType.transfer,
            from_account="assets:venmo",
            to_account=account,
            reference=check_number or None,
            description=f"Transfer from Venmo {match.group(1)}",
            memo=None,
            source=Source(str(path), line_no),
        )

    # Money leaving (negative amount)
    if amount < 0:
        # Determine semantic type based on description
        if description.startswith("Check #"):
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
            reference=check_number or None,
            description=description,
            memo=None,
            source=Source(str(path), line_no),
        )

    # Money arriving (positive amount)
    assert amount > 0, f"Expected positive amount for {description}"
    return Transaction(
        event_id=event_id,
        posted_date=posted_date,
        amount=amount,
        type=TxType.deposit,
        from_account=None,
        to_account=account,
        reference=check_number or None,
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


def extract_deposit_annotations(yaml_file: Path) -> List[DepositAnnotation]:
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
                        invoice=Invoice.from_str(check.get("invoice")) or None,
                    )
                )
            deposits.append(DepositAnnotation(date=date, checks=checks))

    return deposits


def apply_deposit_annotations(
    transactions: List[Transaction],
    annotations_path: Path,
    max_days_after: int = 14,  # Bank posts within 2 weeks of deposit
) -> List[Transaction]:
    """Apply deposit annotations to transactions"""
    from hoa.models import TxType
    from datetime import timedelta

    # Load all deposit annotations
    all_deposits = []
    if annotations_path.is_dir():
        for path in sorted(annotations_path.glob("*.yaml")):
            all_deposits.extend(extract_deposit_annotations(path))

    # Sort both lists by date
    all_deposits.sort(key=lambda d: d.date)
    transactions.sort(key=lambda t: t.posted_date)

    print(f"\nLoaded {len(all_deposits)} deposit annotations")
    print(f"Processing {len(transactions)} transactions")

    # Filter to only deposit transactions
    deposit_txns = [
        txn
        for txn in transactions
        if txn.type == TxType.deposit and txn.to_account == "assets:truist:checking"
    ]

    print(f"Found {len(deposit_txns)} deposit transactions to match")

    matched_count = 0
    unmatched_deposits = []
    unmatched_txns = []

    # Create a working copy of transactions that we can mark as matched
    txn_matched = {id(txn): False for txn in deposit_txns}

    # For each deposit annotation, find matching transaction
    for deposit in all_deposits:
        match_found = False

        # Look for transactions on or after deposit date (within window)
        for txn in deposit_txns:
            # Skip if already matched
            if txn_matched[id(txn)]:
                continue

            # Check if this transaction could match this deposit
            if (
                txn.posted_date >= deposit.date
                and txn.posted_date <= deposit.date + timedelta(days=max_days_after)
                and txn.amount == deposit.total_amount
            ):

                print(
                    f"MATCH: Deposit {deposit.date} ${deposit.total_amount} → Transaction {txn.posted_date}"
                )

                # Mark this transaction as matched
                txn_matched[id(txn)] = True
                matched_count += 1
                match_found = True

                # Attach annotation to transaction
                # We need to update the transaction in the original list
                break

        if not match_found:
            unmatched_deposits.append(deposit)

    # Find unmatched transactions
    for txn in deposit_txns:
        if not txn_matched[id(txn)]:
            unmatched_txns.append(txn)

    # Now rebuild the transactions list with annotations
    # Create a map of deposit annotations by (date_range, amount)
    deposit_map = {}
    for deposit in all_deposits:
        for days_offset in range(max_days_after + 1):
            key = (deposit.date + timedelta(days=days_offset), deposit.total_amount)
            if key not in deposit_map:  # First match wins
                deposit_map[key] = deposit

    # Apply annotations to transactions
    annotated = []
    for txn in transactions:
        if txn.type == TxType.deposit and txn.to_account == "assets:truist:checking":

            key = (txn.posted_date, txn.amount)
            if key in deposit_map:
                deposit = deposit_map[key]
                txn = txn.with_updates(
                    description=deposit.description, annotation=deposit
                )
                # Remove from map so it's not used again
                del deposit_map[key]

        annotated.append(txn)

    # Report results
    print(f"\nMatched {matched_count} deposits")

    if unmatched_deposits:
        print(f"\nWarning: {len(unmatched_deposits)} unmatched deposit annotations:")
        for d in unmatched_deposits:
            print(f"  {d.date}: ${d.total_amount} - {d.description}")
            for check in d.checks:
                print(
                    f"    - Check {check.check_number or 'CASH'} from {check.payer_name} for ${check.amount} (Invoice: {check.invoice})"
                )

    if unmatched_txns:
        print(
            f"\nWarning: {len(unmatched_txns)} bank transactions without annotations:"
        )
        for txn in unmatched_txns:
            print(f"  {txn.posted_date}: ${txn.amount} - {txn.description}")

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
