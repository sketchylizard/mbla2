#!/usr/bin/env python3
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List
import re  # NEW
import yaml
import sys

from hoa.models import (
    Invoice,
    Posting,
    Transaction,
    Source,
    TxType,
)  # added Invoice, Posting
from hoa import config


def _build_opening_postings(
    entry_account: str,
    equity_account: str,
    amount: Decimal,  # positive = debit balance, negative = credit balance
    opening_date: date,
) -> List[Posting]:
    """
    Build a balanced pair of postings for an opening balance entry.

    The YAML uses debit/credit from the HOA's perspective:
      debit:  normal asset balance (e.g. cash in bank, amounts owed to us)
      credit: contra-asset balance (e.g. overpayment -- we owe them)

    amount = debit - credit, so:
      amount > 0  -> debit balance  -> positive posting in entry_account
      amount < 0  -> credit balance -> negative posting in entry_account
    """
    invoice = None

    # If this is a receivables account, extract lot number and build invoice.
    # Serial 99 is reserved for opening balance adjustments.
    match = re.match(r"assets:receivables:lot(\d+)", entry_account)
    if match:
        lot = int(match.group(1))
        invoice = Invoice.create(year=opening_date.year, lot=lot, serial=99)

    return [
        Posting(account=entry_account, amount=amount, invoice=invoice),
        Posting(account=equity_account, amount=-amount),
    ]


def extract_events(path: Path) -> List[Transaction]:
    events: List[Transaction] = []

    yaml_data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))

    opening_date = yaml_data.get("date")
    description = yaml_data.get("description", "Opening balance")
    equity_account = yaml_data.get("account", "equity:opening_balances")

    for entry in yaml_data.get("balances", []):
        debit = Decimal(str(entry.get("debit", "0")))
        credit = Decimal(str(entry.get("credit", "0")))
        amount = debit - credit  # positive = normal asset debit balance

        entry_account = entry["account"]

        postings = _build_opening_postings(
            entry_account, equity_account, amount, opening_date
        )

        event = Transaction(
            posted_date=opening_date,
            description=description,
            amount=abs(amount),
            bank="manual",
            type=TxType.manual,
            from_account=None,
            to_account=None,
            postings=postings,
            source=Source(file=str(path), line=0),
        )
        events.append(event)

    return events


def process() -> List[Transaction]:
    events: List[Transaction] = []

    manual_root = config.SOURCES / "manual"

    files = sorted(manual_root.glob("*.yaml"))
    for path in files:
        events.extend(extract_events(path))

    return events


# ----------------------------
# CLI
# ----------------------------


def main(argv: list[str]) -> int:
    if not argv:
        print(
            "Usage: manual.py <file.csv | directory> [...]",
            file=sys.stderr,
        )
        return 2

    all_events = process()

    Transaction.write_ndjson(all_events, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
