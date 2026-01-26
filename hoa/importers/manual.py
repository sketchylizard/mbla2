#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import List, DefaultDict, Tuple
import yaml
import sys

from hoa.models import Transaction, Source, TxType
from hoa import accounts, config


def extract_events(path: Path) -> List[Transaction]:
    events: List[Transaction] = []

    yaml_data = yaml.safe_load(path.read_text(encoding="utf-8-sig"))

    opening_date = yaml_data.get("date")
    description = yaml_data.get("description", "Opening balance")
    account = yaml_data.get("account", "equity:opening_balances")

    for entry in yaml_data.get("balances", []):
        credit = Decimal(str(entry.get("credit", "0")))
        debit = Decimal(str(entry.get("debit", "0")))
        amount = credit - debit
        event = Transaction(
            posted_date=opening_date,
            description=description,
            amount=amount,
            bank="manual",
            type=TxType.manual,
            from_account=entry["account"],
            to_account=account,
            source=Source(file=str(path), line=0),
        )
        events.append(event)

    return events


def process(manual_root: Path) -> List[Transaction]:
    events: List[Transaction] = []

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

    all_events: list[Transaction] = []

    all_events = process(config.SOURCES / "manual")

    Transaction.write_ndjson(all_events, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
