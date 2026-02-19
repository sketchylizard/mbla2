#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import List, Iterable, Optional
import csv
import sys

from hoa.models import Source, Transaction, TxType
from hoa import accounts
from hoa import config

# ----------------------------
# helpers
# ----------------------------


def parse_amount(value: str) -> Decimal:
    """
    Venmo amounts are strings like '+150.00' or '-42.75'
    """
    return Decimal(value.strip().replace(",", "").replace("$", "").replace(" ", ""))


def make_event_id(
    source: Source,
    reference: str | None = None,
) -> str:
    """
    Stable, reproducible event identifier.

    Priority:
    - Venmo transaction ID (if present)
    - fallback to file + line number
    """
    if reference:
        return f"venmo:{reference}"
    return f"venmo:{source.file}:{source.line_no}"


# ----------------------------
# extraction
# ----------------------------


class VenmoClass(Enum):
    add_funds = "add_funds"
    transfer = "transfer"
    payment = "payment"


VENMO_TYPE_MAP = {
    "add funds": VenmoClass.add_funds,
    "instant add funds": VenmoClass.add_funds,
    "standard transfer": VenmoClass.transfer,
    "instant transfer": VenmoClass.transfer,
    "charge": VenmoClass.transfer,
    "payment": VenmoClass.payment,
}


@dataclass
class VenmoContext:
    amount: Decimal
    venmo_type: VenmoClass
    from_: str
    to: str
    funding_source: str | None
    destination: str | None


def normalize_payment_parties(venmo_type, from_, to):
    if venmo_type == "charge":
        # Venmo swaps roles
        return to, from_  # payer, payee
    else:
        # payment
        return from_, to


def handle_add_funds(ctx: VenmoContext):
    return dict(
        actual_type=TxType.transfer,
        from_account=accounts.normalize(ctx.funding_source),
        to_account="assets:venmo",
        counterparty="Transfer to Venmo",
    )


def handle_transfer(ctx: VenmoContext):
    dest = ctx.destination.strip()
    if dest == "":
        to_account = "expenses:venmo:payments"
        description = ctx.from_
    else:
        to_account = accounts.normalize(dest)
        description = f"Transfer to {dest}"

    source = ctx.funding_source.strip()
    if source == "":
        from_account = "assets:venmo"
    else:
        from_account = accounts.normalize(source)
    return dict(
        actual_type=TxType.transfer,
        from_account=from_account,
        to_account=to_account,
        counterparty=description,
    )


# In venmo.py, handle_payment()


def handle_payment(ctx: VenmoContext):
    payer, payee = normalize_payment_parties(ctx.venmo_type, ctx.from_, ctx.to)

    if ctx.amount < 0:
        # Me paying someone
        actual_type = "debit"
        from_account = (
            accounts.normalize(ctx.funding_source)
            if ctx.funding_source
            else "assets:venmo"
        )
        to_account = None
        counterparty = payee
        assert payer == "Jason Stewart"
    else:
        # Someone paying me
        actual_type = TxType.credit
        from_account = None
        to_account = "assets:venmo"
        counterparty = ctx.from_
        assert payee == "Jason Stewart"

    return dict(
        actual_type=actual_type,
        from_account=from_account,
        to_account=to_account,
        counterparty=counterparty,
    )


VENMO_HANDLERS = {
    VenmoClass.add_funds: handle_add_funds,
    VenmoClass.transfer: handle_transfer,
    VenmoClass.payment: handle_payment,
}


class UnknownVenmoType(Exception):
    def __init__(self, venmo_type: str):
        super().__init__(f"Unknown Venmo transaction type: '{venmo_type}'")
        self.venmo_type = venmo_type


def extract_events(path: Path) -> List[Transaction]:
    events: List[Transaction] = []

    with path.open(newline="", encoding="utf-8-sig") as f:
        next(f)  # skip metadata
        next(f)  # skip metadata
        reader = csv.DictReader(f)

        for line_no, row in enumerate(reader, start=3):

            # Skip rows without a Venmo ID
            tx_id = (row.get("ID") or "").strip()
            if not tx_id:
                continue

            try:
                source = Source(file=str(path), line=line_no)

                posted_date = datetime.fromisoformat(row["Datetime"]).date()

                source_type = row["Type"].strip().lower()
                venmo_class = VENMO_TYPE_MAP.get(source_type)

                if venmo_class is None:
                    raise UnknownVenmoType(source_type)

                amount = parse_amount(row["Amount (total)"])
                ctx = VenmoContext(
                    amount=amount,
                    venmo_type=source_type,
                    from_=row.get("From", "").strip(),
                    to=row.get("To", "").strip(),
                    funding_source=row.get("Funding Source", "").strip(),
                    destination=row.get("Destination", "").strip(),
                )

                result = VENMO_HANDLERS[venmo_class](ctx)

                note = (row.get("Note") or "").strip()
                note = note.replace("\n", " | ")

                events.append(
                    Transaction(
                        posted_date=posted_date,
                        amount=abs(ctx.amount),
                        bank="venmo",
                        description=result["counterparty"],
                        memo=note or None,
                        from_account=result["from_account"],
                        to_account=result["to_account"],
                        type=result["actual_type"],
                        reference=tx_id,
                        source=source,
                    )
                )

            except Exception as e:
                raise RuntimeError(f"Error parsing {path.name}:{line_no}: {e}") from e

    return events


def process() -> List[Transaction]:
    events: List[Transaction] = []

    venmo_root = config.SOURCES / "venmo"

    statements_path = venmo_root / "statements"
    files = sorted(statements_path.glob("*.csv"))
    for path in files:
        events.extend(extract_events(path))

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

    all_events = process(config.SOURCES / "venmo")

    Transaction.write_ndjson(all_events, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
