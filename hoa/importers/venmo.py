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

from hoa.models import Transaction, Source
from hoa import accounts


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
    ADD_FUNDS = "add_funds"
    TRANSFER_OUT = "transfer_out"
    PAYMENT = "payment"


VENMO_TYPE_MAP = {
    "add funds": VenmoClass.ADD_FUNDS,
    "instant add funds": VenmoClass.ADD_FUNDS,
    "standard transfer": VenmoClass.TRANSFER_OUT,
    "instant transfer": VenmoClass.TRANSFER_OUT,
    "payment": VenmoClass.PAYMENT,
    "charge": VenmoClass.PAYMENT,
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
        actual_type="transfer",
        from_account=accounts.normalize(ctx.funding_source),
        to_account="assets:venmo",
        counterparty=None,
    )


def handle_transfer_out(ctx: VenmoContext):
    return dict(
        actual_type="transfer",
        from_account="assets:venmo",
        to_account=accounts.normalize(ctx.destination),
        counterparty=None,
    )


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
        actual_type = "credit"
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
    VenmoClass.ADD_FUNDS: handle_add_funds,
    VenmoClass.TRANSFER_OUT: handle_transfer_out,
    VenmoClass.PAYMENT: handle_payment,
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
                event_id = make_event_id(source, reference=tx_id)

                source_type = row["Type"].strip().lower()
                venmo_class = VENMO_TYPE_MAP.get(source_type)

                if venmo_class is None:
                    raise UnknownVenmoType(source_type)

                ctx = VenmoContext(
                    amount=parse_amount(row["Amount (total)"]),
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
                        event_id=event_id,
                        posted_date=posted_date,
                        amount=abs(ctx.amount),
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


def process(venmo_root: Path) -> List[Transaction]:
    events: List[Transaction] = []

    files = sorted(venmo_root.glob("*.csv"))
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

    all_events = process(config.SOURCES / "truist")

    Transaction.write_ndjson(all_events, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
