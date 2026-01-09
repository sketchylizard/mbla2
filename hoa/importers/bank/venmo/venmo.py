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

from hoa.models import FinancialEvent


# ----------------------------
# helpers
# ----------------------------


def parse_amount(value: str) -> Decimal:
    """
    Venmo amounts are strings like '+150.00' or '-42.75'
    """
    return Decimal(value.strip().replace(",", "").replace("$", "").replace(" ", ""))


def make_event_id(
    source_file: Path,
    line_no: int,
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
    return f"venmo:{source_file.name}:{line_no}"


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


def normalize_account(account: str) -> str:
    """
    Normalize Venmo account names to a consistent format.
    For example, "Venmo balance" becomes "assets:venmo"
    """
    account = account.strip().lower()
    if account == "venmo balance":
        return "assets:venmo"
    elif account.startswith("bank account"):
        return f"external:{account}"
    elif account.startswith("card"):
        return f"external:{account}"
    else:
        return f"external:{account}"


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
        from_account=normalize_account(ctx.funding_source),
        to_account="assets:venmo",
        counterparty=None,
    )


def handle_transfer_out(ctx: VenmoContext):
    return dict(
        actual_type="transfer",
        from_account="assets:venmo",
        to_account=normalize_account(ctx.destination),
        counterparty=None,
    )


def handle_payment(ctx: VenmoContext):
    payer, payee = normalize_payment_parties(ctx.venmo_type, ctx.from_, ctx.to)

    if ctx.amount < 0:
        # Me paying someone
        actual_type = "debit"
        from_account = (
            normalize_account(ctx.funding_source)
            if ctx.funding_source
            else "assets:venmo"
        )
        to_account = (None,)
        counterparty = (payee,)
        assert payer == "Jason Stewart"
    else:
        # Someone paying me
        actual_type = ("credit",)
        from_account = (None,)
        to_account = ("assets:venmo",)
        counterparty = (ctx.from_,)
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


def extract_events(path: Path) -> List[FinancialEvent]:
    events: List[FinancialEvent] = []

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
                posted_date = datetime.fromisoformat(row["Datetime"]).date()
                event_id = make_event_id(
                    source_file=path,
                    line_no=line_no,
                    reference=tx_id,
                )

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
                    FinancialEvent(
                        event_id=event_id,
                        posted_date=posted_date,
                        amount=abs(ctx.amount),
                        description=result["counterparty"],
                        memo=note or None,
                        from_account=result["from_account"],
                        to_account=result["to_account"],
                        type=result["actual_type"],
                        source_file=path,
                        source_line=line_no,
                        reference=tx_id,
                        source_type=source_type,
                    )
                )

            except Exception as e:
                raise RuntimeError(f"Error parsing {path.name}:{line_no}: {e}") from e

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
            "Usage: venmo.py <file.csv | directory> [...]",
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
