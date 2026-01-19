from __future__ import annotations

from dataclasses import dataclass, replace, asdict
from datetime import date
from datetime import date
from decimal import Decimal
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from hoa import config
from pathlib import Path
from typing import Callable, List, Protocol
from typing import Optional, Iterable, TextIO, Dict, Any, Self
import json


@dataclass(frozen=True)
class Source:
    """
    Provenance information for a Transaction.

    This is a value object: it has no independent identity and
    is always embedded in a Transaction.
    """

    file: str  # path relative to sources/
    line: int | None  # line number or item index within file


@dataclass(frozen=True)
class Invoice:
    """
    Invoice number format: YYYYLLXX
    - YYYY: 4-digit year
    - LL: 2-digit lot number (zero-padded)
    - XX: 2-digit serial (00 = dues invoice)
    """

    invoice_number: str

    def __post_init__(self):
        # Coerce to string in case it came in as int
        invoice_str = str(self.invoice_number)

        # Use object.__setattr__ because dataclass is frozen
        object.__setattr__(self, "invoice_number", invoice_str)

        if len(invoice_str) != 8:
            raise ValueError(f"Invoice number must be 8 characters, got: {invoice_str}")

        # Validate format
        if not invoice_str.isdigit():
            raise ValueError(f"Invoice number must be all digits, got: {invoice_str}")

    @property
    def year(self) -> int:
        return int(self.invoice_number[0:4])

    @property
    def lot(self) -> int:
        return int(self.invoice_number[4:6])

    @property
    def serial(self) -> int:
        return int(self.invoice_number[6:8])

    @property
    def is_dues(self) -> bool:
        return self.serial == 0

    def __str__(self) -> str:
        return self.invoice_number

    @classmethod
    def create(cls, year: int, lot: int, serial: int = 0) -> Self:
        """Factory method to create invoice from components"""
        invoice_str = f"{year:04d}{lot:02d}{serial:02d}"
        return cls(invoice_str)

    @classmethod
    def from_str(cls, s: str | None) -> Self | None:
        """Safe constructor that returns None for empty/None input"""
        if not s:
            return None
        return cls(s)


@dataclass
class Posting:
    account: str = ""
    amount: Decimal = Decimal(0)
    invoice: Invoice | None = None
    reference: str | None = None

    @classmethod
    def from_annotation_dict(cls, d: dict) -> "Posting":
        return cls(
            account=d["account"],
            amount=Decimal(str(d["amount"])),
            lot=d.get("lot"),
            invoice=Invoice.create(d.get("invoice")),
        )


class Annotation(Protocol):
    """Protocol for transaction annotations"""

    def get_postings(self, txn: Transaction) -> list[Posting]:
        """Generate postings for this annotation"""
        ...


@dataclass(frozen=True)
class Transaction:
    # Identity / ordering
    event_id: str
    posted_date: date
    amount: Decimal
    # Source provenance
    source: Source

    # Accounts (may be inferred later)
    from_account: str | None = None
    to_account: str | None = None
    type: str | None = None  # "debit", "credit", "transfer".
    reference: str | None = None  # Venmo ID, check number, ref #
    postings: List[Posting] | None = None

    # Descriptive information
    description: str = ""
    memo: str | None = None
    annotation: Annotation | None = None

    # Transfer provenance (for transfer events, the matching event)
    transfer_source: Source | None = None  # for transfers, the matching event

    # ---- helpers ----

    def with_updates(self, **changes) -> "Transaction":
        return replace(self, **changes)

    def with_transfer_source(self, source: Source) -> "Transaction":
        if self.transfer_source is not None:
            raise ValueError("transfer_source already set")
        return replace(self, transfer_source=source)

    def hash(self) -> str:
        parts = [
            self.from_account or "",
            self.to_account or "",
            self.posted_date.isoformat(),
            self.type,
            _normalize(self.description),
            str(self.amount),
            str(self.event_id),
        ]

        data = "\x1f".join(parts)
        return sha256(data.encode("utf-8")).hexdigest()

    # ---- NDJSON I/O ----

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["posted_date"] = self.posted_date.isoformat()
        d["amount"] = str(self.amount)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Transaction":
        source_file = data.get("source_file")
        source_line = data.get("source_line")
        source = Source(file=source_file, line=source_line)

        transfer_source_file = data.get("transfer_source_file") or None
        transfer_source_line = data.get("transfer_source_line") or None
        transfer_source = (
            Source(file=transfer_source_file, line=transfer_source_line)
            if transfer_source_file is None or transfer_source_line is None
            else None
        )

        return cls(
            event_id=data["event_id"],
            posted_date=date.fromisoformat(data["posted_date"]),
            amount=Decimal(data["amount"]),
            type=data.get("type"),
            from_account=data.get("from_account"),
            to_account=data.get("to_account"),
            reference=data.get("reference"),
            description=data.get("description", ""),
            memo=data.get("memo"),
            source=source,
            transfer_source=transfer_source,
        )

    @classmethod
    def read_ndjson(cls, stream: TextIO) -> Iterable["Transaction"]:
        for line_no, line in enumerate(stream, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield cls.from_dict(json.loads(line))
            except Exception as e:
                raise RuntimeError(f"Invalid Transaction at line {line_no}: {e}")

    @classmethod
    def write_ndjson(
        cls,
        events: Iterable["Transaction"],
        stream: TextIO,
    ) -> None:
        class PathEncoder(json.JSONEncoder):
            def default(self, obj):
                from pathlib import Path

                if isinstance(obj, Path):
                    return str(obj)
                return super().default(obj)

        for ev in events:
            json.dump(ev.to_dict(), stream, separators=(",", ":"), cls=PathEncoder)
            stream.write("\n")


class TxType(str, Enum):
    fee = "fee"
    deposit = "deposit"
    credit = "credit"
    check = "check"
    debit = "debit"
    transfer = "transfer"

    @classmethod
    def from_str(cls, raw: str) -> "TxType":
        """
        Parse the CSV type string and return a TxType enum.
        Converts to uppercase to match the Enum values.
        """
        try:
            return cls(raw.strip().lower())
        except ValueError:
            raise ValueError(f"Unknown transaction type: {raw}")


class BankAccount(str, Enum):
    checking = "checking"
    savings = "savings"

    @classmethod
    def from_str(cls, raw: str) -> "BankAccount":
        """
        Parse the account suffix or label from the CSV header.
        """
        try:
            return cls(raw.strip().lower())
        except ValueError:
            raise ValueError(f"Unknown bank account type: {raw}")


Rule = Callable[["Source"], str] | None


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.strip().lower().split())
