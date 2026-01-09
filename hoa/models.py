from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from hoa import config
from pathlib import Path
from typing import Callable, List, Self, Tuple

from dataclasses import dataclass, replace, asdict
from datetime import date
from decimal import Decimal
from typing import Optional, Iterable, TextIO, Dict, Any
import json


@dataclass(frozen=True)
class FinancialEvent:
    # Identity / ordering
    event_id: str
    posted_date: date
    amount: Decimal

    # Accounts (may be inferred later)
    from_account: str | None = None
    to_account: str | None = None
    type: str | None = None  # "debit", "credit", "transfer".
    reference: str | None = None  # Venmo ID, check number, ref #

    # Descriptive information
    description: str = ""
    memo: str | None = None

    # Source provenance (never destroyed)
    source_file: str | None = None
    source_line: int | None = None
    source_type: str | None = None  # "DEBIT", "Payment", etc.

    # ---- helpers ----

    def with_updates(self, **changes) -> "FinancialEvent":
        return replace(self, **changes)

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
    def from_dict(cls, data: Dict[str, Any]) -> "FinancialEvent":
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
            source_file=data.get("source_file"),
            source_line=data.get("source_line"),
            source_type=data.get("source_type"),
        )

    @classmethod
    def read_ndjson(cls, stream: TextIO) -> Iterable["FinancialEvent"]:
        for line_no, line in enumerate(stream, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield cls.from_dict(json.loads(line))
            except Exception as e:
                raise RuntimeError(f"Invalid FinancialEvent at line {line_no}: {e}")

    @classmethod
    def write_ndjson(
        cls,
        events: Iterable["FinancialEvent"],
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
    credit = "credit"
    debit = "debit"
    fee = "fee"
    deposit = "deposit"
    check = "check"
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


@dataclass
class Source:
    """
    Provenance information for a Transaction.

    This is a value object: it has no independent identity and
    is always embedded in a Transaction.
    """

    kind: str  # 'bank_csv', 'receipt_yaml', 'manual_yaml'
    bank_code: str | None  # 'truist', etc., or None
    file: str  # path relative to sources/
    line: int | None  # line number or item index within file


Rule = Callable[["Source"], str] | None


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.strip().lower().split())


@dataclass(frozen=True)
class Transaction:
    posted_date: date
    effective_date: date
    type: TxType
    description: str
    memo: str | None
    serial: str | None
    account: str
    amount: Decimal
    line: int  # line number within source file
    ordinal: int = 0  # differentiate same-day entries

    def hash(self) -> str:
        parts = [
            self.account,
            self.posted_date.isoformat(),
            self.type,
            _normalize(self.description),
            self.serial or "",
            str(self.amount),
            str(self.ordinal),
        ]

        data = "\x1f".join(parts)
        return sha256(data.encode("utf-8")).hexdigest()


@dataclass
class Posting:
    posting_id: int = None  # unique per journal entry
    journal_id: int = None  # FK to journal_entry
    account: str = ""
    amount: Decimal = Decimal(0)
    lot: int = None
    invoice: str = None
    reference: str = None

    @classmethod
    def from_annotation_dict(cls, d: dict) -> "Posting":
        return cls(
            account=d["account"],
            amount=Decimal(str(d["amount"])),
            lot=d.get("lot"),
            invoice=d.get("invoice"),
            journal_id=None,
            posting_id=None,
        )
