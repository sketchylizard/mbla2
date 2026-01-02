from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from hoa import config
from pathlib import Path
from typing import Callable, Optional, Self
import toml


class TxType(str, Enum):
    credit = "credit"
    debit = "debit"
    fee = "fee"
    deposit = "deposit"
    check = "check"

    @classmethod
    def from_csv(cls, raw: str) -> "TxType":
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
    def from_csv(cls, raw: str) -> "BankAccount":
        """
        Parse the account suffix or label from the CSV header.
        """
        try:
            return cls(raw.strip().lower())
        except ValueError:
            raise ValueError(f"Unknown bank account type: {raw}")


@dataclass(frozen=True)
class Source:
    """
    Provenance information for a Transaction.

    This is a value object: it has no independent identity and
    is always embedded in a Transaction.
    """

    kind: str  # 'bank_csv', 'receipt_yaml', 'manual_yaml'
    file: str  # path relative to sources/
    line: Optional[int]  # line number or item index within file
    bank_code: Optional[str]  # 'truist', etc., or None


Rule = Callable[["Source"], Optional[str]]


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.strip().lower().split())


@dataclass(frozen=True)
class Transaction:
    posted_date: date  # ISO date
    effective_date: date
    type: TxType
    description: str
    memo: str | None
    serial: int | None
    account: str
    amount: Decimal

    def hash(self, sequence: int | None = None) -> str:
        parts = [
            self.account,
            self.posted_date.isoformat(),
            self.type,
            _normalize(self.description),
            str(self.serial),
            str(self.amount),
        ]

        if sequence is not None:
            parts.append(str(sequence))

        data = "\x1f".join(parts)
        return sha256(data.encode("utf-8")).hexdigest()


@dataclass
class Posting:
    posting_id: int  # unique per journal entry
    journal_id: int  # FK to journal_entry
    account: str
    amount: Decimal
    lot: int
    invoice: str

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
