from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from hoa import config
from pathlib import Path
from typing import Callable, List, Self, Tuple
import toml


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
