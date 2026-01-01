from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from hoa import config
from typing import Callable, Optional
from hashlib import sha256


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
    Provenance information for a JournalEntry.

    This is a value object: it has no independent identity and
    is always embedded in a JournalEntry.
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


@dataclass
class JournalEntry:
    posted_date: str  # ISO date
    effective_date: str
    tx_type: str
    description: str
    memo: str | None
    serial: str | None
    account: str
    amount: Decimal

    def hash(self, sequence: int | None = None) -> str:
        parts = [
            self.account,
            self.posted_date,
            self.tx_type,
            _normalize(self.description),
            _normalize(self.serial),
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
