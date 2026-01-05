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


@dataclass(frozen=True)
class JournalEntry:
    posted_date: date
    effective_date: date
    type: TxType
    description: str
    memo: str | None
    serial: str | None
    amount: Decimal
    postings: list[Posting]
    transactions: List[Transaction]


TRANSFER_KEYWORDS = (
    "transfer from",
    "transfer to",
)


def is_transfer_desc(desc: str) -> bool:
    d = desc.lower()
    return any(k in d for k in TRANSFER_KEYWORDS)


@dataclass
class TransferReducer:
    """
    Pairs two transactions that represent the two sides
    of an internal account transfer.
    """

    def try_reduce(
        self,
        txns: list[Transaction],
        i: int,
    ) -> Tuple[int, JournalEntry] | None:

        if i + 1 >= len(txns):
            return None

        a = txns[i]
        b = txns[i + 1]

        # --- hard requirements ---
        if a.posted_date != b.posted_date:
            return None

        if abs(a.amount) != abs(b.amount):
            return None

        if a.amount != -b.amount:
            return None

        if a.account == b.account:
            return None

        if not (is_transfer_desc(a.description) or is_transfer_desc(b.description)):
            return None

        # --- determine canonical ordering ---
        debit = a if a.amount < 0 else b
        credit = b if debit is a else a

        amount = abs(debit.amount)

        # --- build journal entry ---
        je = JournalEntry(
            posted_date=a.posted_date,
            effective_date=a.effective_date,
            type=TxType.transfer,
            description=f"Transfer: {credit.account} → {debit.account}",
            memo=None,
            serial=None,
            amount=amount,
            postings=[
                Posting(
                    posting_id=None,
                    journal_id=None,
                    lot=None,
                    invoice=None,
                    account=credit.account,
                    amount=amount,
                ),
                Posting(
                    account=debit.account,
                    amount=-amount,
                ),
            ],
            transactions=[txns[i], txns[i + 1]],
        )

        return (2, je)
