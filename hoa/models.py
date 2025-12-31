from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from hoa import config
from typing import Callable, Optional
import hashlib


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
class SourceTransaction:
    account: BankAccount
    posted_date: date
    type: TxType
    serial: Optional[str]
    description: str
    merchant: Optional[str]
    amount: Decimal

    def sha1(self) -> str:
        parts = [
            config.BANK_CODE,
            self.account.value if self.account else "",
            self.posted_date.isoformat(),
            self.type.value if self.type else "",
            self.serial or "",
            self.description or "",
            self.merchant or "",
            f"{self.amount:.2f}",
        ]
        s = "|".join(parts)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()


Rule = Callable[["SourceTransaction"], Optional[str]]
