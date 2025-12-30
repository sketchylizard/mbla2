from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional
import hashlib
from hoa import config


@dataclass(frozen=True)
class SourceTransaction:
    account: str  # checking or savings
    posted_date: date
    type: str
    serial: Optional[str]
    description: str
    merchant: Optional[str]
    amount: Decimal

    def sha1(self) -> str:
        parts = [
            config.BANK_CODE,
            self.account or "",
            self.posted_date.isoformat(),
            self.type or "",
            self.serial or "",
            self.description or "",
            self.merchant or "",
            f"{self.amount:.2f}",
        ]
        s = "|".join(parts)
        return hashlib.sha1(s.encode("utf-8")).hexdigest()
