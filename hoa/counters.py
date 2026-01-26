from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Dict, Tuple
import json

from hoa import config
from hoa.models import Source, Transaction, TxType


def _normalize_account_for_id(account: str) -> str:
    """
    Convert account path to short form for IDs.

    Examples:
        "assets:venmo" → "venmo"
        "assets:truist:checking" → "truist-checking"
        "assets:truist:savings" → "truist-savings"
        "expenses:technology:cloud" → "expenses-technology-cloud"
    """
    # Remove common prefixes
    account = account.replace("assets:", "")
    account = account.replace("liabilities:", "")

    # Replace colons with dashes for readability
    return account.replace(":", "-")


class CounterManager:
    def __init__(self, cache_file: Path):
        self.cache_file = cache_file
        self.counters: Dict[str, int] = defaultdict(int)
        self._load()

    def _generate_id(
        self,
        type: TxType,
        posted_date: date,
        from_account: str | None = None,
        to_account: str | None = None,
        bank_reference: str | None = None,
    ) -> str:
        """
        Generate appropriate ID based on transaction type.
        """
        year = posted_date.year

        if type == TxType.transfer:
            # Transfer: from:to:year-seq
            from_short = _normalize_account_for_id(from_account)
            to_short = _normalize_account_for_id(to_account)

            key = f"xfer:{from_short}:{to_short}"
            self.counters[key] += 1
            seq = self.counters[key]

            return f"{from_short}:{to_short}:{seq:03d}"

        elif type == "deposit":
            # Deposit: account:dep:year-seq
            acct = _normalize_account_for_id(to_account)
            key = f"{acct}:dep:{year}"
            self.counters[key] += 1
            seq = self.counters[key]

            return f"{key}-{seq:03d}"

        elif type == "check" and bank_reference:
            # Check with bank reference
            acct = _normalize_account_for_id(from_account)
            if bank_reference.startswith("975"):
                # Bank check: use reference number
                check_num = bank_reference[3:]
                return f"{acct}:chk:{year}-{check_num}"
            else:
                # Handwritten check
                return f"{acct}:chk:{year}-{bank_reference}"

        else:
            # Generic: account:type:year-seq
            acct = _normalize_account_for_id(to_account or from_account)
            if from_account and from_account.startswith("external:"):
                print(f"From account: {from_account}")
            if to_account and to_account.startswith("external:"):
                print(f"To account: {to_account}")
            key = f"{acct}:{type}:{year}"
            self.counters[key] += 1
            seq = self.counters[key]

            return f"{key}-{seq:03d}"

    def make_transaction(
        self,
        posted_date: date,
        amount: Decimal,
        type: TxType,
        bank: str,
        from_account: str | None = None,
        to_account: str | None = None,
        reference: str | None = None,
        description: str = "",
        memo: str | None = None,
        source: Source = None,
    ) -> Transaction:
        return Transaction(
            posted_date=posted_date,
            amount=amount,
            bank=bank,
            type=type,
            from_account=from_account,
            to_account=to_account,
            reference=reference,
            description=description,
            memo=memo,
            source=source,
        )

    def save(self) -> None:
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_file, "w") as f:
            json.dump(dict(self.counters), f, indent=2, sort_keys=True)

    def _load(self) -> None:
        if self.cache_file.exists():
            with open(self.cache_file) as f:
                self.counters.update(json.load(f))
