"""
Annotation system for enriching transactions with additional information.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Protocol
from abc import ABC, abstractmethod
import re
import yaml

from hoa.models import Transaction, TxType, Posting, Invoice


@dataclass
class Annotation:
    """
    Represents one bank transaction, which may include multiple checks in the case of a deposit,
    or, multiple accounts in the case of a categorization rule.
    """

    reference: str
    total: Decimal
    postings: List[Posting]
    description: str = ""
    memo: str = ""

    def matches(self, txn: Transaction) -> bool:
        return txn.reference == self.reference and self.total == txn.amount

    def apply(self, txn: Transaction) -> Transaction:
        """Apply deposit annotation to transaction"""
        return txn.with_updates(
            description=self.description, memo=self.memo, annotation=self
        )

    @classmethod
    def load(cls, yaml_file: Path) -> dict[str, list]:
        """Load all annotation types from a single YAML file"""

        with yaml_file.open() as f:
            data = yaml.safe_load(f)

        results = {}

        # Dispatch based on top-level key
        if "deposits" in data:
            results["deposits"] = [
                cls._load_deposit(entry) for entry in data["deposits"]
            ]

        return results

    @classmethod
    def _load_deposit(entry: dict) -> Annotation:
        checks = []
        for c in entry["checks"]:
            invoice = Invoice(c["invoice"])
            checks.append(
                Posting(
                    account=f"assets:receivables:lot{invoice.lot}",
                    amount=-Decimal(str(c["amount"])),
                    lot=invoice.lot,
                    invoice=invoice,
                    reference=str(c["check_number"]) if c.get("check_number") else None,
                )
            )

        expected_total = Decimal(str(entry["amount"])) if "amount" in entry else None

        return Annotation(
            deposit_id=entry["id"],
            checks=checks,
            expected_total=expected_total,
        )
