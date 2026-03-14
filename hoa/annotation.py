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
    postings: List[Posting]
    total: Decimal | None = None
    description: str | None = None
    memo: str | None = None

    def matches(self, txn: Transaction) -> bool:
        if txn.reference != self.reference:
            return False
        if self.total is None or self.total == txn.amount:
            return True
        return False

    def apply(self, txn: Transaction) -> Transaction:
        """Apply deposit annotation to transaction"""
        return txn.with_updates(annotation=self)

    @classmethod
    def load(cls, yaml_file: Path) -> List[Annotation]:
        """Load all annotation types from a single YAML file"""

        with yaml_file.open() as f:
            data = yaml.safe_load(f)

        results = []

        # Dispatch based on top-level key
        if "deposits" in data:
            results.extend([cls._load_deposit(entry) for entry in data["deposits"]])
        elif "checks" in data:
            results.extend([cls._load_check(entry) for entry in data["checks"]])

        return results

    @classmethod
    def load_all(cls, yaml_dir: Path) -> List[Annotation]:
        if not yaml_dir.is_dir():
            raise ValueError(f"Expected a directory of YAML files, got {yaml_dir}")

        annotations = []

        for file in yaml_dir.glob("*.yaml"):
            if file.is_file():
                annotations.extend(cls.load(file))

        return annotations

    @classmethod
    def _load_deposit(cls, entry: dict) -> Annotation:
        checks = []
        calculated_total = 0
        names = []

        for c in entry["checks"]:
            amount = Decimal(str(c["amount"]))
            calculated_total += amount
            names.append(c["name"])

            if "account" in c:
                # Explicit account override — no invoice
                account = c["account"]
                invoice = None
            else:
                invoice = Invoice(c["invoice"])
                account = f"assets:receivables:lot{invoice.lot:02}"

            checks.append(
                Posting(
                    account=account,
                    amount=-amount,
                    invoice=invoice,
                    reference=str(c["check_number"]) if c.get("check_number") else None,
                )
            )

        if len(names) == 1:
            description = f"Deposit from {names[0]}"
        else:
            description = ", ".join(name for name in names[:2])
            if len(names) > 2:
                description += f", +{len(names) - 2} more"
            description = f"Multiple deposits: {description}"

        expected_total = (
            Decimal(str(entry["amount"])) if "amount" in entry else calculated_total
        )

        return Annotation(
            reference=entry["id"],
            postings=checks,
            total=expected_total,
            description=description,
        )

    @classmethod
    def _load_check(cls, entry: dict) -> Annotation:
        if "postings" in entry:
            postings = []
            for p in entry["postings"]:
                invoice = Invoice(str(p["invoice"])) if p.get("invoice") else None
                # Derive full account name from invoice if account is bare 'assets:receivables'
                account = p["account"]
                if account == "assets:receivables" and invoice:
                    account = f"assets:receivables:lot{invoice.lot:02}"
                postings.append(
                    Posting(
                        account=account,
                        amount=Decimal(str(p["amount"])),
                        invoice=invoice,
                    )
                )
            return Annotation(
                reference=str(entry["id"]),
                postings=postings,
                total=Decimal(str(entry["amount"])) if "amount" in entry else None,
                description=entry.get("description"),
                memo=entry.get("memo"),
            )

        # Single account case
        return Annotation(
            reference=str(entry["id"]),
            postings=[Posting(account=entry["account"])],
            total=None,
            description=entry.get("description"),
            memo=entry.get("memo"),
        )


def apply_annotations(
    events: List[Transaction], annotation_root: Path
) -> List[Transaction]:
    """Apply annotations to the given events, returning a new list of events with annotations applied."""
    annotations = Annotation.load_all(annotation_root)

    annotations.sort(key=lambda a: a.reference)

    events.sort(key=lambda e: (e.reference or "~~~~~~~~~~"))

    for annotation in annotations:
        found = False
        for txn_index, event in enumerate(events):
            if annotation.matches(event):
                events[txn_index] = annotation.apply(event)
                # annotation should only match one transaction
                found = True
                print(f"Applied annotation {annotation.reference} to event")
                break

        if not found:
            # If we didn't find a match, we can log a warning or raise an error
            print(
                f"Warning: No match found for annotation {annotation.reference} in events",
                file=sys.stderr,
            )

    return events
