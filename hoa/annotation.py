"""
Annotation system for enriching transactions with additional information.
"""

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
class CheckDetail:
    """Details about a single check within a deposit"""

    check_number: str | None
    payer_name: str
    amount: Decimal
    lot: int | None
    invoice: Invoice | None


# =============================================================================
# Base Annotation Interface
# =============================================================================


class TransactionAnnotation(ABC):
    """Base class for all transaction annotations"""

    @abstractmethod
    def matches(self, txn: Transaction) -> bool:
        """Check if this annotation applies to the transaction"""
        pass

    @abstractmethod
    def apply(self, txn: Transaction) -> Transaction:
        """Apply the annotation to the transaction"""
        pass

    @abstractmethod
    def get_postings(self, txn: Transaction) -> list[Posting] | None:
        """Generate postings for this annotation, if applicable"""
        pass


# =============================================================================
# Deposit Annotations
# =============================================================================


@dataclass
class DepositAnnotation(TransactionAnnotation):
    """Represents one bank deposit transaction, which may include multiple checks"""

    effective_date: date  # Date check was received/written
    checks: list[CheckDetail]
    posted_date: date | None = None  # Optional: actual bank posting date

    @property
    def matching_date(self) -> date:
        """The date to use for matching against bank transactions"""
        return self.posted_date if self.posted_date else self.effective_date

    @property
    def total_amount(self) -> Decimal:
        return sum(c.amount for c in self.checks)

    @property
    def description(self) -> str:
        if len(self.checks) == 1:
            return self.checks[0].payer_name
        else:
            names = ", ".join(c.payer_name for c in self.checks[:2])
            if len(self.checks) > 2:
                names += f", +{len(self.checks) - 2} more"
            return f"Multiple deposits: {names}"

    def matches(self, txn: Transaction) -> bool:
        """Deposits don't use pattern matching - they're matched separately"""
        return False

    def apply(self, txn: Transaction) -> Transaction:
        """Apply deposit annotation to transaction"""
        return txn.with_updates(description=self.description, annotation=self)

    def get_postings(self, txn: Transaction) -> list[Posting]:
        """Generate postings for this deposit"""
        postings = []
        for check in self.checks:
            postings.append(
                Posting(
                    account=f"income:dues:{txn.posted_date.year}",
                    amount=-check.amount,
                    lot=check.lot,
                    invoice=check.invoice,
                    reference=check.check_number,
                )
            )
        return postings

    @classmethod
    def load_from_data(cls, data: dict) -> List["DepositAnnotation"]:
        """Load deposit annotations from parsed YAML data"""
        deposits = []

        for entry in data.get("deposits", []):
            checks = []
            for check in entry["checks"]:
                checks.append(
                    CheckDetail(
                        check_number=check.get("check"),
                        payer_name=check["name"],
                        amount=Decimal(check["amount"]),
                        lot=check.get("lot"),
                        invoice=Invoice.from_str(check.get("invoice")),
                    )
                )

            deposits.append(
                cls(
                    effective_date=entry["date"],  # YAML still uses 'date' key
                    checks=checks,
                    posted_date=entry.get("posted_date"),
                )
            )

        return deposits


# =============================================================================
# Categorization Rules
# =============================================================================


@dataclass
class CategorizationRule(TransactionAnnotation):
    """A rule for categorizing transactions based on pattern matching"""

    pattern: str
    account: str | None = None  # Simple account assignment
    description: str | None = None
    memo: str | None = None  # Optional memo field
    amount: Decimal | None = None
    reference: str | None = None  # Match on check number or reference
    postings: list[dict] | None = None  # For complex multi-posting rules

    def __post_init__(self):
        # Compile the regex pattern
        self.regex = re.compile(self.pattern) if self.pattern else None

    def matches(self, txn: Transaction) -> bool:
        """Check if transaction matches this rule"""

        # Check description pattern
        if self.regex and not self.regex.search(txn.description):
            return False

        # Check amount if specified
        if self.amount is not None:
            if txn.amount != abs(self.amount):
                return False

        # Check reference if specified
        if self.reference is not None:
            if txn.reference != self.reference:
                return False

        return True

    def apply(self, txn: Transaction) -> Transaction:
        """Apply rule actions to transaction"""
        updates = {}

        # Update description if specified
        if self.description:
            updates["description"] = self.description

        # Update memo if specified
        if self.memo:
            updates["memo"] = self.memo

        # If we have custom postings, use those
        if self.postings:
            updates["annotation"] = self
        # Otherwise, simple account assignment
        elif self.account:
            if txn.from_account and not txn.to_account:
                updates["to_account"] = self.account
            elif txn.to_account and not txn.from_account:
                updates["from_account"] = self.account

        return txn.with_updates(**updates) if updates else txn

    def get_postings(self, txn: Transaction) -> list[Posting] | None:
        """Generate postings for this rule, if specified"""
        if not self.postings:
            return None

        postings = []
        for posting_data in self.postings:
            postings.append(
                Posting(
                    account=posting_data["account"],
                    amount=Decimal(posting_data["amount"]),
                    lot=posting_data.get("lot"),
                    invoice=Invoice.from_str(posting_data.get("invoice")),
                    reference=posting_data.get("reference"),
                )
            )
        return postings

    @classmethod
    def load_from_data(cls, data: dict) -> List["CategorizationRule"]:
        """Load categorization rules from parsed YAML data"""
        rules = []

        for rule_data in data.get("rules", []):
            reference = rule_data.get("reference")
            rules.append(
                cls(
                    pattern=rule_data.get("pattern"),
                    account=rule_data.get("account"),
                    description=rule_data.get("description"),
                    memo=rule_data.get("memo"),
                    amount=(
                        Decimal(rule_data["amount"]) if "amount" in rule_data else None
                    ),
                    reference=str(reference) if reference is not None else None,
                    postings=rule_data.get("postings"),
                )
            )

        return rules


# =============================================================================
# Unified Annotation Loader
# =============================================================================


def load_annotations(
    annotations_path: Path,
) -> tuple[List[CategorizationRule], List[DepositAnnotation]]:
    """
    Load all annotations from YAML files in a directory.

    Scans all .yaml files and dispatches to appropriate loaders based on
    the root keys present in each file ('rules' or 'deposits').

    Returns:
        (rules, deposits) - Lists of loaded annotations
    """
    all_rules = []
    all_deposits = []

    if not annotations_path.is_dir():
        return (all_rules, all_deposits)

    for yaml_file in sorted(annotations_path.glob("*.yaml")):
        with yaml_file.open(encoding="utf-8-sig") as f:
            data = yaml.safe_load(f)

        # Dispatch based on root keys in the YAML
        if "rules" in data:
            all_rules.extend(CategorizationRule.load_from_data(data))

        if "deposits" in data:
            all_deposits.extend(DepositAnnotation.load_from_data(data))

    return (all_rules, all_deposits)


# =============================================================================
# Apply Annotations
# =============================================================================


def apply_categorization_rules(
    transactions: list[Transaction],
    rules: list[CategorizationRule],
    verbose: bool = False,
) -> list[Transaction]:
    """
    Apply categorization rules to transactions (first match wins).
    """
    if not rules:
        return transactions

    categorized = []
    match_stats = {}

    for txn in transactions:
        # Try each rule in order (first match wins)
        for rule in rules:
            if rule.matches(txn):
                if verbose:
                    old_desc = txn.description
                    old_account = txn.to_account or txn.from_account

                txn = rule.apply(txn)
                match_stats[rule.pattern] = match_stats.get(rule.pattern, 0) + 1

                if verbose:
                    new_desc = txn.description
                    new_account = txn.to_account or txn.from_account
                    changes = []
                    if old_desc != new_desc:
                        changes.append(f"desc: '{old_desc}' → '{new_desc}'")
                    if old_account != new_account:
                        changes.append(f"account: {old_account} → {new_account}")
                    if rule.postings:
                        changes.append(f"{len(rule.postings)} custom postings")
                    if changes:
                        print(
                            f"  {txn.posted_date} ${txn.amount}: {', '.join(changes)}"
                        )

                break

        categorized.append(txn)

    if verbose and match_stats:
        print(f"\nRule match statistics:")
        for pattern, count in match_stats.items():
            print(f"  '{pattern}': {count} matches")

    return categorized


def apply_deposit_annotations(
    transactions: List[Transaction],
    deposits: List[DepositAnnotation],
    max_days_after: int = 14,
    verbose: bool = False,
) -> List[Transaction]:
    """
    Apply deposit annotations to transactions.

    Matches deposits to bank transactions using either:
    - The deposit date + max_days_after window, OR
    - An explicit posted_date if specified in the annotation
    """
    if not deposits:
        return transactions

    # Sort both lists
    deposits.sort(key=lambda d: d.matching_date)
    transactions.sort(key=lambda t: t.posted_date)

    # Filter to only deposit transactions to the checking account
    deposit_txns = [
        txn
        for txn in transactions
        if txn.type == TxType.deposit and txn.to_account == "assets:truist:checking"
    ]

    if verbose:
        print(f"\nLoaded {len(deposits)} deposit annotations")
        print(f"Found {len(deposit_txns)} deposit transactions to match")

    # Track which transactions have been matched
    txn_matched = {id(txn): False for txn in deposit_txns}
    deposit_map = {}
    matched_deposits = set()

    # For each deposit annotation, find matching transaction
    for deposit in deposits:
        # Determine matching window
        if deposit.posted_date:
            start_date = deposit.posted_date
            end_date = deposit.posted_date
        else:
            start_date = deposit.effective_date
            end_date = deposit.effective_date + timedelta(days=max_days_after)

        # Look for transactions within the window
        for txn in deposit_txns:
            if txn_matched[id(txn)]:
                continue

            if (
                txn.posted_date >= start_date
                and txn.posted_date <= end_date
                and txn.amount == deposit.total_amount
            ):

                if verbose:
                    if deposit.posted_date:
                        print(
                            f"MATCH: Deposit {deposit.effective_date} (posted {deposit.posted_date}) "
                            f"${deposit.total_amount} → Transaction {txn.posted_date}"
                        )
                    else:
                        print(
                            f"MATCH: Deposit {deposit.effective_date} ${deposit.total_amount} → "
                            f"Transaction {txn.posted_date}"
                        )

                key = (txn.posted_date, txn.amount, id(txn))
                deposit_map[key] = deposit
                matched_deposits.add(id(deposit))
                txn_matched[id(txn)] = True
                break

    # Apply annotations to transactions
    annotated = []
    for txn in transactions:
        if txn.type == TxType.deposit and txn.to_account == "assets:truist:checking":

            key = (txn.posted_date, txn.amount, id(txn))
            if key in deposit_map:
                deposit = deposit_map[key]
                txn = deposit.apply(txn)

        annotated.append(txn)

    # Report unmatched items if verbose
    if verbose:
        matched_count = len(matched_deposits)
        print(f"\nMatched {matched_count} deposits")

        unmatched_deposits = [d for d in deposits if id(d) not in matched_deposits]
        if unmatched_deposits:
            print(
                f"\nWarning: {len(unmatched_deposits)} unmatched deposit annotations:"
            )
            for d in unmatched_deposits:
                posted_str = f" (posted {d.posted_date})" if d.posted_date else ""
                print(
                    f"  {d.effective_date}{posted_str}: ${d.total_amount} - {d.description}"
                )
                for check in d.checks:
                    check_num = (
                        f"#{check.check_number}" if check.check_number else "cash"
                    )
                    invoice_str = (
                        f" (Invoice: {check.invoice})" if check.invoice else ""
                    )
                    print(
                        f"    - Check {check_num} from {check.payer_name} "
                        f"for ${check.amount}{invoice_str}"
                    )

        unmatched_txns = [txn for txn in deposit_txns if not txn_matched[id(txn)]]
        if unmatched_txns:
            print(
                f"\nWarning: {len(unmatched_txns)} bank transactions without annotations:"
            )
            for txn in unmatched_txns:
                print(f"  {txn.posted_date}: ${txn.amount} - {txn.description}")

    return annotated
