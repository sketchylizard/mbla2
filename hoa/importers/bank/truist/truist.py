from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Self
import csv
import re
import toml

from hoa import config
from hoa.annotation_store import (
    AnnotationStore,
    ResolvedAnnotation,
    PendingAnnotation,
)
from hoa.journal import Journal
from hoa.models import (
    Transaction,
    Posting,
    Source,
    TxType,
)

RENAMING_RULES_FILE = "renaming_rules.toml"
POSTING_RULES_FILE = "posting_rules.toml"


@dataclass(frozen=True)
class RenamingRule:
    pattern: re.Pattern[str]
    replacement: str
    amount: Decimal | None = None
    type: frozenset[TxType] | None = None

    def matches(self, entry: Transaction) -> bool:
        if self.type is not None and entry.type not in self.type:
            return False
        if self.amount is not None and abs(entry.amount) != abs(self.amount):
            return False
        if not self.pattern.search(entry.description):
            return False
        return True

    @classmethod
    def load(cls, rules_path: Path) -> list[Self]:
        if not rules_path.exists():
            raise FileNotFoundError(f"Rules file not found: {rules_path}")

        data = toml.loads(rules_path.read_text(encoding="utf-8"))

        rules: list[RenamingRule] = []
        for rule in data["rule"]:
            raw_type = rule.get("type")

            if raw_type is None:
                types = None
            elif isinstance(raw_type, list):
                types = frozenset(TxType[t.lower()] for t in raw_type)
            else:
                types = frozenset({TxType[raw_type.lower()]})

            rules.append(
                cls(
                    re.compile(rule["pattern"]),
                    rule["replacement"],
                    Decimal(rule["amount"]) if "amount" in rule else None,
                    types,
                )
            )

        return rules


@dataclass(frozen=True)
class PostingRule:
    pattern: re.Pattern[str]
    account: str
    lot: int | None = None
    invoice: str | None = None

    @classmethod
    def load(cls, rules_path: Path) -> list[Self]:
        if not rules_path.exists():
            raise FileNotFoundError(f"Rules file not found: {rules_path}")

        data = toml.loads(rules_path.read_text(encoding="utf-8"))

        rules: list[PostingRule] = []
        for rule in data["rule"]:
            rules.append(
                cls(
                    re.compile(rule["pattern"]),
                    rule["account"],
                )
            )

        return rules


# Directory containing truist.py
here = Path(__file__).resolve().parent

renaming_rules = RenamingRule.load(here / RENAMING_RULES_FILE)
posting_rules = PostingRule.load(here / POSTING_RULES_FILE)


def apply_renaming(entry: Transaction, rules: list[RenamingRule]) -> Transaction:
    """
    Returns a new Transaction with the description updated according to renaming rules.
    Falls back to normalizing the description if no rule matches.
    """
    for rule in rules:
        if rule.matches(entry):
            new_desc = rule.pattern.sub(rule.replacement, entry.description)
            return replace(entry, description=new_desc)

    # Fallback: capitalize first letter, lowercase the rest
    normalized_desc = entry.description.capitalize()
    return replace(entry, description=normalized_desc)


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def parse_amount(value: str) -> Decimal:
    """Truist amounts look like "$1,234.56"."""
    value = value.replace("$", "").replace(",", "").strip()
    # if the value is surrounded by parentheses, it's negative
    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1].strip()
    return Decimal(value)


def classify_postings(entry: Transaction) -> list[Posting]:
    for rule in posting_rules:
        if rule.pattern.search(entry.description.lower()):
            return [
                Posting(
                    posting_id=None,  # filled later
                    journal_id=None,  # filled later
                    account=rule.account,
                    amount=-entry.amount,  # sign convention
                    lot=rule.lot,
                    invoice=rule.invoice,
                )
            ]

    return []


def import_file(
    absPath: Path,
    relPath: Path,
    journal: Journal,
    annotations: AnnotationStore,
) -> None:
    print(f"Importing Truist file: {relPath}")

    bank_code = "truist"

    annotations_path = absPath.with_name(f"{absPath.stem}_annotations.toml")
    annotations.load_resolved(annotations_path)

    current_account: str | None = None
    seen_hashes: dict[str, int] = {}

    with absPath.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)

        for line_no, row in enumerate(reader, start=1):
            if not row:
                continue

            first = row[0].strip()

            # Detect account headers
            if first.startswith("Transactions for "):
                current_account = first.replace("Transactions for ", "").strip()
                continue

            # Skip CSV header rows
            if first == "Posted Date":
                continue

            if current_account is None:
                continue  # safety check

            try:
                (
                    posted_date,
                    _transaction_date,
                    type,
                    serial,
                    description,
                    _merchant,
                    _category,
                    _subcategory,
                    amount_str,
                    _balance,
                ) = row

                amount = parse_amount(amount_str)

                posted_date_iso = datetime.strptime(posted_date, "%m/%d/%Y").date()

                entry = Transaction(
                    posted_date=posted_date_iso,
                    effective_date=posted_date_iso,
                    type=type.lower(),
                    description=description,
                    memo=None,
                    serial=int(serial.strip()) if serial else None,
                    account=current_account,
                    amount=amount,
                )

                entry = apply_renaming(entry, renaming_rules)

                # Compute semantic hash, handle same-file collisions
                sequence = None
                h = entry.hash()
                if h in seen_hashes:
                    seq = 0
                    while True:
                        h = entry.hash(seq)
                        if h not in seen_hashes:
                            sequence = seq
                            break
                        seq += 1

                seen_hashes[h] = 1

                source = Source(
                    "bank_csv",
                    str(relPath),
                    line_no,
                    "truist",
                )

                # Check for resolved annotation
                resolved = annotations.match(entry)
                if resolved:
                    entry = replace(
                        entry, description=resolved.description, memo=resolved.memo
                    )

                # Attempt to insert; None if duplicate
                journal_id = journal.add_entry(entry, source, h)
                if journal_id is None:
                    print(
                        f"Duplicate detected (skipping): "
                        f"{posted_date} {description} {amount} {h}"
                    )
                    continue

                # Add bank-side posting
                posting1 = Posting(
                    None,
                    journal_id=journal_id,
                    account=f"assets:{current_account}",
                    amount=amount,
                    lot=None,
                    invoice=None,
                )

                posting_id1 = journal.add_posting(journal_id, posting1)

                postings = resolved.postings if resolved else classify_postings(entry)

                if not postings:
                    # We did not match any postings, create a balancing posting to "expenses:uncategorized" or "income:uncategorized"
                    uncategorized_account = (
                        "expenses:uncategorized"
                        if amount < Decimal("0.00")
                        else "income:uncategorized"
                    )
                    postings = [
                        Posting(
                            posting_id=None,
                            journal_id=None,
                            account=uncategorized_account,
                            amount=-amount,
                            lot=None,
                            invoice=None,
                        )
                    ]
                    # Add this to the resolved annotations for next time
                    new_resolved = ResolvedAnnotation(
                        hash=entry.hash(),
                        description=entry.description,
                        memo=entry.memo,
                        postings=postings,
                    )
                    annotations.add_resolved(new_resolved)

                total = sum(p.amount for p in postings)
                if total + amount != Decimal("0.00"):
                    raise ValueError(
                        f"Postings total {total} does not offset entry amount {amount}"
                    )

                for p in postings:
                    posting = replace(
                        p,
                        posting_id=None,
                        journal_id=journal_id,
                    )
                    posting_id = journal.add_posting(journal_id, posting)

            except Exception as e:
                print(f"Error parsing line {line_no} in {relPath.name}: {e}")
                raise

        annotations.save_resolved()


def import_files(absPath: Path, journal: Journal) -> None:
    rel_path = absPath.relative_to(config.SOURCES)
    print(f"Importing Truist files: {rel_path}")

    annotations = AnnotationStore.load(absPath / "pending.toml")

    for file_path in absPath.glob("*.csv"):
        if file_path.is_file():
            abs_path = file_path.resolve()
            rel_path = abs_path.relative_to(config.SOURCES)
            import_file(abs_path, rel_path, journal, annotations)

    annotations.save_pending()
    print("Truist import completed.")
