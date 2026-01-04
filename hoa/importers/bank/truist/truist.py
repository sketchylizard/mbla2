from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Self, Tuple, List, Set
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
    JournalEntry,
    Posting,
    Source,
    Transaction,
    TransferReducer,
    TxType,
)

RENAMING_RULES_FILE = "renaming_rules.toml"
POSTING_RULES_FILE = "posting_rules.toml"

PendingAnnotations = AnnotationStore[PendingAnnotation]
ResolvedAnnotations = AnnotationStore[ResolvedAnnotation]


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


def apply_renaming(rules: list[RenamingRule], entry: Transaction) -> Transaction:
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


class ResolvedReducer:
    def __init__(self, resolved: ResolvedAnnotations):
        self._store = resolved

    def try_reduce(
        self,
        items: list[Transaction],
        index: int,
    ) -> Tuple[int, JournalEntry, Source] | None:

        tx = items[index]

        matched = self._store.match(tx)
        if matched == None:
            return None

        journalEntry = JournalEntry(
            posted_date=tx.posted_date,
            effective_date=tx.effective_date,
            type=tx.type,
            description=matched.description,
            memo=matched.memo,
            serial=tx.serial,
            amount=tx.amount,
            postings=[
                *matched.postings,
                Posting(
                    account=tx.account,
                    amount=-sum(p.amount for p in matched.postings),
                ),
            ],
            transactions=[items[index]],
        )

        return (1, journalEntry)


class PendingReducer:
    def __init__(self, resolved: ResolvedAnnotations, pending: PendingAnnotations):
        self._resolved = resolved
        self._store = pending

    def try_reduce(
        self,
        items: list[Transaction],
        index: int,
    ) -> Tuple[int, JournalEntry] | None:

        tx = items[index]

        matched = self._store.match(tx)
        if matched == None:
            return None

        journalEntry = JournalEntry(
            posted_date=tx.posted_date,
            effective_date=tx.effective_date,
            type=tx.type,
            description=matched.description,
            memo=matched.memo,
            serial=tx.serial,
            amount=tx.amount,
            postings=[
                *matched.postings,
                Posting(
                    account=tx.account,
                    amount=-sum(p.amount for p in matched.postings),
                ),
            ],
            transactions=[tx],
        )

        resolvedAnnotation = matched.resolve(tx.hash())
        self._resolved.add(resolvedAnnotation)
        self._store.remove(matched)

        return (1, journalEntry)


class AutoMatchReducer:
    def __init__(self, rules: list[PostingRule]):
        self.rules = rules

    def try_reduce(
        self,
        items: list[Transaction],
        index: int,
    ) -> Tuple[int, JournalEntry, Source] | None:
        tx = items[index]

        for rule in self.rules:
            if not rule.pattern.search(tx.description.lower()):
                continue

            journalEntry = JournalEntry(
                posted_date=tx.posted_date,
                effective_date=tx.effective_date,
                type=tx.type,
                description=tx.description,
                memo=tx.memo,
                serial=tx.serial,
                amount=tx.amount,
                postings=[
                    Posting(account=tx.account, amount=tx.amount),
                    Posting(account=rule.account, amount=-tx.amount),
                ],
                transactions=[tx],
            )

            return (1, journalEntry)

        return None


class FallbackReducer:
    def try_reduce(
        self,
        items: list[Transaction],
        index: int,
    ) -> Tuple[int, JournalEntry, Source] | None:
        tx = items[index]

        counter_account = "expenses:unknown" if tx.amount < 0 else "income:unknown"

        postings = [
            Posting(account=tx.account, amount=tx.amount),
            Posting(account=counter_account, amount=-tx.amount),
        ]

        entry = JournalEntry(
            posted_date=tx.posted_date,
            effective_date=tx.effective_date,
            type=tx.type,
            description=tx.description,
            memo=tx.memo,
            serial=tx.serial,
            amount=tx.amount,
            postings=postings,
            transactions=[tx],
        )

        resolved = ResolvedAnnotation(
            hash=tx.hash(),
            description=tx.description,
            memo=tx.memo,
            postings=postings,
        )

        return (1, entry)


def read_lines_from_csv(
    file_path: Path,
    previous_hashes: Set[str],
) -> List[Transaction]:
    current_account: str | None = None

    transactions: List[Transaction] = []

    with file_path.open(newline="", encoding="utf-8-sig") as f:
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
                    transaction_date,
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
                effective_date_iso = datetime.strptime(
                    transaction_date, "%m/%d/%Y"
                ).date()

                entry = apply_renaming(
                    renaming_rules,
                    Transaction(
                        posted_date=posted_date_iso,
                        effective_date=posted_date_iso,
                        type=type.lower(),
                        description=description,
                        memo=None,
                        serial=int(serial.strip()) if serial else None,
                        account=current_account,
                        amount=amount,
                        line=line_no,
                    ),
                )

                h = entry.hash()
                if h in previous_hashes:
                    continue  # skip duplicate

                previous_hashes.add(h)
                transactions.append(entry)

            except Exception as e:
                print(f"Error parsing line {line_no} in {file_path.name}: {e}")
                raise
    return transactions


def import_file(
    absPath: Path,
    relPath: Path,
    journal: Journal,
    pending: PendingAnnotations,
    previous_hashes: Set[str],
) -> None:
    bank_code = "truist"

    annotations_path = absPath.with_name(f"{absPath.stem}_annotations.toml")
    resolved = ResolvedAnnotations(annotations_path, "resolved")
    resolved.load()

    transactions = read_lines_from_csv(absPath, previous_hashes)
    transaction_count = len(transactions)

    # Sort transactions by date, amount, type, description
    transactions.sort(key=lambda tx: tx.sort_key())

    reducers = [
        TransferReducer(),
        ResolvedReducer(resolved),
        PendingReducer(resolved, pending),
        AutoMatchReducer(posting_rules),
        FallbackReducer(),
    ]

    created_entries = 0

    source = Source(
        kind="bank_csv",
        bank_code=bank_code,
        file=str(relPath),
        line=None,
    )

    i = 0
    while i < len(transactions):
        skip_count = 0
        for reducer in reducers:
            result = reducer.try_reduce(transactions, i)
            if result is not None:
                (skip_count, combined_entry) = result
                journal_id = journal.add_entry(combined_entry, source)
                if journal_id is not None:
                    created_entries += 1
                    break

        i += skip_count

    print(
        f"  {relPath.stem:10s} : Parsed: {transaction_count} transactions, {created_entries} new journal entries."
    )
    resolved.save()


def import_files(absPath: Path, journal: Journal) -> None:
    rel_path = absPath.relative_to(config.SOURCES)
    print(f"Importing Truist files: {rel_path}")

    previous_hashes = journal.get_hashes()

    pending = PendingAnnotations(absPath / "pending.toml", "pending")
    pending.load()

    for file_path in sorted(absPath.glob("*.csv")):
        if file_path.is_file():
            abs_path = file_path.resolve()
            rel_path = abs_path.relative_to(config.SOURCES)
            import_file(abs_path, rel_path, journal, pending, previous_hashes)

    pending.save()
    print("Truist import completed.")
