from dataclasses import dataclass, replace
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Self, Tuple, List
from collections import defaultdict
import csv
import re

from hoa import config
from hoa.annotation_store import (
    AnnotationStore,
)
from hoa.journal import (
    Journal,
    JournalEntry,
)

from hoa.models import (
    Posting,
    Source,
    Transaction,
    TxType,
)

# Directory containing truist.py
here = Path(__file__).resolve().parent

RENAMING_RULES_FILE = "renaming.rules"
POSTING_RULES_FILE = "posting.rules"


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
        rules: list[Self] = []
        if not rules_path.exists():
            raise FileNotFoundError(f"Rules file not found: {rules_path}")

        lines = rules_path.read_text(encoding="utf-8").splitlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("pattern:"):
                pattern = line[len("pattern:") :].strip()
                replacement = None
                amount = None
                types = None
                i += 1
                # Parse following lines for replacement and optional fields
                while i < len(lines):
                    next_line = lines[i].strip()
                    if next_line.startswith("replacement:"):
                        replacement = next_line[len("replacement:") :].strip()
                    elif next_line.startswith("amount:"):
                        amount = Decimal(next_line[len("amount:") :].strip())
                    elif next_line.startswith("type:"):
                        raw_types = next_line[len("type:") :].strip()
                        # Accept comma-separated or bracketed lists
                        if raw_types.startswith("[") and raw_types.endswith("]"):
                            raw_types = raw_types[1:-1]
                        types = frozenset(
                            TxType[t.strip().lower()]
                            for t in raw_types.split(",")
                            if t.strip()
                        )
                    elif next_line == "" or next_line.startswith("pattern:"):
                        break
                    i += 1
                if pattern and replacement is not None:
                    try:
                        rule = cls(
                            pattern=re.compile(pattern),
                            replacement=replacement,
                            amount=amount,
                            type=types,
                        )
                        rules.append(rule)

                    except re.error as e:
                        print(
                            f"Error compiling regex pattern '{pattern}': {e} at line {i}"
                        )
            i += 1

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

        rules: list[Self] = []
        lines = rules_path.read_text(encoding="utf-8").splitlines()
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith("pattern:"):
                pattern = line[len("pattern:") :].strip()
                replacement = None
                i += 1
                # Parse following lines for replacement
                while i < len(lines):
                    next_line = lines[i].strip()
                    if next_line.startswith("replacement:"):
                        replacement = next_line[len("replacement:") :].strip()
                    i += 1
                if pattern and replacement is not None:
                    rules.append(
                        cls(
                            pattern=re.compile(pattern),
                            replacement=replacement,
                        )
                    )
            i += 1

        return rules


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


TRANSFER_KEYWORDS = (
    "transfer from",
    "transfer to",
)


@dataclass
class TransferReducer:
    """
    Pairs two transactions that represent the two sides
    of an internal account transfer.
    """

    def _is_transfer_desc(self, desc: str) -> bool:
        d = desc.lower()
        return any(k in d for k in TRANSFER_KEYWORDS)

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

        if not (
            self._is_transfer_desc(a.description)
            or self._is_transfer_desc(b.description)
        ):
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


class ReconciledReducer:
    def __init__(self, reconciled: AnnotationStore):
        self._store = reconciled

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
            transactions=[tx],
        )

        return (1, journalEntry)


class PendingReducer:
    def __init__(self, reconciled: AnnotationStore, pending: AnnotationStore):
        self._reconciled = reconciled
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

        reconciledAnnotation = matched.resolve(tx.hash())
        self._reconciled.add(reconciledAnnotation)
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

        return (1, entry)


def read_lines_from_csv(
    file_path: Path,
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
                        serial=serial.strip() if serial else None,
                        account=current_account,
                        amount=amount,
                        line=line_no,
                    ),
                )

                transactions.append(entry)

            except Exception as e:
                print(f"Error parsing line {line_no} in {file_path.name}: {e}")
                raise

    # Check for transactions that look like duplicates. We assume no duplicates within a given file, so any transaction
    # that appears to be a duplicate needs a unique number. This can happen when several deposits are made on the same
    # day, with the same amount and description. For instance mobile deposits of dues checks. Sort by date, amount,
    # type, description. Any adjacent transactions that match on those fields get a unique ordinal.
    transactions.sort(
        key=lambda tx: (tx.account, tx.posted_date, tx.amount, tx.type, tx.description)
    )

    last_key: Tuple[str, date, Decimal, str, str] | None = None
    ordinal_counter = defaultdict(int)
    for tx in transactions:
        key = (tx.account, tx.posted_date, tx.amount, tx.type, tx.description)
        if key == last_key:
            ordinal_counter[key] += 1
            replace(tx, ordinal=ordinal_counter[key])
        else:
            ordinal_counter[key] = 0
            last_key = key

    return transactions


def import_file(
    absPath: Path,
    relPath: Path,
    journal: Journal,
    pending: AnnotationStore,
) -> None:
    bank_code = "truist"

    annotations_path = absPath.with_name(f"{absPath.stem}.ann")
    reconciled = AnnotationStore(annotations_path)
    reconciled.load()

    transactions = read_lines_from_csv(absPath)
    transaction_count = len(transactions)

    # Sort transactions by date, amount, type, description. Ignore account because we're trying to find matching
    # transactions from different accounts
    transactions.sort(
        key=lambda tx: (
            tx.posted_date,
            abs(tx.amount),
            tx.type,
            tx.description,
        )
    )

    reducers = [
        TransferReducer(),
        ReconciledReducer(reconciled),
        PendingReducer(reconciled, pending),
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
    reconciled.save()


def import_files(absPath: Path, journal: Journal) -> None:
    rel_path = absPath.relative_to(config.SOURCES)
    print(f"Importing Truist files: {rel_path}")

    previous_hashes = journal.get_hashes()

    pending = AnnotationStore(absPath / "pending.ann")
    pending.load()

    for file_path in sorted(absPath.glob("*.csv")):
        if file_path.is_file():
            abs_path = file_path.resolve()
            rel_path = abs_path.relative_to(config.SOURCES)
            import_file(abs_path, rel_path, journal, pending)

    pending.save()
    print("Truist import completed.")
