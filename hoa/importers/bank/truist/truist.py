from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable
import csv
import re
import tomllib

from hoa import config
from hoa.journal import Journal
from hoa.models import JournalEntry, Posting, Source, TxType

RENAMING_RULES_FILE = "renaming_rules.toml"
POSTING_RULES_FILE = "posting_rules.toml"


@dataclass(frozen=True)
class RenamingRule:
    pattern: re.Pattern[str]
    replacement: str
    amount: Decimal | None = None
    type: frozenset[TxType] | None = None

    def matches(self, entry: JournalEntry) -> bool:
        if self.type is not None and entry.type not in self.type:
            print(
                f"({entry.description}, {entry.type}, {entry.amount}) doesn't match {self.type}"
            )
            return False
        if self.amount is not None and abs(entry.amount) != abs(self.amount):
            print(
                f"({entry.description}, {entry.type}, {entry.amount}) doesn't match {self.amount}"
            )
            return False
        if not self.pattern.search(entry.description):
            print(
                f"({entry.description}, {entry.type}, {entry.amount}) doesn't match {str(self.pattern)}"
            )
            return False
        return True


@dataclass(frozen=True)
class PostingRule:
    pattern: re.Pattern[str]
    account: str
    lot: int | None = None
    invoice: str | None = None


def load_renaming_rules() -> list[RenamingRule]:
    # Directory containing truist.py
    here = Path(__file__).resolve().parent

    rules_path = here / RENAMING_RULES_FILE

    if not rules_path.exists():
        raise FileNotFoundError(f"Rules file not found: {rules_path}")

    data = tomllib.loads(rules_path.read_text(encoding="utf-8"))

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
            RenamingRule(
                re.compile(rule["pattern"]),
                rule["replacement"],
                Decimal(rule["amount"]) if "amount" in rule else None,
                types,
            )
        )

    return rules


def load_posting_rules() -> list[PostingRule]:
    # Directory containing truist.py
    here = Path(__file__).resolve().parent

    rules_path = here / POSTING_RULES_FILE

    if not rules_path.exists():
        raise FileNotFoundError(f"Rules file not found: {rules_path}")

    data = tomllib.loads(rules_path.read_text(encoding="utf-8"))

    rules: list[PostingRule] = []
    for rule in data["rule"]:
        rules.append(
            PostingRule(
                re.compile(rule["pattern"]),
                rule["account"],
            )
        )

    return rules


renaming_rules: list[RenamingRule] = load_renaming_rules()
posting_rules: list[PostingRule] = load_posting_rules()


def apply_renaming(entry: JournalEntry, rules: list[RenamingRule]) -> JournalEntry:
    """
    Returns a new JournalEntry with the description updated according to renaming rules.
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


def classify_postings(entry: JournalEntry) -> list[Posting]:
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


def import_file(absPath: Path, relPath: Path, journal: Journal) -> None:
    print(f"Importing Truist file: {relPath}")

    bank_code = "truist"

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
                    raw_desc,
                    _merchant,
                    _category,
                    _subcategory,
                    amount_str,
                    _balance,
                ) = row

                amount = parse_amount(amount_str)

                posted_date_iso = (
                    datetime.strptime(posted_date, "%m/%d/%Y").date().isoformat()
                )

                entry = JournalEntry(
                    posted_date=posted_date_iso,
                    effective_date=posted_date_iso,
                    type=type.lower(),
                    description=raw_desc,
                    memo=None,
                    serial=serial.strip() if serial else None,
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

                # Attempt to insert; None if duplicate
                journal_id = journal.add_entry(entry, source, h)
                if journal_id is None:
                    print(
                        f"Duplicate detected (skipping): "
                        f"{posted_date} {description} {amount}"
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

                # Add classified postings
                classified_postings = classify_postings(entry)
                for posting in classified_postings:
                    posting_id = journal.add_posting(journal_id, posting)

            except Exception as e:
                print(f"Error parsing line {line_no} in {relPath.name}: {e}")
                raise
