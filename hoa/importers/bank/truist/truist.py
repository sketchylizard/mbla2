from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable
import csv
import re
import tomllib

from hoa import config
from hoa.journal import Journal
from hoa.models import JournalEntry, Posting, Source

RENAMING_RULES_FILE = "renaming_rules.toml"
POSTING_RULES_FILE = "posting_rules.toml"

RenamingRule = tuple[re.Pattern[str], str]


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
        rules.append(
            (
                re.compile(rule["pattern"]),
                rule["replacement"],
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


def normalize_description(
    description: str, renaming_rules: Iterable[RenamingRule]
) -> str:
    """
    Apply the first matching normalization rule.
    If no rule matches, return the original description.
    """
    for pattern, replacement in renaming_rules:
        if pattern.search(description):
            return pattern.sub(replacement, description).strip()

    description = description.strip()

    # If it's mostly uppercase, normalize it
    if description.isupper():
        return description.capitalize()

    return description


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
                    tx_type,
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

                description = normalize_description(raw_desc, renaming_rules)

                entry = JournalEntry(
                    posted_date=posted_date_iso,
                    effective_date=posted_date_iso,
                    tx_type=tx_type.lower(),
                    description=description,
                    memo=None,
                    serial=serial.strip() if serial else None,
                    account=current_account,
                    amount=amount,
                )

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
                    account=f"Assets:{current_account}",
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
