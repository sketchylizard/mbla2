#!/usr/bin/env python3

from datetime import date
from decimal import Decimal
from pathlib import Path
import argparse
import csv
import shutil

from hoa import config
from hoa.models import SourceTransaction

DATE_PATTERNS = [
    # 12_20_2025 or 12-20-2025
    r"(\d{1,2})[._-](\d{1,2})[._-](\d{4})",
    # 2025_12_20 or 2025-12-20
    r"(\d{4})[._-](\d{1,2})[._-](\d{1,2})",
]


def parse_dates_from_filename(name: str) -> list[date]:
    import re

    dates = []

    for pattern in DATE_PATTERNS:
        regex = re.compile(pattern)
        for match in regex.finditer(name):
            parts = match.groups()
            try:
                if len(parts[0]) == 4:
                    y, m, d = map(int, parts)
                else:
                    m, d, y = map(int, parts)
                dates.append(date(y, m, d))
            except ValueError:
                pass

    return sorted(set(dates))


def conform_filename(original: Path) -> str | None:
    dates = parse_dates_from_filename(original.name)
    if len(dates) >= 2:
        start, end = dates[0], dates[-1]
        return f"{config.BANK_CODE}_{start.isoformat()}_{end.isoformat()}.csv"
    return None


def move_to_statements(file_path: Path, dry_run=False, copy_only=False) -> Path:
    """
    Move or copy file into statements directory with conformed name.
    Returns the Path of the canonical file in statements/.
    """
    statements_dir = Path(config.STATEMENTS)
    statements_dir.mkdir(parents=True, exist_ok=True)

    # Conform filename with bank code and date range
    conformed_name = conform_filename(file_path)
    if conformed_name is None:
        conformed_name = file_path.name  # fallback

    dest_path = statements_dir / conformed_name

    action = "Would copy" if dry_run else "Copying"
    if copy_only:
        print(f"{action} {file_path} -> {dest_path}")
        if not dry_run:
            shutil.copy2(file_path, dest_path)
    else:
        action = "Would move" if dry_run else "Moving"
        print(f"{action} {file_path} -> {dest_path}")
        if not dry_run:
            shutil.move(str(file_path), str(dest_path))

    return dest_path


DESCRIPTION_RENAME = {
    "INTEREST PAYMENT": "Interest payment",
    "SERVICE CHARGES - PRIOR PERIOD": "Service charges for prior period",
}


import re


def normalize_description(desc: str) -> str:
    desc = desc.strip()
    # Known replacements
    replacements = {
        "INTEREST PAYMENT": "Interest payment",
        "SERVICE CHARGES - PRIOR PERIOD": "Service charges for prior period",
    }
    if desc in replacements:
        return replacements[desc]

    # Pattern for Venmo cashout
    venmo_match = re.search(r"CASHOUT VENMO", desc, re.IGNORECASE)
    if venmo_match:
        return "Venmo cashout"

    # Fallback: title case all-uppercase
    return desc.title() if desc.isupper() else desc


def normalize_merchant(merchant: str | None) -> str | None:
    if not merchant:
        return None
    merchant = merchant.strip()
    if "VENMO" in merchant.upper():
        return "Venmo"
    return merchant.title() if merchant.isupper() else merchant


def parse_truist_csv(file_path: Path) -> list[dict]:
    """
    Parse a Truist CSV file and return a list of transaction dicts:
    {
        'account': str,       # checking or savings
        'posted_date': date,
        'type': str,
        'serial': str | None,
        'description': str,
        'merchant': str | None,
        'amount': Decimal,
    }
    """
    transactions = []
    current_account = None

    with file_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)

        for row in reader:
            if not row or all(not col.strip() for col in row):
                continue  # skip empty rows

            # Detect account code line
            if row[0].startswith("Transactions for"):
                current_account = row[0].split()[-1]
                continue

            # Skip header row
            if row[0] == "Posted Date":
                continue

            # Regular transaction row
            posted_str, trans_str, trans_type, serial, full_desc, merchant, *rest = row
            amount_str = rest[-2]  # Amount column

            try:
                posted_date = date.fromisoformat(posted_str)
            except ValueError:
                # Truist format may be MM/DD/YYYY
                month, day, year = map(int, posted_str.split("/"))
                posted_date = date(year, month, day)

            # Normalize amount
            amount = Decimal(
                amount_str.replace("$", "")
                .replace(",", "")
                .replace("(", "-")
                .replace(")", "")
            )

            transactions.append(
                SourceTransaction(
                    account=current_account,
                    posted_date=posted_date,
                    type=trans_type.strip(),
                    serial=serial.strip() if serial else None,
                    description=normalize_description(full_desc),
                    merchant=normalize_merchant(merchant),
                    amount=amount,
                )
            )

    return transactions


def main() -> None:
    parser = argparse.ArgumentParser(description="Import bank CSV files")
    parser.add_argument("files", nargs="+", help="CSV files to import")
    parser.add_argument("--dry-run", action="store_true", help="Do not modify any data")
    parser.add_argument(
        "--copy-only", action="store_true", help="Copy instead of move files"
    )
    parser.add_argument("--db", help="Override path to ledger database")

    args = parser.parse_args()

    for filename in args.files:
        path = Path(filename).expanduser()

        if not path.exists():
            print(f"File does not exist: {path}")
            continue

        print(f"Input file: {path}")
        print(f"Bank: {config.BANK_CODE}")

        canonical_path = move_to_statements(
            path, dry_run=args.dry_run, copy_only=args.copy_only
        )

        transactions = parse_truist_csv(canonical_path)
        print(f"Parsed {len(transactions)} transactions from {canonical_path}")

        # For now, just print the first few
        for tx in transactions:
            print(f"{tx.sha1()} | {tx.posted_date} | {tx.amount} | {tx.description}")


if __name__ == "__main__":
    main()
