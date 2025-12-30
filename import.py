#!/usr/bin/env python3

import argparse
from pathlib import Path
from datetime import date
import shutil

from hoa import config

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

        # Placeholder: call importer / ledger processing
        # importer.import_csv(canonical_path, db=args.db)

        print(f"Ready to import: {canonical_path}\n")


if __name__ == "__main__":
    main()
