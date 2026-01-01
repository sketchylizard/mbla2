#!/usr/bin/env python3
"""
import.py

Entry point for importing source files into the HOA Journal.
Dispatches to specialized importer modules based on file location.
"""

import sys
from pathlib import Path
from typing import Sequence

from hoa import config
from hoa.journal import Journal

from hoa.importers.bank.truist import truist
from hoa.importers import receipts, manual


def normalize_source_path(path: str | Path) -> Path:
    """
    Resolve path to an absolute path and ensure it lives under config.SOURCES.
    Returns a Path object relative to SOURCES.
    """
    p = Path(path).expanduser().resolve()
    sources_root = config.SOURCES.resolve()

    try:
        rel = p.relative_to(sources_root)
    except ValueError:
        print(f"Error: {p} is not under sources directory {sources_root}")
        return None

    return rel


def dispatch_importer(abs_path: Path, rel_path: Path, journal: Journal) -> None:
    """
    Determine the appropriate importer based on rel_path and call it.
    """
    if not rel_path.parts:
        print(f"Error: empty path {abs_path}")
        return

    head = rel_path.parts[0]

    # Bank imports
    if head == config.BANK.name:
        if len(rel_path.parts) < 2:
            print(f"Error: bank source missing bank code: {rel_path}")
            return
        bank_code = rel_path.parts[1]
        if bank_code == "truist":
            truist.import_file(abs_path, rel_path, journal)
            return

        print(f"Error: no importer for bank '{bank_code}'")
        return

    # Receipt imports
    if head == config.RECEIPTS.name:
        print(f"Importing receipt file: {rel_path}")
        receipts.import_file(abs_path, rel_path, journal)
        return

    # Manual entry imports
    if head == config.MANUAL.name:
        print(f"Importing manual entry file: {rel_path}")
        manual.import_file(abs_path, rel_path, journal)
        return

    print(f"Error: unrecognized source path: {rel_path}")


def import_sources(paths: Sequence[str], journal: Journal) -> None:
    """
    Iterate over the given paths, normalize, and dispatch each for import.
    """
    for p in paths:
        abs_path = Path(p).expanduser().resolve()
        rel_path = normalize_source_path(abs_path)
        if rel_path is None:
            continue
        dispatch_importer(abs_path, rel_path, journal)


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <file_or_directory> [more files...]")
        sys.exit(1)

    journal = Journal(config.DATABASE)
    paths = sys.argv[1:]
    import_sources(paths, journal)
    print("Import completed.")


if __name__ == "__main__":
    main()
