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


def import_banks(journal: Journal) -> None:
    """
    Import all files under a given bank directory.
    """

    for bank_path in (config.SOURCES / "bank").glob("*"):
        if bank_path.is_dir():
            rel_path = bank_path.relative_to(config.SOURCES)

            if len(rel_path.parts) < 2:
                print(f"Error: bank source missing bank code: {rel_path}")
                return

            bank_code = rel_path.parts[1]

            if bank_code == "truist":
                truist.import_files(bank_path, journal)
            else:
                print(f"Error: no importer for bank '{bank_code}'")


def import_receipts(journal: Journal) -> None:
    """
    Import all files under the receipts directory.
    """
    receipts_path = config.SOURCES / "receipts"
    for path in receipts_path.glob("*.toml"):
        if path.is_file():
            abs_path = path.resolve()
            pass


def main():

    journal = Journal(config.DATABASE)

    sources_root = config.SOURCES.resolve()

    # Parse each kind of source
    import_banks(journal)
    import_receipts(journal)

    print("Import completed.")


if __name__ == "__main__":
    main()
