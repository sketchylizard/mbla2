#!/usr/bin/env python3
"""
import.py

Entry point for importing source files into the HOA Journal.
Dispatches to specialized importer modules based on file location.
"""

from decimal import Decimal
from pathlib import Path
import locale

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


def print_summary(
    journal: Journal,
    checking_before: Decimal,
    savings_before: Decimal,
) -> None:

    checking_after = journal.get_balance("Checking 0947")
    savings_after = journal.get_balance("Savings 9625")

    print("\nACCOUNT BALANCES (after import)")
    print("-------------------------------")

    print(f"Checking 0947")
    print(
        f"  Opening balance (prior):   {locale.currency(checking_before, grouping=True)}"
    )
    # print(f"  Period activity:")
    # print(f"    Debits (checks/fees):   -$539.00")
    # print(f"    Credits (deposits):    +$1,200.00")
    # print(f"  Net change:               +$661.00")
    print(
        f"  Expected ending balance:  {locale.currency(checking_after, grouping=True)}"
    )
    print(f"")
    print(f"Savings 9625")
    print(
        f"  Opening balance (prior):   {locale.currency(savings_before, grouping=True)}"
    )
    # print(f"  Period activity:           +$XX.XX")
    print(
        f"  Expected ending balance:   {locale.currency(savings_after, grouping=True)}"
    )


def main():

    journal = Journal(config.DATABASE)

    checking_before = journal.get_balance("Checking 0947")
    savings_before = journal.get_balance("Savings 9625")

    sources_root = config.SOURCES.resolve()

    # Parse each kind of source
    import_banks(journal)
    import_receipts(journal)

    print_summary(journal, checking_before, savings_before)


if __name__ == "__main__":
    main()
