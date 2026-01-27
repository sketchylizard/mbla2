#!/usr/bin/env python3
"""
import.py

Entry point for importing source files into the HOA Journal.
Dispatches to specialized importer modules based on file location.
"""

from decimal import Decimal
from pathlib import Path
from typing import List
import locale
import os

from hoa import config
from hoa.journal import Journal, Posting, JournalEntry
from hoa.models import merge_transfers, Source, Transaction, TxType
from hoa.members import MemberDirectory, Lot


def print_summary(
    journal: Journal,
    checking_before: Decimal,
    savings_before: Decimal,
) -> None:

    checking_after = journal.get_balance("assets:truist:checking")
    savings_after = journal.get_balance("assets:truist:savings")

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


def is_applicable(event: Transaction) -> bool:
    """
    Determine if the given Transaction should be journalized.
    """

    # If the from or to account are two "assets:truist" accounts, then we can journalize it.
    if event.from_account and event.from_account.startswith("assets:truist"):
        return True

    if event.to_account and event.to_account.startswith("assets:truist"):
        return True

    if event.bank == "venmo":
        # check for keywords
        keywords = [
            "mbla",
            "miles branch",
            "mbloa",
            "dues",
            "lot",
            "lonna",
            "carson",
            "lauren",
            "hoa",
            "mbhoa",
        ]
        if event.memo is None:
            return False

        for keyword in keywords:
            if keyword in event.memo.lower():
                return True
        return False

    return True


def journal_entry_from_event(
    event: Transaction,
    directory: MemberDirectory,
) -> JournalEntry:

    # If we have an annotation with get_postings method, use it
    if event.annotation:
        contra_postings = event.annotation.postings
        remaining_amount = event.amount
        for p in contra_postings:
            if p.amount != Decimal(0):
                if abs(p.amount) > abs(remaining_amount):
                    raise ValueError(
                        f"Annotation postings exceed transaction amount: "
                        f"{p.amount} > {remaining_amount}"
                    )
                remaining_amount -= p.amount
            else:
                if remaining_amount == 0:
                    raise ValueError(
                        f"Annotation postings exceed transaction amount: "
                        f"no remaining amount for open posting"
                    )
                p.amount = remaining_amount
                remaining_amount = Decimal(0)

        # The bank side posting
        if event.from_account:
            # Money leaving
            bank_posting = Posting(account=event.from_account, amount=-event.amount)
        else:
            # Money arriving
            bank_posting = Posting(account=event.to_account, amount=event.amount)

        description = event.annotation.description or event.description or ""
        memo = event.annotation.memo or event.memo

        return JournalEntry(
            posted_date=event.posted_date,
            amount=event.amount,
            description=description,
            type=event.type,
            memo=memo,
            postings=[bank_posting, *contra_postings],
            reference=event.reference,
            source=event.source,
            transfer_source=event.transfer_source,
        )

    # Otherwise, use existing two-posting logic
    lot = directory.find_lot_by_name(event.description)

    from_account = event.from_account
    to_account = event.to_account
    memo = event.memo

    if lot:
        if from_account is None:
            from_account = f"assets:receivables:lot{lot.lot_number}"
        elif to_account is None:
            to_account = f"assets:payables:lot{lot.lot_number}"

    assert (
        from_account is not None or to_account is not None
    ), "At least one of from_account or to_account must be set"

    other_account = None

    if event.type == "debit" or event.type == "deposit":
        other_account = "assets:income:unknown"
    if event.type in ("credit", "check", "fee"):
        other_account = "expenses:unknown"

    if from_account is None:
        from_account = other_account

    if to_account is None:
        to_account = other_account

    if from_account is None or to_account is None:
        print(event)
        raise ValueError(
            f"Cannot journalize event: " f"from={from_account}, to={to_account}"
        )

    postings = (
        Posting(account=from_account, amount=-event.amount),
        Posting(account=to_account, amount=event.amount),
    )

    return JournalEntry(
        posted_date=event.posted_date,
        amount=event.amount,
        description=event.description or "",
        type=event.type,
        memo=memo,
        postings=postings,
        reference=event.reference,
        source=event.source,
        transfer_source=event.transfer_source,
    )


def create_journal_entries(
    journal: Journal, directory: MemberDirectory, events: List[Transaction]
) -> None:
    skipped = []

    for event in events:
        if not is_applicable(event):
            skipped.append(event)
            continue

        entry = journal_entry_from_event(event, directory)
        journal.add_entry(entry)

    print("\nSkipped events:")
    for event in skipped:
        print(
            f"  - {event.description}, {event.memo}, ({event.amount}), from: {event.from_account}, to: {event.to_account}    "
        )


def filter_out_external_accounts(events: List[Transaction]) -> List[Transaction]:
    filtered = []
    for event in events:
        # Skip if from_account or to_account contains "external:"
        if not is_applicable(event):
            continue
        filtered.append(event)
    return filtered


def main():

    journal = Journal(config.DATABASE)
    directory = MemberDirectory(config.DIRECTORY)

    checking_before = journal.get_balance("assets:truist:checking")
    savings_before = journal.get_balance("assets:truist:savings")

    sources_root = config.SOURCES.resolve()

    # Recursively find all files under the sources directory and dispatch to the appropriate importer based on file
    # path.

    all_transactions = []

    dirs = list(sources_root.glob("*"))
    for dir in dirs:
        if not dir.is_dir():
            continue

        importer_name = dir.name  # 'truist', 'venmo', 'journals'

        # if importer_name == "venmo":
        #     continue

        # Dynamically import the processor
        try:
            importer = __import__(
                f"hoa.importers.{importer_name}", fromlist=["process"]
            )
            transactions = importer.process(dir)

            print(f"Processed {len(transactions)} from {importer_name}")

            # Filter out external accounts
            transactions = filter_out_external_accounts(transactions)

            # Match transfer transactions
            all_transactions = merge_transfers(all_transactions, transactions, 5)

        except ModuleNotFoundError:
            print(f"Warning: No importer found for {importer_name}")

    create_journal_entries(journal, directory, all_transactions)

    print_summary(journal, checking_before, savings_before)


if __name__ == "__main__":
    main()
