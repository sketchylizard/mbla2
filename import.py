#!/usr/bin/env python3
"""
import.py

Entry point for importing source files into the HOA Journal.
Dispatches to specialized importer modules based on file location.
"""

from decimal import Decimal
from pathlib import Path
from typing import List

import importlib
import locale
import os
import pkgutil

from hoa import config
from hoa.journal import Journal, Posting, JournalEntry
from hoa.members import MemberDirectory, Lot
from hoa.models import Invoice, merge_transfers, Source, Transaction, TxType
import hoa.importers


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
        if event.memo is None:
            return False

        memo_lower = event.memo.lower()
        return any(keyword in memo_lower for keyword in config.VENMO_HOA_KEYWORDS)

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
    invoice = None
    is_dues = False

    if lot:
        # Check if this is a dues payment using HOA keywords
        memo_lower = (memo or "").lower()
        is_dues = any(keyword in memo_lower for keyword in config.VENMO_HOA_KEYWORDS)

        if is_dues:
            # Dues payment - use receivables and assign invoice
            fiscal_year = event.posted_date.year
            if event.posted_date.month == 12:  # December pays next year
                fiscal_year += 1
            invoice = Invoice(f"{fiscal_year}{lot.lot_number:02d}00")

        if from_account is None:
            from_account = f"assets:receivables:lot{lot.lot_number:02}"
        elif to_account is None:
            to_account = f"assets:payables:lot{lot.lot_number:02}"

    if event.postings:
        # If there are already postings from an annotation, we won't apply the name-based logic
        # to avoid conflicts. Instead, we'll just journalize the event as-is.
        return JournalEntry(
            posted_date=event.posted_date,
            amount=event.amount,
            description=event.description or "",
            type=event.type,
            memo=event.memo,
            postings=event.postings,
            reference=event.reference,
            source=event.source,
            transfer_source=event.transfer_source,
        )

    assert (
        from_account is not None or to_account is not None
    ), "At least one of from_account or to_account must be set"

    other_account = None

    if event.type in ("debit", "deposit"):
        other_account = "expenses:unknown"
    if event.type in ("credit",):
        other_account = "income:unknown"
    if event.type in ("check", "fee"):
        other_account = "expenses:unknown"

    # Override for dues payments
    if is_dues and event.type == "credit":
        other_account = "income:dues"

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
        Posting(account=from_account, amount=-event.amount, invoice=invoice),
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

    all_transactions = []
    # Load each importer and let it process any new transactions, then merge them into the journal entries list.
    for finder, importer_name, ispkg in pkgutil.iter_modules(hoa.importers.__path__):
        try:
            importer = importlib.import_module(f"hoa.importers.{importer_name}")
            transactions = importer.process()
            print(f"Processed {len(transactions)} from {importer_name}")
            transactions = filter_out_external_accounts(transactions)
            all_transactions = merge_transfers(all_transactions, transactions, 5)
        except Exception as e:
            print(f"Warning: importer {importer_name} failed: {e}")

    create_journal_entries(journal, directory, all_transactions)

    print_summary(journal, checking_before, savings_before)


if __name__ == "__main__":
    main()
