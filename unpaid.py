#!/usr/bin/env python3
"""
unpaid.py

Report lots that have not paid 2026 dues.

Usage:
    unpaid.py           # human-readable report
    unpaid.py --bcc     # email addresses in BCC format
"""

import sys
from hoa import config
from hoa.members import MemberDirectory
import sqlite3


def get_dues_balances(db_path, fiscal_year: int) -> dict[int, int]:
    """Unpaid dues only — serial 00 invoices for the given year."""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT lot, SUM(amount) AS balance
            FROM posting
            WHERE account LIKE 'assets:receivables:lot%'
              AND invoice LIKE ?
            GROUP BY lot
        """,
            (f"{fiscal_year}%00",),
        )
        return {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        conn.close()


def get_total_balances(db_path) -> dict[int, int]:
    """Total receivables balance across all charges."""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            SELECT lot, SUM(amount) AS balance
            FROM posting
            WHERE account LIKE 'assets:receivables:lot%'
            GROUP BY lot
            HAVING SUM(amount) != 0
        """
        )
        return {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        conn.close()


def main():
    fiscal_year = 2026
    bcc_mode = "--bcc" in sys.argv
    full_dues = int(config.DUES[fiscal_year] * 100)  # in cents

    directory = MemberDirectory(config.DIRECTORY)
    payment_totals = get_dues_balances(config.DATABASE, fiscal_year)
    billable_lots = directory.get_all_lots_for_billing()

    unpaid = []
    partial = []

    for lot_num in sorted(billable_lots):
        total = payment_totals.get(
            lot_num, full_dues
        )  # not in results = never billed, treat as full dues owed
        if total == full_dues:
            unpaid.append(lot_num)
        elif total > 0:
            partial.append((lot_num, total))
        # total == 0 means fully paid, skip

    if bcc_mode:
        emails = []
        for lot_num in unpaid:
            lot = directory.get_lot(lot_num)
            emails.extend(lot.emails)
        print(", ".join(emails))
    else:
        print(f"Dues status for {fiscal_year} (full dues: ${full_dues / 100:.2f})\n")

        print(f"Unpaid ({len(unpaid)} lots):")
        for lot_num in unpaid:
            lot = directory.get_lot(lot_num)
            owners = ", ".join(lot.owners)
            emails = ", ".join(lot.emails) if lot.emails else "no email on file"
            print(f"  Lot {lot_num:2d}: {owners} <{emails}>")

        print(f"\nPartial ({len(partial)} lots):")
        for lot_num, total in partial:
            lot = directory.get_lot(lot_num)
            owners = ", ".join(lot.owners)
            paid = (full_dues - total) / 100
            shortfall = total / 100
            print(
                f"  Lot {lot_num:2d}: {owners} -- paid ${paid:.2f}, owes ${shortfall:.2f}"
            )


if __name__ == "__main__":
    main()
