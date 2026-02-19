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


def get_paid_lots(db_path, fiscal_year: int) -> set[int]:
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT DISTINCT lot FROM posting WHERE invoice LIKE ? AND lot IS NOT NULL",
            (f"{fiscal_year}%",),
        )
        return {row[0] for row in cursor.fetchall()}
    finally:
        conn.close()


def main():
    fiscal_year = 2026
    bcc_mode = "--bcc" in sys.argv

    directory = MemberDirectory(config.DIRECTORY)
    paid_lots = get_paid_lots(config.DATABASE, fiscal_year)

    billable_lots = directory.get_all_lots_for_billing()
    unpaid = sorted(lot for lot in billable_lots if lot not in paid_lots)

    if bcc_mode:
        emails = []
        for lot_num in unpaid:
            lot = directory.get_lot(lot_num)
            emails.extend(lot.emails)
        print(", ".join(emails))
    else:
        print(
            f"Unpaid dues for {fiscal_year} ({len(unpaid)} of {len(billable_lots)} lots):"
        )
        print()
        for lot_num in unpaid:
            lot = directory.get_lot(lot_num)
            owners = ", ".join(lot.owners)
            emails = ", ".join(lot.emails) if lot.emails else "no email on file"
            print(f"  Lot {lot_num:2d}: {owners} <{emails}>")


if __name__ == "__main__":
    main()
