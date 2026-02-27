#!/usr/bin/env python3
"""
paid200.py - List owners who paid $200 in dues for FY2026
"""

import sqlite3
from hoa import config
from hoa.members import MemberDirectory


def main():
    directory = MemberDirectory(config.DIRECTORY)

    conn = sqlite3.connect(config.DATABASE)
    cursor = conn.execute(
        """
        SELECT p.lot, p.amount / 100.0 AS amount
        FROM posting p
        WHERE p.invoice LIKE '2026%00'
          AND p.journal_id <> 208
          AND p.amount = -20000
        ORDER BY p.lot
        """
    )

    print(f"Owners who paid $200 for FY2026 dues:\n")
    for lot_num, amount in cursor.fetchall():
        lot = directory.get_lot(lot_num)
        owners = ", ".join(lot.owners) if lot else "Unknown"
        print(f"  Lot {lot_num:2d}  {owners}")

    conn.close()


if __name__ == "__main__":
    main()
