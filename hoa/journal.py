# journal.py

from __future__ import annotations
from pathlib import Path
import sqlite3
from typing import List, Tuple
from decimal import Decimal
from datetime import date

from hoa.models import Transaction, Posting, Source, TransactionAndSource

from hoa import config


class Journal:
    def __init__(self, db_path: Path | str = config.DATABASE):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, autocommit=False)
        self.conn.row_factory = sqlite3.Row
        self._initialize_tables()

    def _initialize_tables(self) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS journal_entry (
                journal_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                posted_date     TEXT NOT NULL,        -- ISO date
                effective_date  TEXT NOT NULL,        -- ISO date
                tx_type         TEXT NOT NULL,
                description     TEXT NOT NULL,
                amount          INTEGER NOT NULL,     -- stored as cents
                memo            TEXT,
                serial          TEXT
            )
            """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS posting (
            posting_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id INTEGER NOT NULL,
            account TEXT NOT NULL,
            amount INTEGER NOT NULL,
            lot INTEGER,
            invoice TEXT,
            FOREIGN KEY(journal_id) REFERENCES journal_entry(journal_id)
        )
        """
        )

        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS journal_entry_source (
                journal_id TEXT NOT NULL,
                source_hash     TEXT NOT NULL UNIQUE, -- ensures uniqueness
                source_bank     TEXT,                 -- from Source
                source_filename TEXT NOT NULL,        -- from Source
                source_line_no  INTEGER NOT NULL,     -- from Source
                PRIMARY KEY (source_hash)
            )
            """
        )

        self.conn.commit()

    def _add_posting(self, journal_id: int, posting: Posting) -> int:
        """
        Insert a single Posting for the given journal_id.
        posting_id is automatically assigned based on existing postings.
        """
        if abs(posting.amount) == Decimal(8283.00):
            print("Debug: Adding posting with amount 8283.00")
        cursor = self.conn.cursor()
        cursor.execute(
            """
        INSERT INTO posting
        (journal_id, account, amount, lot, invoice)
        VALUES (?, ?, ?, ?, ?)
        """,
            (
                journal_id,
                posting.account,
                int(posting.amount * 100),  # store as integer cents
                posting.lot,
                posting.invoice,
            ),
        )
        posting_id = cursor.lastrowid
        return posting_id

    def add_source(
        self,
        journal_id: int,
        source: Source,
        hash: str,
    ) -> None:

        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO journal_entry_source (journal_id,
            source_hash, source_bank, source_filename, source_line_no)
                VALUES (?, ?, ?, ?, ?)
            """,
            (
                journal_id,
                hash,
                source.bank_code,
                source.file,
                source.line,
            ),
        )

    def add_entry(self, entry: Journal) -> int | None:
        """
        Inserts a journal entry into the database.

        Parameters
        ----------
        entry : Transaction
            The in-memory semantic entry.
        source : Source
            Information about the origin (filename, line number, etc.)
        source_hash : str
            Computed hash for uniqueness.

        Returns
        -------
        int | None
            journal_id if inserted successfully,
            None if the source_hash already exists.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
            INSERT INTO journal_entry
            (posted_date, effective_date,
             tx_type, description, amount,
             memo, serial)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    entry.posted_date,
                    entry.effective_date,
                    entry.type,
                    entry.description,
                    int(entry.amount * 100),  # store as integer cents
                    entry.memo,
                    entry.serial,
                ),
            )
            journal_id = cursor.lastrowid

            for posting in entry.postings:
                self._add_posting(journal_id, posting)

            for tx, source in entry.transactions:
                self.add_source(journal_id, source, tx.hash())

            self.conn.commit()
            return journal_id
        except sqlite3.IntegrityError as e:
            # Duplicate source_hash

            print(f"IntegrityError: {e}")
            return None

    def close(self) -> None:
        self.conn.close()

    def is_duplicate(self, source_hash: str) -> bool:
        """
        Check if a journal entry with the given source_hash already exists.

        Parameters
        ----------
        source_hash : str
            The hash to check for uniqueness.

        Returns
        -------
        bool
            True if a duplicate exists, False otherwise.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT 1 FROM journal_entry_source WHERE source_hash = ?
            """,
            (source_hash,),
        )
        return cursor.fetchone() is not None
