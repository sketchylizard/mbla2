# journal.py

from __future__ import annotations
from pathlib import Path
import sqlite3
from typing import List, Optional
from decimal import Decimal
from datetime import date

from hoa.models import JournalEntry, Posting, Source

from hoa import config


class Journal:
    def __init__(self, db_path: Path | str = config.DATABASE):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
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
                serial          TEXT,
                account         TEXT NOT NULL,
                bank            TEXT NOT NULL,
                source_hash     TEXT NOT NULL UNIQUE, -- ensures uniqueness
                source_filename TEXT NOT NULL,        -- from Source
                source_line_no  INTEGER NOT NULL      -- from Source
            );
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
        self.conn.commit()

    def add_entry(
        self, entry: JournalEntry, source: Source, source_hash: str
    ) -> int | None:
        """
        Inserts a journal entry into the database.

        Parameters
        ----------
        entry : JournalEntry
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
            (posted_date, effective_date, tx_type, description, amount, memo, serial, account, source_hash, bank, source_filename, source_line_no)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    entry.posted_date,
                    entry.effective_date,
                    entry.tx_type,
                    entry.description,
                    int(entry.amount * 100),  # store as integer cents
                    entry.memo,
                    entry.serial,
                    entry.account,
                    source_hash,
                    source.bank_code,
                    source.file,
                    source.line,
                ),
            )
            journal_id = cursor.lastrowid
            self.conn.commit()
            return journal_id
        except sqlite3.IntegrityError:
            # Duplicate source_hash
            return None

    def add_posting(self, journal_id: int, posting: Posting) -> int:
        """
        Insert a single Posting for the given journal_id.
        posting_id is automatically assigned based on existing postings.
        """
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
        self.conn.commit()
        return posting_id

    def close(self) -> None:
        self.conn.close()
