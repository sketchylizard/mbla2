# journal.py

from __future__ import annotations
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Set
import sqlite3

from hoa.models import Transaction, Posting, Source, TxType

from hoa import config


@dataclass(frozen=True)
class JournalEntry:
    posted_date: date
    effective_date: date
    type: TxType
    description: str
    memo: str | None
    serial: str | None
    amount: Decimal
    postings: list[Posting]
    transactions: List[Transaction]


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
                serial          TEXT -- check # or other serial number
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
            reference TEXT, -- check # or other reference
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
        cursor = self.conn.cursor()
        cursor.execute(
            """
        INSERT INTO posting
        (journal_id, account, amount, lot, invoice, reference)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                journal_id,
                posting.account,
                int(posting.amount * 100),  # store as integer cents
                posting.lot,
                posting.invoice,
                posting.reference,
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

    def _validate_entry(self, entry: JournalEntry) -> None:
        posting_accounts = []

        total: Decimal = 0
        for p in entry.postings:
            posting_accounts.append(p.account)
            if p.amount == 0:
                raise ValueError(
                    f"Zero-amount posting in account '{p.account}' "
                    f"for entry '{entry.description}'"
                )
            total += p.amount

        if total != Decimal(0):
            raise ValueError(
                f"JournalEntry postings do not balance to zero: {total} "
                f"for entry '{entry.description}' on {entry.posted_date}"
            )

        # We have at least once source transaction (maybe two for transfers)
        # Make sure each source account is only listed once.
        for tx in entry.transactions:
            if posting_accounts.count(tx.account) != 1:
                raise ValueError(
                    f"Source transaction account '{tx.account}' "
                    f"not found exactly once in postings for entry "
                    f"'{entry.description}' on {entry.posted_date}"
                )

    def add_entry(self, entry: JournalEntry, source: Source) -> int | None:
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
            self._validate_entry(entry)

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

            for tx in entry.transactions:
                self.add_source(journal_id, replace(source, line=tx.line), tx.hash())

            self.conn.commit()
            return journal_id
        except sqlite3.IntegrityError as e:
            # Duplicate source_hash

            print(f"IntegrityError: {e}")
            return None

    def close(self) -> None:
        self.conn.close()

    def get_hashes(self) -> Set[str]:
        """
        Returns a set of all source_hashes in the journal_entry_source table.
        """

        cursor = self.conn.cursor()
        cursor.execute("SELECT source_hash FROM journal_entry_source")
        hashes = set(row[0] for row in cursor.fetchall())
        return hashes

    def get_balance(
        self,
        account: str,
        *,
        as_of: date | None = None,
    ) -> Decimal:
        """
        Return the balance of an account.

        Balance is defined as the sum of postings.amount
        using posted_date semantics.

        If as_of is provided, only journal entries with
        posted_date <= as_of are included.
        """
        sql = """
        SELECT
            COALESCE(SUM(p.amount), 0)
        FROM posting p
        JOIN journal_entry j
          ON j.journal_id = p.journal_id
        WHERE p.account = :account
          AND (:as_of IS NULL OR j.posted_date <= :as_of)
        """

        params = {
            "account": account,
            "as_of": as_of.isoformat() if as_of else None,
        }

        cur = self.conn.execute(sql, params)
        row = cur.fetchone()

        # row[0] is guaranteed not NULL because of COALESCE
        return Decimal(int(row[0])) / 100
