from hoa import config
from hoa.models import SourceTransaction
from decimal import Decimal
import sqlite3


class Ledger:
    def __init__(self, db_path: str):
        import sqlite3

        self.conn = sqlite3.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        c = self.conn.cursor()
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS source (
            hash TEXT PRIMARY KEY,
            bank TEXT,
            account TEXT,
            posted_date DATE,
            type TEXT,
            serial TEXT,
            description TEXT,
            merchant TEXT,
            amount NUMERIC,
            raw_csv TEXT
        )
        """
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS ledger_entry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_hash TEXT,
            account TEXT,
            amount NUMERIC,
            memo TEXT,
            FOREIGN KEY(source_hash) REFERENCES source(hash)
        )
        """
        )
        self.conn.commit()

    def add_source(self, tx: SourceTransaction, raw_csv: str) -> str:
        """Add immutable source row; returns hash."""
        h = tx.sha1()
        try:
            self.conn.execute(
                """
                INSERT INTO source(hash, bank, account, posted_date, type, serial, description, merchant, amount, raw_csv)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    h,
                    config.BANK_CODE,
                    tx.account,
                    tx.posted_date.isoformat(),
                    tx.type,
                    tx.serial,
                    tx.description,
                    tx.merchant,
                    float(tx.amount),
                    raw_csv,
                ),
            )
            self.conn.commit()
        except sqlite3.IntegrityError:
            # Already exists
            pass
        return h

    def add_entry(
        self, source_hash: str, account: str, amount: Decimal, memo: str | None = None
    ):
        """Add a ledger posting referencing a source transaction."""
        self.conn.execute(
            """
            INSERT INTO ledger_entry(source_hash, account, amount, memo)
            VALUES (?, ?, ?, ?)
        """,
            (source_hash, account, float(amount), memo),
        )
        self.conn.commit()
