# config.py

from pathlib import Path
import locale

# Project root = parent of this file (assuming config.py is in hoa/)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

SOURCES = PROJECT_ROOT / "sources"

BANK = SOURCES / "bank"
RECEIPTS = SOURCES / "receipts"
JOURNALS = SOURCES / "journals"
DIRECTORY = SOURCES / "directory.yaml"

BANKS = {
    "truist": "Truist Bank",
}

DATABASE = PROJECT_ROOT / "mbla.db"

ASSOCIATION_NAME = "Miles Branch Landowners Association"

# Set locale (do once at startup)
locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
