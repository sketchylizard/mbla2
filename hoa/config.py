# config.py

from pathlib import Path
from decimal import Decimal

import locale

# Project root = parent of this file (assuming config.py is in hoa/)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

SOURCES = PROJECT_ROOT / "sources"

DIRECTORY = SOURCES / "directory.yaml"

BANKS = {
    "truist": "Truist Bank",
}

DATABASE = PROJECT_ROOT / "mbla.db"

ASSOCIATION_NAME = "Miles Branch Landowners Association"

# Venmo transaction filtering
VENMO_HOA_KEYWORDS = [
    "carson",
    "dues",
    "hoa",
    "lauren",
    "loa",
    "lonna",
    "lot",
    "mbhoa",
    "mbla",
    "mbloa",
    "miles branch",
]

# Dues and fees by fiscal year
DUES = {
    2024: Decimal("150.00"),
    2025: Decimal("150.00"),
    2026: Decimal("200.00"),
}

LATE_FEE = {
    2024: Decimal("75.00"),
    2025: Decimal("75.00"),
    2026: Decimal("75.00"),
}

START_YEAR = 2024
# Set locale (do once at startup)
locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
