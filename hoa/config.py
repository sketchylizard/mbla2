# config.py

from pathlib import Path

# Project root = parent of this file (assuming config.py is in hoa/)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Root of all statements (per-bank files go under this folder)
STATEMENTS = PROJECT_ROOT / "statements"

BANK_CODE = "truist"
BANK_NAME = "Truist Bank"
