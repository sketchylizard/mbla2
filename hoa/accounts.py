# All accounts that have been seen when importing from Truist or Venmo:

import re

ACCOUNT_NORMALIZATION = [
    # Truist checking
    (re.compile(r"Transactions for Checking 0947"), "assets:truist:checking"),
    (re.compile(r"Truist \*0947"), "assets:truist:checking"),
    (re.compile(r"ONLINE (FROM|TO) \**0947"), "assets:truist:checking"),
    (re.compile(r"0947"), "assets:truist:checking"),
    # Truist savings
    (re.compile(r"Transactions for Savings 9625"), "assets:truist:savings"),
    (re.compile(r"Truist \*9625"), "assets:truist:savings"),
    (re.compile(r"ONLINE FROM \**9625"), "assets:truist:savings"),
    (re.compile(r"9625"), "assets:truist:savings"),
    # Coastal FCU
    (re.compile(r"COASTAL FEDERAL CREDIT UNION (.*)\*9027"), "assets:coastal:external"),
    # Debit card
    (re.compile(r"Visa \*0670"), "assets:coastal:external"),
]


def normalize(raw: str | None) -> str | None:
    if raw is None:
        return None

    raw = raw.strip()
    for pattern, normalized in ACCOUNT_NORMALIZATION:
        if pattern.search(raw):
            return normalized
    return f"external:{raw}"
