from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List

from hoa.journal import Posting  # assuming this exists
from hoa.journal import Transaction  # for entry.hash()
from hoa.models import TxType

# ============================================================================
# Annotation File Grammar (Informal EBNF + Semantic Rules)
#
# This file describes one or more annotations separated by a blank line.
# An annotation has three ordered sections:
#
#   1. Key section        (exactly one of: reconciled OR pending)
#   2. General fields     (optional, key=value pairs)
#   3. Postings           (one or more account postings)
#
# ---------------------------------------------------------------------------
# High-level structure
#
#   annotation =
#       key-section
#       general-section
#       posting-section
#       blank-line
#
# ---------------------------------------------------------------------------
# 1. Key section (mutually exclusive)
#
# Exactly ONE of the following key forms is allowed.
# Mixing reconciled and pending keys is an error.
#
#   key-section = reconciled-key | pending-key
#
# Reconciled key:
#
#   reconciled-key =
#       "hash" "=" <hex-string> NEWLINE
#
# Pending key:
#
#   pending-key =
#       "date"   "=" <YYYY-MM-DD> NEWLINE
#       "amount" "=" <decimal>    NEWLINE
#       "type"   "=" <identifier> NEWLINE
#       [ "serial" "=" <string> NEWLINE ]
#
# Semantic rules:
#   - Reconciled entries MUST contain only a hash key.
#   - Pending entries MUST contain date, amount, and type.
#   - serial is optional and participates in matching if present.
#
# ---------------------------------------------------------------------------
# 2. General fields (optional)
#
# General fields are simple key=value pairs that apply to the annotation
# as a whole (e.g. description, memo).
#
#   general-section =
#       { general-line }
#
#   general-line =
#       <identifier> "=" <string> NEWLINE
#
# Semantic rules:
#   - General fields MUST appear after the key section.
#   - General fields MUST appear before any posting lines.
#   - Unknown field names may be rejected or ignored by the parser.
#
# ---------------------------------------------------------------------------
# 3. Postings (one or more, required)
#
# Postings describe how the transaction is allocated.
#
#   posting-section =
#       posting-line { posting-line }
#
#   posting-line =
#       "account" "=" <account-name>
#       { " " posting-attribute }
#       NEWLINE
#
#   posting-attribute =
#       "amount"  "=" <decimal>
#     | "lot"     "=" <integer>
#     | "invoice" "=" <string>
#
# Semantic rules:
#   - At least one posting line is REQUIRED.
#   - Once a posting line is encountered, no further general fields are allowed.
#   - amount defaults to the transaction amount (negated for balancing posts)
#     if not explicitly provided.
#
# ---------------------------------------------------------------------------
# Lexical notes
#
#   <account-name> ::= name ( ":" name )*
#   <identifier>   ::= [A-Za-z][A-Za-z0-9_-]*
#   <decimal>      ::= -?[0-9]+(\.[0-9]+)?
#   <hex-string>   ::= [0-9a-fA-F]+
#
# ---------------------------------------------------------------------------
# Annotation termination
#
#   blank-line = NEWLINE
#
# A blank line terminates the current annotation. End-of-file also terminates
# the final annotation.
#
# ============================================================================


@dataclass
class PendingKey:
    date: date | None = None
    amount: Decimal | None = None
    type: TxType | None = None
    serial: str | None = None


@dataclass
class ReconciledKey:
    hash: str | None = None


# -----------------------------
# Data classes for annotations
# -----------------------------
@dataclass
class Annotation:
    key: PendingKey | ReconciledKey | None = None
    description: str | None = None
    memo: str | None = None
    postings: List[Posting] = field(default_factory=list)

    @property
    def is_reconciled(self) -> bool:
        return isinstance(self.key, ReconciledKey)

    def matches(self, entry: Transaction) -> bool:
        """Check if this resolved annotation matches the given journal entry."""

        # Check to see if we have a hash (reconciled) or a pending key
        if self.is_reconciled:
            return self.key.hash == entry.hash()
        else:
            pending_key = self.key
            # Pending dates will always be earlier than or equal to posted date
            if pending_key.date > entry.posted_date:
                return False
            if pending_key.amount != abs(entry.amount):
                return False
            if pending_key.type and pending_key.type != entry.type:
                return False
            if pending_key.serial and pending_key.serial != entry.serial:
                return False
            return True

    def resolve(self, entry_hash: str) -> Annotation:
        """Convert pending to resolved using the journal entry hash."""
        return Annotation(
            key=ReconciledKey(entry_hash),
            description=self.description,
            memo=self.memo,
            postings=self.postings,
        )


###########################################################################
# Parsing
# ###########################################################################
class AnnotationParserError(Exception):
    pass


class AnnotationParser:
    STATE_START = "START"
    STATE_KEY = "KEY"
    STATE_GENERAL = "GENERAL"
    STATE_POSTING = "POSTING"
    STATE_END = "END"

    def __init__(self, lines: List[str]):
        self.lines = lines
        self.line_no = 0
        self.state = self.STATE_START
        self.current: Optional[Annotation] = None
        self.annotations: List[Annotation] = []

    def parse(self) -> List[Annotation]:
        while self.line_no < len(self.lines):
            line = self.lines[self.line_no].strip()
            self.line_no += 1

            if line == "" or line.startswith("#"):
                self._handle_blank()
                continue

            if line.startswith("account="):
                self._handle_posting(line)
            else:
                self._handle_key_or_general(line)

        # EOF handling
        if self.state == self.STATE_POSTING:
            self._finalize_annotation()
        elif self.state not in (self.STATE_START, self.STATE_END):
            raise AnnotationParserError(
                f"Unexpected EOF at line {self.line_no}, annotation incomplete."
            )
        return self.annotations

    # -----------------------------
    # State handlers
    # -----------------------------
    def _handle_blank(self):
        if self.state == self.STATE_POSTING:
            self._finalize_annotation()
        # otherwise ignore

    def _handle_key_or_general(self, line: str):
        name, sep, value = line.partition("=")
        if sep != "=":
            raise AnnotationParserError(
                f"Line {self.line_no}: expected 'key=value' format, got: {line}"
            )
        name = name.strip()
        value = value.strip()

        if self.state in (self.STATE_START, self.STATE_END):
            self.current = Annotation()
            self.state = self.STATE_KEY

        # If we’re in KEY state but line is NOT a key field, transition to GENERAL
        if self.state == self.STATE_KEY and name not in {
            "hash",
            "date",
            "amount",
            "type",
            "check",
        }:
            self.state = self.STATE_GENERAL

        # Now delegate based on state
        if self.state == self.STATE_KEY:
            self._parse_key_field(name, value)
            if self._key_complete():
                self.state = self.STATE_GENERAL
        elif self.state == self.STATE_GENERAL:
            self._parse_general_field(name, value)
        elif self.state == self.STATE_POSTING:
            raise AnnotationParserError(
                f"Line {self.line_no}: general field '{name}' after postings have started."
            )

    def _handle_posting(self, line: str):
        if self.state in (self.STATE_START, self.STATE_END):
            raise AnnotationParserError(
                f"Line {self.line_no}: posting encountered before annotation key."
            )
        if self.state in (self.STATE_KEY, self.STATE_GENERAL):
            self.state = self.STATE_POSTING

        posting = self._parse_posting_line(line)
        self.current.postings.append(posting)

    # -----------------------------
    # Field parsing
    # -----------------------------
    def _parse_key_field(self, name: str, value: str):
        if name == "hash":
            if isinstance(self.current.key, PendingKey):
                raise AnnotationParserError(
                    f"Line {self.line_no}: cannot mix reconciled and pending key fields"
                )
            self.current.key = ReconciledKey(hash=value)
        elif name in ("date", "amount", "type", "check"):
            if isinstance(self.current.key, ReconciledKey):
                raise AnnotationParserError(
                    f"Line {self.line_no}: cannot mix reconciled and pending key fields"
                )
            if not isinstance(self.current.key, PendingKey):
                self.current.key = PendingKey()
            pk: PendingKey = self.current.key
            if name == "date":
                pk.date = date.fromisoformat(value)
            elif name == "amount":
                pk.amount = Decimal(value)
            elif name == "type":
                pk.type = value
            elif name == "check":
                pk.serial = value
        else:
            raise AnnotationParserError(
                f"Line {self.line_no}: unknown key field '{name}'"
            )

    def _parse_general_field(self, name: str, value: str):
        if name == "description":
            self.current.description = value
        elif name == "memo":
            self.current.memo = value
        else:
            raise AnnotationParserError(
                f"Line {self.line_no}: unknown general field '{name}'"
            )

    def _parse_posting_line(self, line: str) -> Posting:
        parts = line.split()
        if not parts:
            raise AnnotationParserError(f"Line {self.line_no}: empty posting line.")

        account_field = parts[0]
        if not account_field.startswith("account="):
            raise AnnotationParserError(
                f"Line {self.line_no}: posting line must start with 'account='"
            )
        account = account_field.split("=", 1)[1]

        amount = None
        lot = None
        invoice = None

        for p in parts[1:]:
            if "=" not in p:
                raise AnnotationParserError(
                    f"Line {self.line_no}: invalid posting subfield '{p}'"
                )
            k, v = p.split("=", 1)
            if k == "amount":
                amount = Decimal(v)
            elif k == "lot":
                lot = int(v)
            elif k == "invoice":
                invoice = v
            else:
                raise AnnotationParserError(
                    f"Line {self.line_no}: unknown posting field '{k}'"
                )

        return Posting(account=account, amount=amount, lot=lot, invoice=invoice)

    # -----------------------------
    # Helpers
    # -----------------------------
    def _key_complete(self) -> bool:
        if isinstance(self.current.key, ReconciledKey):
            return self.current.key.hash is not None
        elif isinstance(self.current.key, PendingKey):
            pk: PendingKey = self.current.key
            return pk.date is not None and pk.amount is not None and pk.type is not None
        return False

    def _finalize_annotation(self):
        if not self.current.postings:
            raise AnnotationParserError(
                f"Line {self.line_no}: annotation must have at least one posting."
            )
        self.annotations.append(self.current)
        self.current = None
        self.state = self.STATE_END


#############################################################################
# Annotation Store
# ###########################################################################
class AnnotationStore:
    def __init__(self, path: Path):
        self._items: List[Annotation] = []
        self.path = path


from pathlib import Path
from typing import List


class AnnotationStore:
    def __init__(self, path: Path):
        self.path = path
        self._items: List[Annotation] = []

    @property
    def items(self) -> List[Annotation]:
        return self._items

    def load(self) -> None:
        """Load annotations from the file at self.path using AnnotationParser."""
        if not self.path.exists():
            return

        with self.path.open("r", encoding="utf-8") as f:
            lines = f.readlines()

        parser = AnnotationParser(lines)
        annotations = parser.parse()

        # Optionally: calculate last posting amount remainder here
        for ann in annotations:
            self._fill_posting_remainders(ann)

        self._items = annotations

    def _fill_posting_remainders(self, ann: Annotation):
        """Fill the last posting with remaining amount if needed."""
        if isinstance(ann.key, PendingKey):
            total_amount = ann.key.amount
        elif isinstance(ann.key, ReconciledKey):
            # For reconciled, amount must be determined from the hash
            # or could be supplied in a single posting
            total_amount = None
        else:
            raise ValueError("Unknown key type")

        postings = ann.postings
        if not postings:
            return

        # If total_amount is known, fill the last posting automatically
        if total_amount is not None:
            supplied = sum(p.amount for p in postings if p.amount is not None)
            none_count = sum(1 for p in postings if p.amount is None)
            if none_count > 1:
                raise ValueError(
                    f"Annotation at line {self.path} has more than one posting without amount."
                )
            elif none_count == 1:
                for p in postings:
                    if p.amount is None:
                        p.amount = total_amount - supplied
                        break

    def _resolve_posting_amounts(self, b: AnnotationBuilder):
        total = (
            b.key.amount
            if isinstance(b.key, PendingKey)
            else self._lookup_transaction_amount(b.key.hash)
        )

        specified = sum(p.amount for p in b.postings if p.amount is not None)
        missing = [p for p in b.postings if p.amount is None]

        if len(missing) > 1:
            raise AnnotationParserError("More than one posting is missing an amount.")

        if missing:
            remainder = total - specified
            if remainder < 0:
                raise AnnotationParserError(
                    f"Posting amounts exceed transaction amount {total}."
                )
            missing[0].amount = remainder
        else:
            if specified != total:
                raise AnnotationParserError(
                    f"Postings do not sum to transaction amount {total}."
                )

    def _freeze_annotation(self, b: AnnotationBuilder) -> Annotation:
        return Annotation(
            key=b.key,
            description=b.description,
            memo=b.memo,
            postings=[
                Posting(
                    account=p.account,
                    amount=p.amount,
                    lot=p.lot,
                    invoice=p.invoice,
                )
                for p in b.postings
            ],
        )

    def _lookup_transaction_amount(self, hash_: str) -> Decimal:
        # placeholder: implement lookup if reconciled entries need amount
        raise NotImplementedError

    def match(self, entry: Transaction) -> Annotation | None:
        for item in self._items:
            if item.matches(entry):
                return item
        return None

    def add(self, item: T) -> None:
        self._items.append(item)

    def remove(self, item: T) -> None:
        self._items.remove(item)

    def is_empty(self) -> bool:
        return not self._items

    def save(self) -> None:
        def to_toml(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if hasattr(obj, "__dict__"):
                d = {}
                for k, v in obj.__dict__.items():
                    if k.startswith("_"):
                        continue
                    if k == "postings" and isinstance(v, list):
                        d[k] = [to_toml(p) for p in v]
                    else:
                        d[k] = to_toml(v)
                return d
            if isinstance(obj, list):
                return [to_toml(x) for x in obj]
            return obj

        if self.is_empty():
            if self.path.exists():
                self.path.unlink()
            return

        # make writable before writing
        if self.path.exists():
            self.path.chmod(0o666)

        with open(self.path, "w", encoding="utf-8") as f:
            for ann in self._items:
                if isinstance(ann.key, ReconciledKey):
                    f.write(f"hash={ann.key.hash}\n")
                else:
                    pk: PendingKey = ann.key
                    f.write(f"date={pk.date.isoformat()}\n")
                    f.write(f"amount={pk.amount}\n")
                    if pk.type:
                        f.write(f"type={pk.type}\n")
                    if pk.serial:
                        f.write(f"check={pk.serial}\n")
                if ann.description:
                    f.write(f"description={ann.description}\n")
                if ann.memo:
                    f.write(f"memo={ann.memo}\n")
                for p in ann.postings:
                    line = f"account={p.account}"
                    if p.amount is not None:
                        line += f" amount={p.amount}"
                    if p.lot is not None:
                        line += f" lot={p.lot}"
                    if p.invoice is not None:
                        line += f" invoice={p.invoice}"
                    f.write(line + "\n")
                f.write("\n")  # blank line between annotations

        self.path.chmod(0o444)  # read-only
