from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Dict, Self
import toml

from hoa.journal import Posting  # assuming this exists
from hoa.journal import Transaction  # for entry.hash()


# -----------------------------
# Data classes for annotations
# -----------------------------
@dataclass
class ResolvedAnnotation:
    hash: str
    description: Optional[str] = None
    memo: Optional[str] = None
    postings: List[Posting] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        postings = []
        for p in data.get("postings", []):
            postings.append(Posting.from_annotation_dict(p))
        description = data.get("description")
        if description is None or description == "":
            raise ValueError("ResolvedAnnotation must have a description")

        return cls(
            hash=data["hash"],
            description=description,
            memo=data.get("memo"),
            postings=postings,
        )


@dataclass
class PendingAnnotation:
    date: Optional[date] = None
    check: Optional[int] = None  # check/serial number
    amount: Optional[Decimal] = None
    description: Optional[str] = None
    memo: Optional[str] = None
    postings: List[Posting] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        postings = [Posting.from_annotation_dict(p) for p in data.get("postings", [])]
        description = data.get("description")
        if description is None or description == "":
            raise ValueError("ResolvedAnnotation must have a description")

        return cls(
            date=data.get("date"),
            check=data.get("check"),
            amount=Decimal(data["amount"]) if data.get("amount") is not None else None,
            description=description,
            memo=data.get("memo"),
            postings=postings,
        )

    def resolve(self, entry_hash: str) -> ResolvedAnnotation:
        """Convert pending to resolved using the journal entry hash."""
        return ResolvedAnnotation(
            hash=entry_hash,
            description=self.description,
            memo=self.memo,
            postings=self.postings,
        )

    def matches(self, entry: Transaction) -> bool:
        """Check if this pending annotation matches the given journal entry."""
        if self.date and self.date != entry.posted_date:
            return False
        if self.check and self.check != entry.serial:
            return False
        if self.amount and self.amount != entry.amount:
            return False
        if (
            self.description
            and self.description.lower() not in entry.description.lower()
        ):
            return False
        return True


def _validate_amount(amount: Decimal, postings: tuple[Posting, ...]) -> None:
    total = sum(p.amount for p in postings)
    if total != amount:
        raise ValueError(
            f"posting total {total} does not match expected amount {amount}"
        )


# -----------------------------
# AnnotationStore
# -----------------------------
class AnnotationStore:
    def __init__(
        self,
        pending: Optional[List[PendingAnnotation]],
        path: Path,
    ):
        self.pending: List[PendingAnnotation] = pending if pending else []
        self.pending_path = path
        self.resolved = {}
        self.resolved_path = None

    # -----------------------------
    # Loading / Saving TOML
    # -----------------------------
    @classmethod
    def load(cls, path: Path) -> Self:
        if not path.exists():
            return cls()

        data = toml.load(path)

        pending = [PendingAnnotation.from_dict(d) for d in data.get("pending", [])]
        return cls(pending=pending, path=path)

    def load_resolved(self, path: Path) -> None:
        self.resolved = {}
        self.resolved_path = path

        if not path or not path.exists():
            return

        with path.open("r") as f:
            data = toml.load(f)
            for d in data.get("resolved", []):
                r = ResolvedAnnotation.from_dict(d)
                # Do NOT overwrite existing hashes
                self.resolved.setdefault(r.hash, r)

    def save_pending(self):
        if not self.pending_path:
            return

        data = {
            "pending": [self._pending_to_dict(p) for p in self.pending],
        }
        with self.pending_path.open("w") as f:
            toml.dump(data, f)

    def save_resolved(self):
        if not self.resolved_path:
            return

        data = {
            "resolved": [self._resolved_to_dict(r) for r in self.resolved.values()],
        }
        with self.resolved_path.open("w") as f:
            toml.dump(data, f)

        self.resolved = {}

    def _pending_to_dict(self, p: PendingAnnotation) -> dict:
        return {
            k: getattr(p, k)
            for k in ["date", "check", "amount", "description", "memo"]
            if getattr(p, k) is not None
        } | {"postings": [vars(post) for post in p.postings]}

    def _resolved_to_dict(self, r: ResolvedAnnotation) -> dict:
        return {
            "hash": r.hash,
            "description": r.description,
            "memo": r.memo,
            "postings": [vars(post) for post in r.postings],
        }

    # -----------------------------
    # Matching logic
    # -----------------------------
    def match(self, entry: Transaction) -> ResolvedAnnotation | None:
        # 1. Exact hash match
        resolved = self.resolved.get(entry.hash())
        if resolved:
            return resolved

        # 2. Pending heuristic match
        for pending in self.pending:
            if pending.matches(entry):
                resolved = pending.resolve(entry.hash())
                self.resolved[resolved.hash] = resolved
                self.pending.remove(pending)
                return resolved

        return None

    def add_resolved(self, resolved: ResolvedAnnotation) -> None:
        self.resolved[resolved.hash] = resolved
