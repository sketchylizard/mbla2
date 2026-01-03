from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List, Self, Generic, TypeVar, Iterable
import toml

from hoa.journal import Posting  # assuming this exists
from hoa.journal import Transaction  # for entry.hash()

T = TypeVar("T")


# -----------------------------
# Data classes for annotations
# -----------------------------
@dataclass
class ResolvedAnnotation:
    hash: str
    description: str | None = None
    memo: str | None = None
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

    def matches(self, entry: Transaction) -> bool:
        """Check if this resolved annotation matches the given journal entry."""
        return self.hash == entry.hash()


@dataclass
class PendingAnnotation:
    date: date | None = None
    check: int | None = None  # check/serial number
    amount: Decimal | None = None
    description: str | None = None
    memo: str | None = None
    postings: List[Posting] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        postings = []
        for p in data.get("postings", []):
            if not isinstance(p, dict):
                raise TypeError(
                    f"Each posting must be a dict, got {type(p).__name__}: {p!r}"
                )
            postings.append(Posting.from_annotation_dict(p))
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
        if self.amount and self.amount != abs(entry.amount):
            return False
        return True


def _validate_amount(amount: Decimal, postings: tuple[Posting, ...]) -> None:
    total = sum(p.amount for p in postings)
    if total != amount:
        raise ValueError(
            f"posting total {total} does not match expected amount {amount}"
        )


class AnnotationStore(Generic[T]):
    def __init__(self, path: Path, type: str):
        self._items = []
        self.path = path
        self.type = type

    def load(self) -> None:
        if not self.path.exists():
            return

        data = toml.load(self.path)

        self._items = []

        for d in data.get(self.type, []):
            self._items.append(PendingAnnotation.from_dict(d))

    def __iter__(self):
        return iter(self._items)

    def match(self, entry: Transaction) -> T | None:
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

        data = {self.type: [to_toml(item) for item in self._items]}
        with open(self.path, "w", encoding="utf-8") as f:
            toml.dump(data, f)
