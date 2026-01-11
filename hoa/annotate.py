#!/usr/bin/env python3
from __future__ import annotations

import sys
import json
from pathlib import Path
from typing import Iterable, List, Optional


# ---------------------------------------------------------------------------
# Imports you already have
# ---------------------------------------------------------------------------

# These are assumed based on our discussions.
# Adjust import paths if needed.
from hoa.models import FinancialEvent
from hoa.annotation_store import AnnotationStore, Annotation


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def read_events_from_stream(stream: Iterable[str]) -> List[FinancialEvent]:
    """
    Read FinancialEvents from a text stream.

    Supports:
      - JSON array
      - JSON object per line
    """
    text = "".join(stream).strip()
    if not text:
        return []

    events: List[FinancialEvent] = []

    if text.startswith("["):
        raw = json.loads(text)
        for obj in raw:
            events.append(FinancialEvent.from_dict(obj))
    else:
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            events.append(FinancialEvent.from_dict(obj))

    return events


def read_events_from_files(paths: List[str]) -> List[FinancialEvent]:
    events: List[FinancialEvent] = []

    for path in paths:
        if path == "-":
            events.extend(read_events_from_stream(sys.stdin))
        else:
            with open(path, "r", encoding="utf-8") as f:
                events.extend(read_events_from_stream(f))

    return events


def apply_annotation(event: FinancialEvent, annotation: Annotation) -> FinancialEvent:

    return event.with_updates(
        type=annotation.type,
        from_account=annotation.from_account,
        to_account=annotation.to_account,
        reference=annotation.reference,
        description=annotation.description,
    )


# ---------------------------------------------------------------------------
# Annotation logic
# ---------------------------------------------------------------------------


def annotate(events: List[FinancialEvent]) -> List[FinancialEvent]:
    """
    Apply reconciled and pending annotations to events.

    This function is:
      - Stateful (annotation stores)
      - Order-sensitive
      - Single-pass streaming after sort
    """

    if not events:
        return []

    # Sort by source_file to guarantee grouping
    events = sorted(events, key=lambda e: e.source_file or "")

    annotated: List[FinancialEvent] = []

    pending_bank: Optional[str] = None
    pending_store: Optional[AnnotationStore] = None

    reconciled_file: Optional[str] = None
    reconciled_store: Optional[AnnotationStore] = None

    def flush_pending():
        nonlocal pending_store
        if pending_store is not None:
            pending_store.save()
            pending_store = None

    def flush_reconciled():
        nonlocal reconciled_store
        if reconciled_store is not None:
            reconciled_store.save()
            reconciled_store = None

    for event in events:
        src = Path(event.source_file)
        # --- Bank boundary (pending annotations) ----------------------------
        bank = src.parts[2]  # assumed property derived from source_file
        if bank != pending_bank:
            flush_pending()
            pending_bank = bank
            pending_store = AnnotationStore(src.with_name("pending.ann"))
            pending_store.load()

        # --- Source file boundary (reconciled annotations) ------------------
        if src != reconciled_file:
            flush_reconciled()
            reconciled_file = src
            reconciled_store = AnnotationStore(src.with_suffix(".ann"))
            reconciled_store.load()

        # --- 1. Reconciled annotations -------------------------------------
        match = reconciled_store.match(event) if reconciled_store else None
        if match:
            event = event.apply_annotation(match)
            event = event.mark_reconciled()
            annotated.append(event)
            continue

        # --- 2. Pending annotations ----------------------------------------
        match = pending_store.match(event) if pending_store else None
        if match:
            pending_store.remove(match)
            reconciled_rule = match.resolve(event.hash())
            reconciled_store.add(reconciled_rule)

            event = event.apply_annotation(reconciled_rule)
            event = event.mark_reconciled()
            annotated.append(event)
            continue

        # --- 3. No annotation ----------------------------------------------
        annotated.append(event)

    # Flush remaining stores
    flush_pending()
    flush_reconciled()

    return annotated


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: List[str]) -> int:
    if not argv:
        # No args means stdin
        events = read_events_from_stream(sys.stdin)
    else:
        events = read_events_from_files(argv)

    annotated = annotate(events)

    FinancialEvent.write_ndjson(annotated, sys.stdout)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
