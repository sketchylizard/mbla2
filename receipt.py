#!/usr/bin/env python3
"""
receipt.py - Generate PDF receipts for HOA dues invoices.

Usage:
    receipt.py <invoice>           # e.g. receipt.py 20261400
    receipt.py --lot <lot> [--year <year>]
    receipt.py --all [--year <year>]   # generate for all lots that have any payment

The receipt shows:
  - Association name / header
  - Lot number, owner names, invoice number
  - Charge line (dues amount, dated Jan 1)
  - Payment lines (date, method, reference, amount)
  - Running balance / amount due
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)

# ---------------------------------------------------------------------------
# Project imports — assumes receipt.py lives at project root
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
from hoa import config
from hoa.members import MemberDirectory
from hoa.models import Invoice


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ReceiptLine:
    posted_date: date
    description: str  # e.g. "Annual dues 2026" or "Payment — Check #1042"
    method: str  # "Dues charge", "Check", "Venmo", "ACH", etc.
    reference: str | None  # check number, Venmo ID, etc.
    amount_cents: int  # positive = charge, negative = payment


@dataclass
class ReceiptData:
    invoice: Invoice
    lot_number: int
    owners: List[str]
    lines: List[ReceiptLine]

    @property
    def total_charged(self) -> int:
        return sum(l.amount_cents for l in self.lines if l.amount_cents > 0)

    @property
    def total_paid(self) -> int:
        return abs(sum(l.amount_cents for l in self.lines if l.amount_cents < 0))

    @property
    def balance_due(self) -> int:
        return sum(l.amount_cents for l in self.lines)


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------


def _fmt_cents(cents: int) -> str:
    return f"${abs(cents) / 100:,.2f}"


def load_receipt_data(
    db_path: Path, invoice_str: str, directory: MemberDirectory
) -> ReceiptData | None:
    """
    Load all journal entries that touch the given invoice number.

    The billing entry (type='manual', from billing.py) contributes the charge line.
    Payment entries contribute negative postings (payments reduce the receivable).
    """
    inv = Invoice(invoice_str)
    lot_obj = directory.get_lot(inv.lot)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Fetch all postings for this invoice, joined to their journal entry
    rows = conn.execute(
        """
        SELECT
            j.journal_id,
            j.posted_date,
            j.type,
            j.description,
            j.memo,
            j.reference,
            p.account,
            p.amount,
            p.reference AS posting_reference
        FROM posting p
        JOIN journal_entry j ON j.journal_id = p.journal_id
        WHERE p.invoice = ?
          AND p.account LIKE 'assets:receivables:%'
        ORDER BY j.posted_date, j.journal_id
        """,
        (invoice_str,),
    ).fetchall()

    conn.close()

    if not rows:
        return None

    lines: List[ReceiptLine] = []

    for row in rows:
        jtype = row["type"]
        amount_cents = row["amount"]  # already in cents (stored as integer)
        posted_date = date.fromisoformat(row["posted_date"])
        description = row["description"] or ""
        memo = row["memo"] or ""
        entry_reference = row["reference"]
        posting_reference = row["posting_reference"]

        # --- Determine method and description ---
        if jtype == "manual":
            # This is the billing charge
            method = "Dues charge"
            display_desc = f"Annual dues {inv.year}"
            ref_display = None
        elif entry_reference and entry_reference.startswith("chk-"):
            # Truist bank-issued check reference like "chk-2025-042"
            method = "Check"
            ref_display = entry_reference
            display_desc = f"Payment by check"
        elif posting_reference:
            method = "Check"
            num = posting_reference.lstrip("#").strip()
            ref_display = f"Check #{num}"
            display_desc = "Payment by check"
        elif entry_reference and entry_reference.isdigit() and len(entry_reference) < 6:
            # Simple check number
            method = "Check"
            ref_display = f"Check #{entry_reference}"
            display_desc = f"Payment by check"
        elif jtype == "deposit":
            # Multi-check deposit — reference is like dep-2026-01
            method = "Check"
            ref_display = entry_reference
            # description might be "Deposit from FirstName LastName"
            display_desc = description or "Payment by check"
        elif jtype == "credit" and memo:
            method = "Venmo"
            ref_display = None
            display_desc = "Payment via Venmo"
        elif jtype in ("credit", "debit"):
            method = "Payment"
            ref_display = entry_reference
            display_desc = description or "Payment"
        else:
            method = jtype.capitalize()
            ref_display = entry_reference
            display_desc = description or method

        lines.append(
            ReceiptLine(
                posted_date=posted_date,
                description=display_desc,
                method=method,
                reference=ref_display,
                amount_cents=amount_cents,
            )
        )

    owners = lot_obj.owners if lot_obj else ["Unknown"]
    return ReceiptData(invoice=inv, lot_number=inv.lot, owners=owners, lines=lines)


# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------


def build_pdf(receipt: ReceiptData, output_path: Path) -> None:
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=letter,
        leftMargin=1 * inch,
        rightMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch,
    )

    styles = getSampleStyleSheet()

    header_style = ParagraphStyle(
        "HOAHeader",
        parent=styles["Title"],
        fontSize=16,
        spaceAfter=4,
        textColor=colors.HexColor("#1a3a5c"),
    )
    subheader_style = ParagraphStyle(
        "HOASubheader",
        parent=styles["Normal"],
        fontSize=10,
        spaceAfter=2,
        textColor=colors.HexColor("#444444"),
        alignment=1,  # centered
    )
    label_style = ParagraphStyle(
        "Label",
        parent=styles["Normal"],
        fontSize=9,
        textColor=colors.HexColor("#666666"),
    )
    value_style = ParagraphStyle(
        "Value",
        parent=styles["Normal"],
        fontSize=11,
        fontName="Helvetica-Bold",
    )
    body_style = styles["Normal"]

    story = []

    # --- Header ---
    story.append(Paragraph(config.ASSOCIATION_NAME, header_style))
    story.append(Paragraph("Dues Receipt", subheader_style))
    story.append(Spacer(1, 0.15 * inch))
    story.append(
        HRFlowable(width="100%", thickness=1.5, color=colors.HexColor("#1a3a5c"))
    )
    story.append(Spacer(1, 0.15 * inch))

    # --- Lot / Owner / Invoice info as a two-column mini-table ---
    owners_str = "<br/>".join(receipt.owners)
    info_data = [
        [
            Paragraph("<b>Lot</b>", body_style),
            Paragraph(f"{receipt.lot_number}", value_style),
            Paragraph("<b>Invoice</b>", body_style),
            Paragraph(str(receipt.invoice), value_style),
        ],
        [
            Paragraph("<b>Owner(s)</b>", body_style),
            Paragraph(owners_str, body_style),
            Paragraph("<b>Fiscal year</b>", body_style),
            Paragraph(str(receipt.invoice.year), body_style),
        ],
        [
            Paragraph("<b>Date issued</b>", body_style),
            Paragraph(f"January 1, {receipt.invoice.year}", body_style),
            Paragraph("", body_style),
            Paragraph("", body_style),
        ],
    ]

    info_table = Table(
        info_data, colWidths=[1.1 * inch, 2.3 * inch, 1.1 * inch, 2.0 * inch]
    )
    info_table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(info_table)
    story.append(Spacer(1, 0.2 * inch))
    story.append(
        HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc"))
    )
    story.append(Spacer(1, 0.15 * inch))

    # --- Transactions table ---
    col_headers = ["Date", "Description", "Method", "Reference", "Amount"]
    table_data = [col_headers]

    running_balance = 0
    for line in receipt.lines:
        running_balance += line.amount_cents
        sign = "+" if line.amount_cents > 0 else "-"
        amount_str = f"{sign}{_fmt_cents(line.amount_cents)}"

        table_data.append(
            [
                line.posted_date.strftime("%m/%d/%Y"),
                line.description,
                line.method,
                line.reference or "—",
                amount_str,
            ]
        )

    col_widths = [0.9 * inch, 2.6 * inch, 0.9 * inch, 1.1 * inch, 0.9 * inch]
    tx_table = Table(table_data, colWidths=col_widths, repeatRows=1)

    tx_table.setStyle(
        TableStyle(
            [
                # Header row
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a3a5c")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                # Data rows
                ("FONTSIZE", (0, 1), (-1, -1), 9),
                (
                    "ROWBACKGROUNDS",
                    (0, 1),
                    (-1, -1),
                    [colors.white, colors.HexColor("#f5f7fa")],
                ),
                ("ALIGN", (4, 1), (4, -1), "RIGHT"),  # amount column right-aligned
                ("ALIGN", (0, 1), (0, -1), "CENTER"),  # date centered
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
            ]
        )
    )

    story.append(tx_table)
    story.append(Spacer(1, 0.2 * inch))

    # --- Summary box ---
    balance_color = (
        colors.HexColor("#c0392b")
        if receipt.balance_due > 0
        else colors.HexColor("#27ae60")
    )
    balance_label = "Amount due" if receipt.balance_due > 0 else "Paid in full"

    summary_data = [
        ["Total charged:", _fmt_cents(receipt.total_charged)],
        ["Total paid:", f"({_fmt_cents(receipt.total_paid)})"],
        [f"{balance_label}:", _fmt_cents(receipt.balance_due)],
    ]

    summary_table = Table(summary_data, colWidths=[5.3 * inch, 1.1 * inch])
    summary_table.setStyle(
        TableStyle(
            [
                ("ALIGN", (1, 0), (1, -1), "RIGHT"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                # Last row bold + colored
                ("FONTNAME", (0, 2), (-1, 2), "Helvetica-Bold"),
                ("TEXTCOLOR", (1, 2), (1, 2), balance_color),
                ("LINEABOVE", (0, 2), (-1, 2), 0.75, colors.HexColor("#aaaaaa")),
            ]
        )
    )
    story.append(summary_table)

    # --- Footer ---
    story.append(Spacer(1, 0.3 * inch))
    story.append(
        HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc"))
    )
    story.append(Spacer(1, 0.1 * inch))

    generated_on = date.today().strftime("%B %d, %Y")
    story.append(
        Paragraph(
            f"<i>Generated {generated_on}. Questions? Contact your HOA treasurer.</i>",
            ParagraphStyle(
                "Footer",
                parent=styles["Normal"],
                fontSize=8,
                textColor=colors.HexColor("#888888"),
                alignment=1,
            ),
        )
    )

    doc.build(story)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def invoices_for_lot(lot_num: int, year: int) -> list[str]:
    return [str(Invoice.create(year=year, lot=lot_num, serial=0))]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate HOA dues receipts")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("invoice", nargs="?", help="Invoice number (e.g. 20261400)")
    group.add_argument("--lot", type=int, help="Lot number")
    group.add_argument(
        "--all",
        action="store_true",
        dest="all_lots",
        help="Generate receipts for all lots with activity",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=date.today().year,
        help="Fiscal year (default: current year)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("receipts"),
        help="Output directory (default: ./receipts/)",
    )
    args = parser.parse_args()

    directory = MemberDirectory(config.DIRECTORY)

    if args.invoice:
        invoice_strs = [args.invoice]
    elif args.lot:
        invoice_strs = invoices_for_lot(args.lot, args.year)
    elif args.all_lots:
        lots = directory.get_all_lots_for_billing()
        invoice_strs = [
            str(Invoice.create(year=args.year, lot=lot_num, serial=0))
            for lot_num in sorted(lots)
        ]
    else:
        parser.print_help()
        return 1

    args.out.mkdir(parents=True, exist_ok=True)

    ok = err = skipped = 0
    for inv_str in invoice_strs:
        try:
            receipt = load_receipt_data(config.DATABASE, inv_str, directory)
            if receipt is None:
                print(f"  SKIP  {inv_str} — no data in database")
                skipped += 1
                continue

            out_file = args.out / f"receipt_{inv_str}.pdf"
            build_pdf(receipt, out_file)

            status = (
                "PAID"
                if receipt.balance_due == 0
                else f"DUE ${receipt.balance_due / 100:.2f}"
            )
            print(f"  {status:12s}  {inv_str}  → {out_file}")
            ok += 1
        except Exception as e:
            print(f"  ERROR  {inv_str}: {e}", file=sys.stderr)
            err += 1

    print(f"\n{ok} generated, {skipped} skipped (no payments), {err} errors.")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
