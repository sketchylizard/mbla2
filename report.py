#!/usr/bin/env python3
"""
report.py

Generate the MBLA Annual Financial Report as a PDF.

Usage:
    report.py                        # defaults: last meeting 2025-08-17, today
    report.py --from 2025-08-17      # explicit start date
    report.py --to 2026-03-14        # explicit end date
    report.py --year 2025            # calendar-year mode: Jan 1 – Dec 31
    report.py --output report.pdf    # custom output path
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    KeepTogether,
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
)

from hoa import config

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def fmt(amount: Decimal) -> str:
    """Format a Decimal as a dollar amount, e.g. $1,234.56 or ($34.00)"""
    if amount < 0:
        return f"(${abs(amount):,.2f})"
    return f"${amount:,.2f}"


def fmt_date(d: date) -> str:
    return d.strftime("%B %d, %Y")


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------


def get_balance(conn: sqlite3.Connection, account: str, as_of: date) -> Decimal:
    """Sum of all postings for account (LIKE pattern ok) up through as_of."""
    cur = conn.execute(
        """
        SELECT COALESCE(SUM(p.amount), 0)
        FROM posting p
        JOIN journal_entry j ON j.journal_id = p.journal_id
        WHERE p.account LIKE ?
          AND j.posted_date <= ?
        """,
        (account, as_of.isoformat()),
    )
    return Decimal(cur.fetchone()[0]) / 100


def get_activity(
    conn: sqlite3.Connection,
    account_pattern: str,
    start: date,
    end: date,
) -> dict[str, Decimal]:
    """
    Return net activity grouped by account for a date range.
    account_pattern uses SQL LIKE syntax, e.g. 'income:%' or 'expenses:%'.
    Returns a dict of account -> net amount (in dollars), excluding zero-balance accounts.
    """
    cur = conn.execute(
        """
        SELECT p.account, COALESCE(SUM(p.amount), 0) AS net
        FROM posting p
        JOIN journal_entry j ON j.journal_id = p.journal_id
        WHERE p.account LIKE ?
          AND j.posted_date >= ?
          AND j.posted_date <= ?
        GROUP BY p.account
        HAVING net != 0
        ORDER BY p.account
        """,
        (account_pattern, start.isoformat(), end.isoformat()),
    )
    return {row[0]: Decimal(row[1]) / 100 for row in cur.fetchall()}


def get_dues_summary(
    conn: sqlite3.Connection,
    fiscal_year: int,
    as_of: date,
) -> tuple[Decimal, Decimal, Decimal]:
    """
    Returns (billed, collected, outstanding) for the given fiscal year as of as_of.

    Billed:      sum of positive postings on receivables:lotXX with serial-00 invoices
    Collected:   sum of negative postings on the same (payments reduce the receivable)
    Outstanding: billed + collected  (collected is negative, so this is the remainder)
    """
    cur = conn.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN p.amount > 0 THEN p.amount ELSE 0 END), 0) AS billed,
            COALESCE(SUM(CASE WHEN p.amount < 0 THEN p.amount ELSE 0 END), 0) AS collected
        FROM posting p
        JOIN journal_entry j ON j.journal_id = p.journal_id
        WHERE p.account LIKE 'assets:receivables:lot%'
          AND p.invoice LIKE ?
          AND j.posted_date <= ?
        """,
        (f"{fiscal_year}%00", as_of.isoformat()),
    )
    row = cur.fetchone()
    billed = Decimal(row[0]) / 100
    collected = Decimal(row[1]) / 100  # negative number
    outstanding = billed + collected  # what remains
    return billed, abs(collected), outstanding


# ---------------------------------------------------------------------------
# PDF construction helpers
# ---------------------------------------------------------------------------

DARK_BLUE = colors.HexColor("#1a3a5c")
MID_BLUE = colors.HexColor("#2e6da4")
LIGHT_GREY = colors.HexColor("#f2f4f7")
MID_GREY = colors.HexColor("#d0d5dd")
WHITE = colors.white
BLACK = colors.black


def build_styles() -> dict:
    base = getSampleStyleSheet()

    styles = {
        "title": ParagraphStyle(
            "ReportTitle",
            parent=base["Title"],
            fontSize=20,
            textColor=DARK_BLUE,
            spaceAfter=4,
        ),
        "subtitle": ParagraphStyle(
            "ReportSubtitle",
            parent=base["Normal"],
            fontSize=11,
            textColor=MID_BLUE,
            spaceAfter=2,
        ),
        "section": ParagraphStyle(
            "SectionHeading",
            parent=base["Heading2"],
            fontSize=12,
            textColor=DARK_BLUE,
            spaceBefore=16,
            spaceAfter=6,
            borderPad=0,
        ),
        "normal": ParagraphStyle(
            "ReportNormal",
            parent=base["Normal"],
            fontSize=10,
            spaceAfter=4,
        ),
        "note": ParagraphStyle(
            "ReportNote",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#666666"),
            spaceAfter=4,
        ),
    }
    return styles


def section_heading(text: str, styles: dict):
    return [
        Paragraph(text, styles["section"]),
        HRFlowable(width="100%", thickness=1, color=MID_BLUE, spaceAfter=4),
    ]


def balance_table(rows: list[tuple[str, Decimal, Decimal]]) -> Table:
    """
    rows: list of (label, opening_balance, closing_balance)
    """
    header = ["Account", "Opening Balance", "Closing Balance", "Change"]
    data = [header]
    for label, opening, closing in rows:
        change = closing - opening
        data.append([label, fmt(opening), fmt(closing), fmt(change)])

    # Totals row
    total_open = sum(r[1] for r in rows)
    total_close = sum(r[2] for r in rows)
    data.append(
        ["Total", fmt(total_open), fmt(total_close), fmt(total_close - total_open)]
    )

    col_widths = [2.8 * inch, 1.4 * inch, 1.4 * inch, 1.4 * inch]
    t = Table(data, colWidths=col_widths)
    t.setStyle(
        TableStyle(
            [
                # Header
                ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("ALIGN", (1, 0), (-1, 0), "RIGHT"),
                # Body rows
                ("FONTNAME", (0, 1), (-1, -2), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -2), 10),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, LIGHT_GREY]),
                # Totals row
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("BACKGROUND", (0, -1), (-1, -1), LIGHT_GREY),
                ("LINEABOVE", (0, -1), (-1, -1), 1, MID_GREY),
                # Grid
                ("BOX", (0, 0), (-1, -1), 0.5, MID_GREY),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, MID_GREY),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return t


def activity_table(
    activity: dict[str, Decimal],
    label_fn=None,
    negate: bool = False,
) -> Table:
    """
    Render an account -> amount dict as a two-column table.
    label_fn: optional callable to prettify account names.
    negate: if True, flip signs (income accounts are stored negative-of-cash).
    """
    if label_fn is None:
        label_fn = lambda a: a

    data = [["Category", "Amount"]]
    total = Decimal(0)
    for account, amount in sorted(activity.items()):
        display = label_fn(account)
        display_amount = -amount if negate else amount
        data.append([display, fmt(display_amount)])
        total += display_amount

    data.append(["Total", fmt(total)])

    col_widths = [4.5 * inch, 1.5 * inch]
    t = Table(data, colWidths=col_widths)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                ("FONTNAME", (0, 1), (-1, -2), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -2), 10),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, LIGHT_GREY]),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("BACKGROUND", (0, -1), (-1, -1), LIGHT_GREY),
                ("LINEABOVE", (0, -1), (-1, -1), 1, MID_GREY),
                ("BOX", (0, 0), (-1, -1), 0.5, MID_GREY),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, MID_GREY),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return t


def dues_income_table(
    dues_rows: list[tuple[str, Decimal, Decimal, Decimal]],
    other_income: dict[str, Decimal],
) -> Table:
    """
    Income table that shows dues billed with an uncollected offset,
    plus any non-dues income lines, then a grand total.

    dues_rows: list of (label, billed, collected, outstanding)
    other_income: dict of account -> amount (already negated to positive dollars)
    """
    data = [["Category", "Amount"]]

    grand_total = Decimal(0)

    for label, billed, collected, outstanding in dues_rows:
        data.append([label + " – billed", fmt(billed)])
        grand_total += billed
        if outstanding > 0:
            data.append(["  Less: uncollected as of report date", fmt(-outstanding)])
            grand_total -= outstanding

    for account, amount in sorted(other_income.items()):
        data.append([prettify_income(account), fmt(amount)])
        grand_total += amount

    data.append(["Total collected", fmt(grand_total)])

    col_widths = [4.5 * inch, 1.5 * inch]
    t = Table(data, colWidths=col_widths)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                ("FONTNAME", (0, 1), (-1, -2), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -2), 10),
                ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, LIGHT_GREY]),
                # Indent and italicize the "Less:" offset rows
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("BACKGROUND", (0, -1), (-1, -1), LIGHT_GREY),
                ("LINEABOVE", (0, -1), (-1, -1), 1, MID_GREY),
                ("BOX", (0, 0), (-1, -1), 0.5, MID_GREY),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, MID_GREY),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return t


def dues_table(rows: list[tuple[str, Decimal, Decimal, Decimal]]) -> Table:
    """
    rows: list of (fiscal_year_label, billed, collected, outstanding)
    """
    header = ["Fiscal Year", "Billed", "Collected", "Outstanding"]
    data = [header] + [
        [label, fmt(billed), fmt(collected), fmt(outstanding)]
        for label, billed, collected, outstanding in rows
    ]

    col_widths = [2.0 * inch, 1.5 * inch, 1.5 * inch, 1.5 * inch]
    t = Table(data, colWidths=col_widths)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), DARK_BLUE),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 10),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
                ("BOX", (0, 0), (-1, -1), 0.5, MID_GREY),
                ("INNERGRID", (0, 0), (-1, -1), 0.25, MID_GREY),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ]
        )
    )
    return t


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------


def prettify_income(account: str) -> str:
    """'income:dues:2026' -> 'Dues – FY2026'"""
    parts = account.split(":")
    if len(parts) >= 2:
        category = parts[1].replace("_", " ").title()
        if len(parts) >= 3:
            return f"{category} \u2013 FY{parts[2]}"
        return category
    return account


def prettify_expense(account: str) -> str:
    """'expenses:bank fees' -> 'Bank Fees'"""
    parts = account.split(":")
    label = parts[-1].replace("_", " ").replace("-", " ").title()
    return label


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


def build_report(
    db_path: Path,
    period_start: date,
    period_end: date,
    output_path: Path,
) -> None:
    conn = sqlite3.connect(db_path)

    styles = build_styles()
    story = []

    # ---- Header ----
    story.append(Paragraph(config.ASSOCIATION_NAME, styles["title"]))
    story.append(Paragraph("Annual Financial Report", styles["subtitle"]))
    story.append(
        Paragraph(
            f"Period: {fmt_date(period_start)} \u2013 {fmt_date(period_end)}",
            styles["subtitle"],
        )
    )
    story.append(
        Paragraph(
            f"Prepared: {fmt_date(date.today())}",
            styles["note"],
        )
    )
    story.append(Spacer(1, 0.15 * inch))

    # ---- Account Balances ----
    # Opening = balance as of the day before the period starts
    prior_day = date(period_start.year, period_start.month, period_start.day)
    # Use period_start itself as the "opening" snapshot (end of that day)
    # so the report reads: "balance at start of period" vs "balance at end of period"
    opening_date = period_start
    closing_date = period_end

    checking_open = get_balance(conn, "assets:truist:checking", opening_date)
    checking_close = get_balance(conn, "assets:truist:checking", closing_date)
    savings_open = get_balance(conn, "assets:truist:savings", opening_date)
    savings_close = get_balance(conn, "assets:truist:savings", closing_date)

    story.extend(section_heading("Bank Account Balances", styles))
    story.append(
        balance_table(
            [
                ("Truist Checking (x0947)", checking_open, checking_close),
                ("Truist Savings  (x9625)", savings_open, savings_close),
            ]
        )
    )
    story.append(
        Paragraph(
            f"Opening balance as of {fmt_date(opening_date)}. "
            f"Closing balance as of {fmt_date(closing_date)}.",
            styles["note"],
        )
    )

    # ---- Income ----
    # Fetch dues data for all fiscal years in the period
    dues_years = [
        fy
        for fy in sorted({period_start.year, period_end.year})
        if get_activity(conn, f"income:dues:{fy}", period_start, period_end)
    ]
    dues_rows = []
    for fy in dues_years:
        billed, collected, outstanding = get_dues_summary(conn, fy, closing_date)
        if billed > 0:
            dues_rows.append((f"Dues – FY{fy}", billed, collected, outstanding))

    # Non-dues income (interest, etc.)
    income_activity = get_activity(conn, "income:%", period_start, period_end)
    other_income = {
        k: -v for k, v in income_activity.items() if not k.startswith("income:dues")
    }

    if dues_rows or other_income:
        income_block = [
            *section_heading("Income", styles),
            dues_income_table(dues_rows, other_income),
            Paragraph(
                "Dues shown at full billed amount. The uncollected offset reflects amounts "
                f"still outstanding as of {fmt_date(closing_date)}.",
                styles["note"],
            ),
        ]
    else:
        income_block = [
            *section_heading("Income", styles),
            Paragraph("No income recorded for this period.", styles["normal"]),
        ]
    story.append(KeepTogether(income_block))

    # ---- Expenses ----
    expense_activity = get_activity(conn, "expenses:%", period_start, period_end)
    # Filter out internal Venmo/transfer expense placeholders
    expense_activity = {
        k: v for k, v in expense_activity.items() if not k.startswith("expenses:venmo")
    }

    if expense_activity:
        expense_block = [
            *section_heading("Expenses", styles),
            activity_table(expense_activity, label_fn=prettify_expense),
        ]
    else:
        expense_block = [
            *section_heading("Expenses", styles),
            Paragraph("No expenses recorded for this period.", styles["normal"]),
        ]
    story.append(KeepTogether(expense_block))

    # ---- Build PDF ----
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=letter,
        leftMargin=1 * inch,
        rightMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch,
        title=f"{config.ASSOCIATION_NAME} – Annual Report",
        author="MBLA Treasurer",
    )
    doc.build(story)
    conn.close()
    print(f"Report written to {output_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate MBLA annual financial report"
    )
    parser.add_argument(
        "--from",
        dest="start",
        default="2025-08-17",
        help="Period start date (YYYY-MM-DD), default: 2025-08-17",
    )
    parser.add_argument(
        "--to",
        dest="end",
        default=date.today().isoformat(),
        help="Period end date (YYYY-MM-DD), default: today",
    )
    parser.add_argument(
        "--output", "-o", default="mbla_annual_report.pdf", help="Output PDF path"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    period_start = date.fromisoformat(args.start)
    period_end = date.fromisoformat(args.end)
    output_path = Path(args.output)

    print(f"Generating report: {fmt_date(period_start)} – {fmt_date(period_end)}")
    build_report(config.DATABASE, period_start, period_end, output_path)


if __name__ == "__main__":
    main()
