from datetime import date

from hoa import config
from hoa.journal import Journal, Posting, JournalEntry
from hoa.members import MemberDirectory
from hoa.models import Invoice, Source, Transaction, TxType


def bill_dues(journal: Journal, directory: MemberDirectory, fiscal_year: int) -> None:
    billable_lots = directory.get_all_lots_for_billing()

    # January 1st of the fiscal year as the posting date
    posted_date = date(fiscal_year, 1, 1)

    amount = config.ANNUAL_DUES
    income_account = f"income:dues:{fiscal_year}"

    source = Source(file="billing.py", line=0)

    added = 0
    skipped = 0

    for lot_num in sorted(billable_lots):
        invoice = Invoice.create(year=fiscal_year, lot=lot_num, serial=0)

        postings = [
            Posting(
                account=f"assets:receivables:lot{lot_num:02}",
                amount=amount,
                invoice=invoice,
                lot=lot_num,
            ),
            Posting(
                account=income_account,
                amount=-amount,
                invoice=invoice,
                lot=lot_num,
            ),
        ]

        entry = JournalEntry(
            posted_date=posted_date,
            description=f"Dues billing {fiscal_year} - Lot {lot_num}",
            type=TxType.manual,
            memo=None,
            reference=str(invoice),
            amount=amount,
            source=source,
            postings=postings,
        )

        journal_id = journal.add_entry(entry)
        if journal_id:
            added += 1
        else:
            skipped += 1

    print(f"Dues billing {fiscal_year}: {added} entries added, {skipped} skipped.")


def get_billing_years() -> list[int]:
    today = date.today()
    # If we're in November or December, generate next year's dues too
    end_year = today.year + 1 if today.month >= 12 else today.year
    return list(range(config.START_YEAR, end_year + 1))


def process() -> list[Transaction]:
    billing_years = get_billing_years()

    lots = MemberDirectory(config.DIRECTORY).get_all_lots_for_billing()

    events: list[Transaction] = []

    for year in billing_years:
        total_dues = config.DUES[year] * len(lots)

        postings = [
            Posting(
                account=f"income:dues:{year}",
                amount=-total_dues,
                invoice=None,
                reference=None,
            )
        ]

        for lot_num in lots:
            invoice = Invoice.create(year=year, lot=lot_num, serial=0)
            postings.append(
                Posting(
                    account=f"assets:receivables:lot{lot_num:02}",
                    amount=config.DUES[year],
                    invoice=invoice,
                    reference=None,
                )
            )

        transaction = Transaction(
            posted_date=date(year, 1, 1),
            amount=total_dues,
            bank="billing",
            description=f"Dues billing {year}",
            memo=None,
            from_account="income:dues",
            to_account=None,
            type=TxType.manual,
            reference=None,
            postings=postings,
            source=Source(file="billing.py", line=0),
        )
        events.append(transaction)

    return events
