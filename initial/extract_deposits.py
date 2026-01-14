csv_path = "/home/jason/projects/mbla/initial/initial_import.csv"
csv_path = "/home/jason/projects/mbla/initial/initial_import.csv"
import csv
import re
import yaml
from collections import defaultdict

csv_path = "/home/jason/projects/mbla/initial/initial_import.csv"

deposits_by_date = defaultdict(list)

invoice_re = re.compile(r"Invoice #(\d{4}-\d{2}00)")
# Find all references: check numbers, venmo, cash
ref_all_re = re.compile(r"(#\d+|venmo|cash)", re.IGNORECASE)

with open(csv_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        inflow = row["Inflow"].replace("$", "").replace(",", "")
        payee = row.get("Payee", "")
        memo = row.get("Memo", "")
        account = row.get("Account", "")
        try:
            if (
                account == "Checking"
                and float(inflow) > 0
                and "Invoice #" in memo
                and "venmo" not in memo.lower()
            ):
                invoice_match = invoice_re.search(memo)
                invoice = invoice_match.group(1) if invoice_match else ""
                # Find all references (check numbers, venmo, cash)
                refs = ref_all_re.findall(memo)
                # The first '#' is part of the invoice, so skip it
                check = ""
                if refs:
                    # If there are multiple, use the second one for check number
                    checks_only = [r for r in refs if r.startswith("#")]
                    if len(checks_only) > 1:
                        check = checks_only[1]
                    elif len(checks_only) == 1 and (len(refs) > 1):
                        # If only one check and another ref (venmo/cash), use that
                        check = refs[1]
                    elif len(refs) == 1 and refs[0] in ("venmo", "cash"):
                        check = refs[0]
                # Remove leading '#' from check number if present
                check_clean = check[1:] if check.startswith("#") else check
                invoice_clean = invoice.replace("-", "")
                check_entry = {
                    "check": check_clean,
                    "name": payee,
                    "amount": float(inflow),
                    "invoice": invoice_clean,
                }
                # Convert date to YYYY-MM-DD format
                import datetime

                date_raw = row.get("Date", "")
                date = date_raw
                try:
                    # Try parsing MM/DD/YYYY
                    date_obj = datetime.datetime.strptime(date_raw, "%m/%d/%Y")
                    date = date_obj.strftime("%Y-%m-%d")
                except Exception:
                    pass
                deposits_by_date[date].append(check_entry)
        except ValueError:
            continue


# Custom YAML output formatting
import sys

print("deposits:")
for date, checks in sorted(deposits_by_date.items()):
    print("")  # blank line before each date
    print(f"- date: {date}")
    print("  checks:")
    for check in checks:
        # Format: - {check: #403, name: Nathalie Worthington, invoice: 2024-2000, amount: 150.00}
        check_str = f"{{check: {check['check']}, name: {check['name']}, invoice: {check['invoice']}, amount: {check['amount']:.2f}}}"
        print(f"    - {check_str}")
