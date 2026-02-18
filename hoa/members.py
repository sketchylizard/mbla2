import yaml
from pathlib import Path
from typing import List
from dataclasses import dataclass


def generate_name_variations(full_name: str) -> List[str]:
    """
    Generate variations of a name by removing middle names/initials.

    Examples:
        "John R. Brading" -> {"John R. Brading", "John Brading"}
        "Kelly Anne Blair" -> {"Kelly Anne Blair", "Kelly Blair"}
        "Jason Stewart" -> {"Jason Stewart"}
    """
    variations = []

    # Add the original name
    variations.append(full_name)

    # Split into parts
    parts = full_name.split()

    if len(parts) <= 2:
        # Already first + last only
        return variations

    # Generate variation with just first and last name
    # (removing all middle names/initials)
    first_last = f"{parts[0]} {parts[-1]}"
    variations.append(first_last)

    return variations


@dataclass
class Lot:
    lot_number: int
    owners: list[str]
    emails: list[str]
    phones: list[str]
    address: list[str]
    mailing: list[str] | None = None
    venmo_names: list[str] = None
    grouped_lots: list[int] | None = None
    hoa_owned: bool = False


class MemberDirectory:
    def __init__(self, yaml_path: Path):
        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        self.lots = {}
        self.name_to_lot = {}  # name (lowercase) -> lot_number

        for lot_num, info in data.items():
            lot = Lot(
                lot_number=int(lot_num),
                owners=info.get("owners", []),
                emails=info.get("emails", []),
                phones=info.get("phones", []),
                address=info.get("address", []),
                mailing=info.get("mailing"),
                grouped_lots=info.get("grouped_lots"),
                hoa_owned=info.get("hoa_owned", False),
                venmo_names=info.get("venmo_names"),
            )
            self.lots[int(lot_num)] = lot

            # Build name lookup index (skip HOA-owned lots)
            if not lot.hoa_owned:
                # Add owner names and variations
                for owner in lot.owners:
                    self.name_to_lot[owner.lower()] = int(lot_num)
                    for variation in generate_name_variations(owner):
                        self.name_to_lot[variation.lower()] = int(lot_num)

                # Add venmo names
                if lot.venmo_names:
                    for vname in lot.venmo_names:
                        self.name_to_lot[vname.lower()] = int(lot_num)

    def find_lot_by_name(self, name: str, exact: bool = True) -> Lot | None:
        """
        Find lot by owner name or Venmo name.

        Args:
            name: Name to search for
            exact: If True, exact match only. If False, partial match allowed.
        """
        if not name:
            return None

        name_lower = name.lower().strip()
        if not name_lower:
            return None

        if exact:
            # O(1) lookup
            lot_num = self.name_to_lot.get(name_lower)
            return self.lots.get(lot_num) if lot_num else None
        else:
            # Partial match - still need to search
            for indexed_name, lot_num in self.name_to_lot.items():
                if name_lower in indexed_name:
                    return self.lots[lot_num]
            return None

    def get_lot(self, lot_number: int) -> Lot | None:
        """Get lot information by number."""
        return self.lots.get(lot_number)

    def get_all_lots_for_billing(self) -> list[int]:
        """Get all lot numbers that should be billed (excludes HOA-owned)."""
        return [num for num, lot in self.lots.items() if not lot.hoa_owned]
