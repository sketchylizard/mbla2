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
    grouped_lots: list[int] | None = None
    hoa_owned: bool = False


class MemberDirectory:
    def __init__(self, yaml_path: Path):
        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        self.lots = {}
        for lot_num, info in data.items():
            self.lots[int(lot_num)] = Lot(
                lot_number=int(lot_num),
                owners=info.get("owners", []),
                emails=info.get("emails", []),
                phones=info.get("phones", []),
                address=info.get("address", []),
                mailing=info.get("mailing"),
                grouped_lots=info.get("grouped_lots"),
                hoa_owned=info.get("hoa_owned", False),
            )

    def find_lot_by_name(self, name: str, exact: bool = False) -> int | None:
        """
        Find lot number by owner name.

        Args:
            name: Name to search for
            exact: If True, require exact match. If False, allow partial matches.

        Examples:
            find_lot_by_name("John Brading") -> 6 (exact match with variation)
            find_lot_by_name("Brading") -> 6 (partial match)
            find_lot_by_name("John R Brading", exact=True) -> 6
        """
        name_lower = name.lower()

        for lot_num, lot in self.lots.items():
            if lot.hoa_owned:
                continue

            for owner in lot.owners:
                if exact:
                    # Check exact match against original and variations
                    if name_lower == owner.lower():
                        return lot_num
                    for variation in generate_name_variations(owner):
                        if name_lower == variation.lower():
                            return lot_num
                else:
                    # Partial match - check if search term appears in any variation
                    if name_lower in owner.lower():
                        return lot_num
                    for variation in generate_name_variations(owner):
                        if name_lower in variation.lower():
                            return lot_num

        return None

    def get_lot(self, lot_number: int) -> Lot | None:
        """Get lot information by number."""
        return self.lots.get(lot_number)

    def get_all_lots_for_billing(self) -> list[int]:
        """Get all lot numbers that should be billed (excludes HOA-owned)."""
        return [num for num, lot in self.lots.items() if not lot.hoa_owned]
