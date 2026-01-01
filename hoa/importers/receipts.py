from pathlib import Path
from hoa.journal import Journal


def import_file(abs_path: Path, rel_path: Path, journal: Journal) -> None:
    """
    Placeholder importer for receipt entries.
    Currently not implemented.

    Args:
        abs_path: Absolute path to the source file
        rel_path: Path relative to config.SOURCES (for Source.file)
        journal: Journal object to add entries to
    """
    print(f"Error: Manual importer is not implemented yet for file: {rel_path}")
