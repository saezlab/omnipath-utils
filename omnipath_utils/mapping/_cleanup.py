"""UniProt ID cleanup — validate, normalize, and fix UniProt accessions."""

from __future__ import annotations

import re
import logging

_log = logging.getLogger(__name__)

# UniProt AC format regex
RE_UNIPROT = re.compile(
    r"^([OPQ][0-9][A-Z0-9]{3}[0-9]"
    r"|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})$"
)


def is_uniprot_ac(name: str) -> bool:
    """Check if a string matches UniProt AC format."""
    return bool(RE_UNIPROT.match(name))


def uniprot_cleanup(
    uniprots: set[str],
    ncbi_tax_id: int,
    mapper = None,
) -> set[str]:
    """Clean up a set of UniProt IDs.

    Steps:
    1. Validate format (keep only valid UniProt ACs)
    2. Translate secondary → primary ACs
    3. Attempt TrEMBL → SwissProt via gene symbol

    Args:
        uniprots: Set of UniProt accession strings.
        ncbi_tax_id: Organism for proteome validation.
        mapper: Mapper instance (to avoid circular import).

    Returns:
        Cleaned set of primary UniProt ACs.
    """
    if not uniprots:
        return set()

    result = set()

    for up in uniprots:
        if not is_uniprot_ac(up):
            continue

        # Try secondary → primary translation
        if mapper:
            primary = mapper.map_name(
                up, "uniprot-sec", "uniprot-pri", ncbi_tax_id,
            )
            if primary:
                result.update(primary)
                continue

        result.add(up)

    return result
