"""Special-case ID translation routing.

Handles ID types that need non-standard translation paths:
- Gene symbols: case normalization, synonym lookup
- RefSeq: version number handling
- Chain translation: via intermediate ID type
"""

from __future__ import annotations

import logging

_log = logging.getLogger(__name__)


def map_genesymbol_fallbacks(
    name: str,
    target_id_type: str,
    ncbi_tax_id: int,
    mapper,
    strict: bool = False,
) -> set[str]:
    """Try various gene symbol normalization strategies.

    Order of attempts:
    1. Direct lookup (exact match)
    2. UPPER case
    3. Capitalized (for rodent symbols like Trp53)
    4. Synonym lookup (genesymbol-syn)
    5. If not strict: first 5 chars, append "1"
    """

    # 1. Direct lookup (already tried by caller)

    # 2. Try uppercase
    result = mapper._direct_lookup(name.upper(), "genesymbol", target_id_type, ncbi_tax_id)
    if result:
        return result

    # 3. Try capitalized (first letter upper, rest lower)
    result = mapper._direct_lookup(name.capitalize(), "genesymbol", target_id_type, ncbi_tax_id)
    if result:
        return result

    # 4. Try gene symbol synonyms
    result = mapper._direct_lookup(name, "genesymbol-syn", target_id_type, ncbi_tax_id)
    if result:
        return result
    result = mapper._direct_lookup(name.upper(), "genesymbol-syn", target_id_type, ncbi_tax_id)
    if result:
        return result

    if strict:
        return set()

    # 5. Try with "1" appended (for isoform suffixes)
    result = mapper._direct_lookup(f"{name}1", "genesymbol", target_id_type, ncbi_tax_id)
    if result:
        return result

    # 6. Try first 5 characters
    if len(name) > 5:
        result = mapper._direct_lookup(name[:5], "genesymbol", target_id_type, ncbi_tax_id)
        if result:
            return result

    return set()


def map_refseq(
    name: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int,
    mapper,
    strict: bool = False,
) -> set[str]:
    """Handle RefSeq IDs which may have version suffixes (.1, .2, etc.)."""

    # Try exact match first
    result = mapper._direct_lookup(name, id_type, target_id_type, ncbi_tax_id)
    if result:
        return result

    # Try without version number
    if "." in name:
        base = name.rsplit(".", 1)[0]
        result = mapper._direct_lookup(base, id_type, target_id_type, ncbi_tax_id)
        if result:
            return result

    if strict:
        return set()

    # Try common version numbers
    base = name.split(".")[0] if "." in name else name
    for ver in range(1, 20):
        result = mapper._direct_lookup(f"{base}.{ver}", id_type, target_id_type, ncbi_tax_id)
        if result:
            return result

    return set()


def map_ensembl_strip_version(
    name: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int,
    mapper,
) -> set[str]:
    """Ensembl IDs sometimes have version suffixes (.1, .2, etc.)."""

    if "." in name:
        base = name.rsplit(".", 1)[0]
        return mapper._direct_lookup(base, id_type, target_id_type, ncbi_tax_id)

    return set()


def chain_map(
    name: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int,
    mapper,
    via: str = "uniprot",
) -> set[str]:
    """Two-step translation: source → intermediate → target."""

    intermediates = mapper.map_name(name, id_type, via, ncbi_tax_id)

    if not intermediates:
        return set()

    result = set()
    for intermediate in intermediates:
        targets = mapper.map_name(intermediate, via, target_id_type, ncbi_tax_id)
        result.update(targets)

    return result
