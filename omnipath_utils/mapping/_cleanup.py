"""UniProt ID cleanup — validate, normalize, prefer SwissProt."""

from __future__ import annotations

import re
import logging

_log = logging.getLogger(__name__)

RE_UNIPROT = re.compile(
    r'^([OPQ][0-9][A-Z0-9]{3}[0-9]'
    r'|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})$'
)


def is_uniprot_ac(name: str) -> bool:
    """Check if a string matches UniProt AC format."""
    return bool(RE_UNIPROT.match(name))


def uniprot_cleanup(
    uniprots: set[str],
    ncbi_tax_id: int,
    mapper=None,
) -> set[str]:
    """Clean up UniProt IDs: prefer SwissProt, validate, normalize.

    Pipeline:
    1. Translate secondary -> primary ACs
    2. For TrEMBL IDs: try to find SwissProt via gene symbol
    3. Filter to organism proteome (if reflist available)
    4. Validate AC format
    """
    if not uniprots or not mapper:
        return uniprots or set()

    # Step 1: secondary -> primary
    result = _primary_uniprot(uniprots, ncbi_tax_id, mapper)

    # Step 2: TrEMBL -> SwissProt via gene symbol
    result = _trembl_to_swissprot(result, ncbi_tax_id, mapper)

    # Step 3: filter to organism proteome
    result = _filter_organism(result, ncbi_tax_id)

    # Step 4: format validation
    result = {u for u in result if is_uniprot_ac(u)}

    return result


def _primary_uniprot(
    uniprots: set[str],
    ncbi_tax_id: int,
    mapper,
) -> set[str]:
    """Translate secondary UniProt ACs to primary."""
    result = set()

    for up in uniprots:
        primary = mapper._direct_lookup(up, 'uniprot-sec', 'uniprot-pri', ncbi_tax_id)
        if primary:
            result.update(primary)
        else:
            result.add(up)  # probably already primary

    return result


def _trembl_to_swissprot(
    uniprots: set[str],
    ncbi_tax_id: int,
    mapper,
) -> set[str]:
    """For TrEMBL IDs, try to find SwissProt via gene symbol.

    For each ID: if it is a SwissProt, keep it. If it is a TrEMBL,
    look up its gene symbol and find the SwissProt with the same symbol.
    If no SwissProt found, keep the TrEMBL.
    """
    try:
        from omnipath_utils.reflists import all_swissprots
        swissprot_set = all_swissprots(ncbi_tax_id)
    except Exception:
        # If reflists not available, can not filter
        return uniprots

    if not swissprot_set:
        return uniprots

    result = set()

    for up in uniprots:
        if up in swissprot_set:
            # Already SwissProt, keep it
            result.add(up)
            continue

        # It is a TrEMBL -- try to find SwissProt via gene symbol
        genesymbols = mapper._direct_lookup(up, 'trembl', 'genesymbol', ncbi_tax_id)
        if not genesymbols:
            genesymbols = mapper._direct_lookup(up, 'uniprot', 'genesymbol', ncbi_tax_id)

        if genesymbols:
            # Map gene symbol -> SwissProt
            for gs in genesymbols:
                swissprots = mapper._direct_lookup(gs, 'genesymbol', 'swissprot', ncbi_tax_id)
                if swissprots:
                    result.update(swissprots)
                    break
            else:
                # No SwissProt found via gene symbol, keep TrEMBL
                result.add(up)
        else:
            # Can not resolve, keep as-is
            result.add(up)

    return result


def _filter_organism(uniprots: set[str], ncbi_tax_id: int) -> set[str]:
    """Filter to IDs present in the organism proteome."""
    try:
        from omnipath_utils.reflists import all_uniprots
        proteome = all_uniprots(ncbi_tax_id)
    except Exception:
        return uniprots

    if not proteome:
        return uniprots

    filtered = uniprots & proteome

    # If filtering removed everything, return originals
    # (the proteome list might be incomplete)
    return filtered if filtered else uniprots
