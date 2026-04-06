"""Unified translation core -- single fallback chain for all APIs."""

from __future__ import annotations

import logging

from omnipath_utils._constants import DEFAULT_ORGANISM
from omnipath_utils.mapping._id_types import IdTypeRegistry

_log = logging.getLogger(__name__)


def translate_core(
    identifiers: list[str],
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
    strict: bool = False,
    raw: bool = False,
    backend: str | None = None,
    uniprot_cleanup: bool = True,
) -> dict[str, set[str]]:
    """Unified translation with fallback chain.

    This function is the single entry point for ALL translation APIs.
    It works in memory mode (dict lookup) and can be extended for DB mode.

    Args:
        identifiers: List of source identifiers.
        id_type: Source ID type.
        target_id_type: Target ID type.
        ncbi_tax_id: Organism (default: 9606).
        strict: Skip fuzzy fallbacks (gene symbol case, append "1", etc.).
        raw: Skip ALL special-case handling. Direct table lookup only.
        backend: Force a specific backend. None = auto-select.
        uniprot_cleanup: Apply UniProt cleanup when target is uniprot.

    Returns:
        Dict mapping each source ID to a set of target IDs.
    """
    from omnipath_utils.mapping._mapper import Mapper

    ncbi_tax_id = ncbi_tax_id or DEFAULT_ORGANISM
    reg = IdTypeRegistry.get()
    id_type = reg.resolve(id_type) or id_type
    target_id_type = reg.resolve(target_id_type) or target_id_type

    mapper = Mapper.get()

    if raw:
        # Raw mode: direct table lookup only, no fallbacks
        table = mapper.which_table(
            id_type,
            target_id_type,
            ncbi_tax_id,
            backend=backend,
        )
        if table:
            return {name: table[name] for name in identifiers}
        return {name: set() for name in identifiers}

    # Full mode: use map_name per ID (includes all special cases)
    # For vectorized performance, try table lookup first for the batch
    table = mapper.which_table(
        id_type,
        target_id_type,
        ncbi_tax_id,
        backend=backend,
    )

    result = {}
    needs_fallback = []

    if table:
        for name in identifiers:
            hits = table[name]
            if hits:
                result[name] = hits
            else:
                needs_fallback.append(name)
    else:
        needs_fallback = list(identifiers)

    # For IDs that did not get a direct hit, use full map_name
    if needs_fallback:
        for name in needs_fallback:
            result[name] = mapper.map_name(
                name,
                id_type,
                target_id_type,
                ncbi_tax_id,
                strict=strict,
                uniprot_cleanup_flag=uniprot_cleanup,
            )

    # Apply cleanup to ALL results if target is uniprot
    # (the direct table hits did not go through map_name cleanup)
    if uniprot_cleanup and target_id_type == 'uniprot' and table:
        from omnipath_utils.mapping._cleanup import (
            uniprot_cleanup as _cleanup,
        )

        for name in identifiers:
            if name in result and result[name] and name not in needs_fallback:
                result[name] = _cleanup(
                    result[name],
                    ncbi_tax_id,
                    mapper=mapper,
                )

    return result
