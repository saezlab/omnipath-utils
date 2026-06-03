"""PubChem structure-bearing mapping backend (Milestone I).

Uses the legacy functional ``pypath.inputs.pubchem.pubchem_mapping(target,
source='cid')`` (already ``dict[str, set]``) — the simplest path for PubChem,
which maps CID ↔ structure id types. No RDKit, no pypath change.
"""

from __future__ import annotations

import logging

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend

_log = logging.getLogger(__name__)

_CID_COLUMN = 'cid'


def _setify(data: dict) -> dict[str, set[str]]:
    return {
        str(k): (set(map(str, v)) if isinstance(v, (set, list, tuple)) else {str(v)})
        for k, v in (data or {}).items()
    }


def _invert(mapping: dict[str, set[str]]) -> dict[str, set[str]]:
    inverted: dict[str, set[str]] = {}
    for source, targets in mapping.items():
        for target in targets:
            inverted.setdefault(target, set()).add(source)
    return inverted


class PubchemBackend(MappingBackend):
    name = 'pubchem'
    yaml_key = 'pubchem'

    def _read_via_pypath(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        *,
        src_col: str,
        tgt_col: str,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        from pypath.inputs.pubchem import pubchem_mapping

        # pubchem_mapping always maps CID -> <other>; bridge both directions.
        if src_col == _CID_COLUMN:
            return _setify(pubchem_mapping(tgt_col, source=_CID_COLUMN))
        if tgt_col == _CID_COLUMN:
            return _invert(_setify(pubchem_mapping(src_col, source=_CID_COLUMN)))
        _log.debug('pubchem backend only bridges via CID (%s -> %s)', src_col, tgt_col)
        return {}

    def _read_direct(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        *,
        src_col: str,
        tgt_col: str,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        raise ImportError('pubchem requires pypath')


register('pubchem', PubchemBackend)
