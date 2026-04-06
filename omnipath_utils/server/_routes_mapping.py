"""Mapping (ID translation) endpoints."""

from __future__ import annotations

import logging

from litestar import Controller, get, post
from sqlalchemy.orm import Session
from litestar.params import Parameter

from omnipath_utils.db._query import translate_ids

_log = logging.getLogger(__name__)


def _resolve_and_cleanup(
    result: dict[str, set[str]],
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int,
) -> dict[str, set[str]]:
    """Apply alias resolution and UniProt cleanup to results."""

    from omnipath_utils.mapping._id_types import IdTypeRegistry

    reg = IdTypeRegistry.get()
    target_resolved = reg.resolve(target_id_type) or target_id_type

    if target_resolved == 'uniprot':

        try:
            from omnipath_utils.mapping._mapper import Mapper
            from omnipath_utils.mapping._cleanup import uniprot_cleanup

            mapper = Mapper.get()

            for src_id in result:
                if result[src_id]:
                    result[src_id] = uniprot_cleanup(
                        result[src_id], ncbi_tax_id, mapper=mapper,
                    )
        except Exception:
            _log.debug(
                'UniProt cleanup failed for target %s, '
                'returning raw results.',
                target_id_type,
                exc_info=True,
            )

    return result


def _apply_fallbacks(
    session: Session,
    id_list: list[str],
    id_type_resolved: str,
    target_resolved: str,
    ncbi_tax_id: int,
    result: dict[str, set[str]],
) -> dict[str, set[str]]:
    """Apply the same fallback chain as the Python API via DB queries.

    Fallbacks:
    1. Gene symbol: uppercase, capitalize, synonyms
    2. Chain translation via uniprot
    """

    # Gene symbol fallbacks
    if id_type_resolved in ('genesymbol', 'genesymbol-syn'):
        unmapped = [i for i in id_list if not result.get(i)]

        if unmapped:
            # Try uppercase
            upper_map = {}
            upper_ids = []
            for name in unmapped:
                u = name.upper()
                if u != name:
                    upper_map[u] = name
                    upper_ids.append(u)

            if upper_ids:
                upper_result = translate_ids(
                    session, upper_ids, id_type_resolved,
                    target_resolved, ncbi_tax_id,
                )
                for upper, targets in upper_result.items():
                    if targets and upper in upper_map:
                        result[upper_map[upper]] = targets

            # Try capitalized (for rodent symbols)
            still_unmapped = [i for i in unmapped if not result.get(i)]
            if still_unmapped:
                cap_map = {}
                cap_ids = []
                for name in still_unmapped:
                    c = name.capitalize()
                    if c != name:
                        cap_map[c] = name
                        cap_ids.append(c)

                if cap_ids:
                    cap_result = translate_ids(
                        session, cap_ids, id_type_resolved,
                        target_resolved, ncbi_tax_id,
                    )
                    for cap, targets in cap_result.items():
                        if targets and cap in cap_map:
                            result[cap_map[cap]] = targets

            # Try synonyms
            still_unmapped = [i for i in unmapped if not result.get(i)]
            if still_unmapped:
                syn_result = translate_ids(
                    session, still_unmapped, 'genesymbol-syn',
                    target_resolved, ncbi_tax_id,
                )
                for src, targets in syn_result.items():
                    if targets:
                        result[src] = targets

                # Also try uppercase synonyms
                still_unmapped = [
                    i for i in still_unmapped if not result.get(i)
                ]
                if still_unmapped:
                    upper_syns = [n.upper() for n in still_unmapped]
                    syn_upper = translate_ids(
                        session, upper_syns, 'genesymbol-syn',
                        target_resolved, ncbi_tax_id,
                    )
                    for idx, name in enumerate(still_unmapped):
                        upper = upper_syns[idx]
                        if syn_upper.get(upper):
                            result[name] = syn_upper[upper]

    # Chain via uniprot if neither side is uniprot
    still_unmapped = [i for i in id_list if not result.get(i)]
    if (
        still_unmapped
        and id_type_resolved != 'uniprot'
        and target_resolved != 'uniprot'
    ):
        intermediate = translate_ids(
            session, still_unmapped, id_type_resolved,
            'uniprot', ncbi_tax_id,
        )
        for src, uniprots in intermediate.items():
            if uniprots:
                final = translate_ids(
                    session, list(uniprots), 'uniprot',
                    target_resolved, ncbi_tax_id,
                )
                targets = set()
                for t in final.values():
                    targets.update(t)
                if targets:
                    result[src] = targets

    return result


class MappingController(Controller):
    """ID translation endpoints."""

    path = '/mapping'

    @get('/translate')
    async def translate(  # noqa: D417
        self,
        session: Session,
        identifiers: str = Parameter(
            description='Comma-separated identifiers',
        ),
        id_type: str = Parameter(description='Source ID type'),
        target_id_type: str = Parameter(description='Target ID type'),
        ncbi_tax_id: int = Parameter(
            default=9606,
            description='NCBI Taxonomy ID',
        ),
        raw: bool = Parameter(
            default=False,
            description='Skip special-case handling, return raw DB results',
        ),
        backend: str | None = Parameter(
            default=None,
            required=False,
            description='Force specific backend',
        ),
    ) -> dict:
        """Translate identifiers from one type to another.

        Args:
            identifiers: Comma-separated identifiers.
            id_type: Source ID type.
            target_id_type: Target ID type.
            ncbi_tax_id: NCBI Taxonomy ID.
            raw: Skip special-case handling.
            backend: Force specific backend (ignored for DB mode).
        """

        from omnipath_utils.mapping._id_types import IdTypeRegistry

        reg = IdTypeRegistry.get()
        id_type_resolved = reg.resolve(id_type) or id_type
        target_resolved = reg.resolve(target_id_type) or target_id_type

        id_list = [
            i.strip() for i in identifiers.split(',') if i.strip()
        ]

        result = translate_ids(
            session,
            id_list,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
        )

        if not raw:
            result = _apply_fallbacks(
                session, id_list, id_type_resolved,
                target_resolved, ncbi_tax_id, result,
            )
            result = _resolve_and_cleanup(
                result, id_type_resolved, target_resolved, ncbi_tax_id,
            )

        mapped = {k: sorted(v) for k, v in result.items() if v}
        unmapped = [
            i for i in id_list if i not in result or not result[i]
        ]

        return {
            'results': mapped,
            'unmapped': unmapped,
            'meta': {
                'id_type': id_type,
                'target_id_type': target_id_type,
                'ncbi_tax_id': ncbi_tax_id,
                'total_input': len(id_list),
                'total_mapped': len(mapped),
                'raw': raw,
                'backend': backend,
            },
        }

    @post('/translate')
    async def translate_post(
        self,
        session: Session,
        data: dict,
    ) -> dict:
        """Translate identifiers via POST with JSON body.

        Body fields:
            identifiers: List of identifiers.
            id_type: Source ID type.
            target_id_type: Target ID type.
            ncbi_tax_id: NCBI Taxonomy ID (default: 9606).
            raw: Skip special-case handling (default: false).
            backend: Force specific backend (default: null).
        """

        from omnipath_utils.mapping._id_types import IdTypeRegistry

        reg = IdTypeRegistry.get()

        id_list = data.get('identifiers', [])
        id_type = data.get('id_type', '')
        target_id_type = data.get('target_id_type', '')
        ncbi_tax_id = data.get('ncbi_tax_id', 9606)
        raw = data.get('raw', False)
        backend = data.get('backend', None)

        id_type_resolved = reg.resolve(id_type) or id_type
        target_resolved = reg.resolve(target_id_type) or target_id_type

        result = translate_ids(
            session,
            id_list,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
        )

        if not raw:
            result = _apply_fallbacks(
                session, id_list, id_type_resolved,
                target_resolved, ncbi_tax_id, result,
            )
            result = _resolve_and_cleanup(
                result, id_type_resolved, target_resolved, ncbi_tax_id,
            )

        mapped = {k: sorted(v) for k, v in result.items() if v}
        unmapped = [
            i for i in id_list if i not in result or not result[i]
        ]

        return {
            'results': mapped,
            'unmapped': unmapped,
            'meta': {
                'id_type': id_type,
                'target_id_type': target_id_type,
                'ncbi_tax_id': ncbi_tax_id,
                'total_input': len(id_list),
                'total_mapped': len(mapped),
                'raw': raw,
                'backend': backend,
            },
        }

    @get('/id-types')
    async def id_types(self) -> list[dict]:
        """List all supported ID types."""

        from omnipath_utils.mapping._id_types import IdTypeRegistry

        reg = IdTypeRegistry.get()

        return [
            {
                'name': name,
                'label': (reg.info(name) or {}).get('label'),
                'entity_type': (reg.info(name) or {}).get('entity_type'),
                'curie_prefix': (reg.info(name) or {}).get('curie_prefix'),
            }
            for name in reg.all_names()
        ]
