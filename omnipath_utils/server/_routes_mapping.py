"""Mapping (ID translation) endpoints."""

from __future__ import annotations

import logging

from litestar import Controller, get, post
from sqlalchemy.orm import Session
from litestar.params import Parameter

from omnipath_utils.db._query import (
    identify_ids,
    translate_ids,
    chain_translate,
    get_all_mappings,
)

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
                        result[src_id],
                        ncbi_tax_id,
                        mapper=mapper,
                    )
        except Exception:
            _log.debug(
                'UniProt cleanup failed for target %s, returning raw results.',
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
                upper_result, _ub = translate_ids(
                    session,
                    upper_ids,
                    id_type_resolved,
                    target_resolved,
                    ncbi_tax_id,
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
                    cap_result, _ = translate_ids(
                        session,
                        cap_ids,
                        id_type_resolved,
                        target_resolved,
                        ncbi_tax_id,
                    )
                    for cap, targets in cap_result.items():
                        if targets and cap in cap_map:
                            result[cap_map[cap]] = targets

            # Try synonyms
            still_unmapped = [i for i in unmapped if not result.get(i)]
            if still_unmapped:
                syn_result, _sb = translate_ids(
                    session,
                    still_unmapped,
                    'genesymbol-syn',
                    target_resolved,
                    ncbi_tax_id,
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
                    syn_upper, _ = translate_ids(
                        session,
                        upper_syns,
                        'genesymbol-syn',
                        target_resolved,
                        ncbi_tax_id,
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
        chain_result, chain_backends = chain_translate(
            session,
            still_unmapped,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
            via='uniprot',
        )
        for src, targets in chain_result.items():
            if targets:
                result[src] = targets

    return result


def _build_translate_response(
    id_list: list[str],
    result: dict[str, set[str]],
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int,
    raw: bool,
    backends_used: set[str],
    backend: str | None,
    loading: bool,
) -> dict:
    """Build the response dict for translate endpoints."""

    mapped = {k: sorted(v) for k, v in result.items() if v}
    unmapped = [i for i in id_list if i not in result or not result[i]]

    meta = {
        'id_type': id_type,
        'target_id_type': target_id_type,
        'ncbi_tax_id': ncbi_tax_id,
        'total_input': len(id_list),
        'total_mapped': len(mapped),
        'raw': raw,
        'backend': sorted(backends_used) if backends_used else backend,
        'loading': loading,
    }

    if loading:
        meta['loading_note'] = (
            f'Table {id_type} -> {target_id_type} is being loaded. '
            'Try again in a few minutes.'
        )

    return {
        'results': mapped,
        'unmapped': unmapped,
        'meta': meta,
    }


def _maybe_trigger_load(
    result: dict,
    raw: bool,
    id_type_resolved: str,
    target_resolved: str,
    ncbi_tax_id: int,
) -> bool:
    """Trigger background loading if no results found. Returns loading flag."""

    if result or raw:
        return False

    from omnipath_utils.db._loader import is_pending, request_table
    from omnipath_utils.db._connection import get_db_url

    if is_pending(id_type_resolved, target_resolved, ncbi_tax_id):
        return True

    queued = request_table(
        id_type_resolved,
        target_resolved,
        ncbi_tax_id,
        db_url=get_db_url(),
    )
    return queued


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

        id_list = [i.strip() for i in identifiers.split(',') if i.strip()]

        result, backends_used = translate_ids(
            session,
            id_list,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
        )

        # If no results from DB, trigger background load
        loading = _maybe_trigger_load(
            result,
            raw,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
        )

        if not raw:
            result = _apply_fallbacks(
                session,
                id_list,
                id_type_resolved,
                target_resolved,
                ncbi_tax_id,
                result,
            )
            result = _resolve_and_cleanup(
                result,
                id_type_resolved,
                target_resolved,
                ncbi_tax_id,
            )

        return _build_translate_response(
            id_list,
            result,
            id_type,
            target_id_type,
            ncbi_tax_id,
            raw,
            backends_used,
            backend,
            loading,
        )

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

        result, backends_used = translate_ids(
            session,
            id_list,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
        )

        # If no results from DB, trigger background load
        loading = _maybe_trigger_load(
            result,
            raw,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
        )

        if not raw:
            result = _apply_fallbacks(
                session,
                id_list,
                id_type_resolved,
                target_resolved,
                ncbi_tax_id,
                result,
            )
            result = _resolve_and_cleanup(
                result,
                id_type_resolved,
                target_resolved,
                ncbi_tax_id,
            )

        return _build_translate_response(
            id_list,
            result,
            id_type,
            target_id_type,
            ncbi_tax_id,
            raw,
            backends_used,
            backend,
            loading,
        )

    @get('/identify')
    async def identify(
        self,
        session: Session,
        identifiers: str = Parameter(
            description='Comma-separated identifiers',
        ),
        ncbi_tax_id: int = Parameter(
            default=9606,
            description='NCBI Taxonomy ID',
        ),
    ) -> dict:
        """Try to identify the type of given identifiers.

        Searches all mapping tables in the database to find which ID types
        contain the given identifiers.
        """

        id_list = [i.strip() for i in identifiers.split(',') if i.strip()]
        result = identify_ids(session, id_list, ncbi_tax_id)

        return {
            'results': result,
            'meta': {
                'ncbi_tax_id': ncbi_tax_id,
                'total_input': len(id_list),
            },
        }

    @get('/all')
    async def all_mappings(
        self,
        session: Session,
        identifiers: str = Parameter(
            description='Comma-separated identifiers',
        ),
        id_type: str = Parameter(description='Source ID type'),
        ncbi_tax_id: int = Parameter(
            default=9606,
            description='NCBI Taxonomy ID',
        ),
    ) -> dict:
        """Return all known mappings for identifiers across all target types."""

        from omnipath_utils.mapping._id_types import IdTypeRegistry

        reg = IdTypeRegistry.get()
        id_type_resolved = reg.resolve(id_type) or id_type

        id_list = [i.strip() for i in identifiers.split(',') if i.strip()]
        result = get_all_mappings(
            session, id_list, id_type_resolved, ncbi_tax_id
        )

        return {
            'results': result,
            'meta': {
                'id_type': id_type,
                'ncbi_tax_id': ncbi_tax_id,
                'total_input': len(id_list),
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

    @get('/loading')
    async def loading_status(self) -> dict:
        """Check which tables are currently being loaded."""

        from omnipath_utils.db._loader import _pending

        return {
            'loading': [
                {
                    'source_type': s,
                    'target_type': t,
                    'ncbi_tax_id': n,
                }
                for s, t, n in sorted(_pending)
            ],
            'count': len(_pending),
        }
