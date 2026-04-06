"""Mapping (ID translation) endpoints."""

from __future__ import annotations

import logging

from litestar import Controller, get, post
from litestar.params import Parameter

from sqlalchemy.orm import Session

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
            from omnipath_utils.mapping._cleanup import uniprot_cleanup
            from omnipath_utils.mapping._mapper import Mapper

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


class MappingController(Controller):
    """ID translation endpoints."""

    path = '/mapping'

    @get('/translate')
    async def translate(
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
    ) -> dict:
        """Translate identifiers from one type to another."""

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
            },
        }

    @post('/translate')
    async def translate_post(
        self,
        session: Session,
        data: dict,
    ) -> dict:
        """Translate identifiers via POST with JSON body."""

        from omnipath_utils.mapping._id_types import IdTypeRegistry

        reg = IdTypeRegistry.get()

        id_list = data.get('identifiers', [])
        id_type = data.get('id_type', '')
        target_id_type = data.get('target_id_type', '')
        ncbi_tax_id = data.get('ncbi_tax_id', 9606)

        id_type_resolved = reg.resolve(id_type) or id_type
        target_resolved = reg.resolve(target_id_type) or target_id_type

        result = translate_ids(
            session,
            id_list,
            id_type_resolved,
            target_resolved,
            ncbi_tax_id,
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
