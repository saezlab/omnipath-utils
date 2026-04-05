"""Reference list endpoints."""

from __future__ import annotations

from litestar import Controller, get
from litestar.params import Parameter


class ReflistController(Controller):
    """Reference list endpoints."""

    path = '/reflists'

    @get('/list-names')
    async def list_names(self) -> list[str]:
        """List available reference list names."""
        return ['swissprot', 'trembl', 'uniprot']

    @get('/{list_name:str}')
    async def get_reflist(
        self,
        list_name: str,
        ncbi_tax_id: int = Parameter(default=9606),
    ) -> dict:
        """Get a reference list."""
        from omnipath_utils.reflists import get_reflist

        ids = get_reflist(list_name, ncbi_tax_id)

        return {
            'list_name': list_name,
            'ncbi_tax_id': ncbi_tax_id,
            'count': len(ids),
            'identifiers': sorted(ids),
        }
