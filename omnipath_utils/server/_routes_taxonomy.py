"""Taxonomy endpoints."""

from __future__ import annotations

from litestar import Controller, get
from litestar.params import Parameter

from omnipath_utils.taxonomy import (
    ensure_ncbi_tax_id,
    ensure_common_name,
    ensure_latin_name,
    ensure_ensembl_name,
    ensure_kegg_code,
    ensure_mirbase_name,
    ensure_oma_code,
    all_organisms,
)


class TaxonomyController(Controller):
    """Taxonomy / organism resolution endpoints."""

    path = '/taxonomy'

    @get('/resolve')
    async def resolve(
        self,
        organism: str = Parameter(
            description='Organism name, code, or NCBI Taxonomy ID',
        ),
    ) -> dict:
        """Resolve an organism identifier to all name forms."""
        taxid = ensure_ncbi_tax_id(organism)

        if taxid is None:
            return {
                'error': f'Unknown organism: {organism}',
                'ncbi_tax_id': None,
            }

        return {
            'ncbi_tax_id': taxid,
            'common_name': ensure_common_name(taxid),
            'latin_name': ensure_latin_name(taxid),
            'ensembl_name': ensure_ensembl_name(taxid),
            'kegg_code': ensure_kegg_code(taxid),
            'mirbase_code': ensure_mirbase_name(taxid),
            'oma_code': ensure_oma_code(taxid),
        }

    @get('/organisms')
    async def organisms(self) -> list[dict]:
        """List all known organisms."""
        orgs = all_organisms()
        return [
            {'ncbi_tax_id': taxid, **info}
            for taxid, info in sorted(orgs.items())
        ]
