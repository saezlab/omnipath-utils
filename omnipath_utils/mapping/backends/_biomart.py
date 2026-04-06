"""Ensembl BioMart mapping backend.

Prefers pypath.inputs.biomart when available; falls back to direct
HTTP requests against the Ensembl BioMart service.
"""

from __future__ import annotations

import logging
import textwrap
from collections import defaultdict

from omnipath_utils.taxonomy import ensure_ensembl_name
from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend

_log = logging.getLogger(__name__)

BIOMART_URL = 'https://www.ensembl.org/biomart/martservice'

XML_TEMPLATE = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE Query>
    <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" datasetConfigVersion="0.6">
      <Dataset name="{dataset}" interface="default">
        {attributes}
      </Dataset>
    </Query>""")

ATTR_TEMPLATE = '<Attribute name="{name}" />'


class BioMartBackend(MappingBackend):
    """Fetch ID mappings from Ensembl BioMart.

    Uses pypath.inputs.biomart (biomart_query) when pypath is available.
    Falls back to direct HTTP against the Ensembl BioMart XML service
    when pypath is not installed.
    """

    name = 'biomart'
    yaml_key = 'ensembl'

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dataset(ncbi_tax_id: int) -> str | None:
        """Build the BioMart dataset name for an organism."""

        ensembl_name = ensure_ensembl_name(ncbi_tax_id)

        if not ensembl_name:
            _log.warning('No Ensembl name for organism %d', ncbi_tax_id)
            return None

        return f'{ensembl_name}_gene_ensembl'

    # ------------------------------------------------------------------
    # pypath.inputs path
    # ------------------------------------------------------------------

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
        """Read mapping data via pypath.inputs.biomart.biomart_query."""

        from pypath.inputs.biomart import biomart_query

        dataset = self._dataset(ncbi_tax_id)

        if not dataset:
            return {}

        attrs = (
            [src_col, tgt_col]
            if src_col != tgt_col
            else [src_col]
        )

        _log.info(
            'BioMart query (pypath): attrs=%s, dataset=%s',
            attrs,
            dataset,
        )

        data: dict[str, set[str]] = defaultdict(set)

        for rec in biomart_query(attrs=attrs, dataset=dataset):
            id_a = getattr(rec, src_col, '')
            id_b = getattr(rec, tgt_col, '')

            if id_a and id_b:
                data[id_a].add(id_b)

        return dict(data)

    # ------------------------------------------------------------------
    # Direct HTTP fallback
    # ------------------------------------------------------------------

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
        """Query Ensembl BioMart directly (no pypath dependency)."""

        import requests

        dataset = self._dataset(ncbi_tax_id)

        if not dataset:
            return {}

        attrs = (
            [src_col, tgt_col]
            if src_col != tgt_col
            else [src_col]
        )
        attr_xml = '\n        '.join(
            ATTR_TEMPLATE.format(name=a) for a in attrs
        )

        xml = XML_TEMPLATE.format(dataset=dataset, attributes=attr_xml)
        xml = xml.replace('\n', '').replace('  ', '')  # compact

        _log.info(
            'Querying BioMart (direct HTTP): %s -> %s (dataset %s)',
            id_type,
            target_id_type,
            dataset,
        )

        resp = requests.get(
            BIOMART_URL,
            params={'query': xml},
            timeout=120,
        )
        resp.raise_for_status()

        data: dict[str, set[str]] = defaultdict(set)
        lines = resp.text.strip().split('\n')

        for line in lines[1:]:  # skip header
            parts = line.split('\t')

            if len(parts) >= 2:
                src = parts[0].strip()
                tgt = parts[1].strip()

                if src and tgt:
                    data[src].add(tgt)

        return dict(data)


register('biomart', BioMartBackend)
