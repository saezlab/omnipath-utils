"""UniProt REST API mapping backend.

Prefers pypath.inputs.uniprot when available; falls back to direct
HTTP requests against the UniProt REST API.
"""

from __future__ import annotations

import re
import logging
from collections import defaultdict

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend
from omnipath_utils.mapping._id_types import IdTypeRegistry

_log = logging.getLogger(__name__)

UNIPROT_STREAM = 'https://rest.uniprot.org/uniprotkb/stream'

# Pattern for stripping qualifiers UniProt sometimes puts in parentheses
# e.g. "BRCA1 (Fragment)" -> we keep the full value as-is for now
_RE_MULTI_SEP = re.compile(r';\s*')


class UniProtBackend(MappingBackend):
    """Fetch ID mappings from UniProt.

    Uses pypath.inputs.uniprot (UniprotQuery) when pypath is available.
    Falls back to direct HTTP against the /uniprotkb/stream endpoint
    when pypath is not installed.
    """

    def read(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        """Query UniProt and build a source -> target mapping dict."""

        try:
            return self._read_via_pypath(
                id_type, target_id_type, ncbi_tax_id, **kwargs,
            )
        except ImportError:
            _log.debug(
                'pypath not available, falling back to direct HTTP '
                'for UniProt backend',
            )
            return self._read_direct(
                id_type, target_id_type, ncbi_tax_id, **kwargs,
            )

    # ------------------------------------------------------------------
    # pypath.inputs path
    # ------------------------------------------------------------------

    def _read_via_pypath(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        """Read mapping data via pypath.inputs.uniprot.UniprotQuery."""

        from pypath.inputs.uniprot import UniprotQuery

        from pkg_infra.utils import to_set, swap_dict

        reg = IdTypeRegistry.get()

        src_field = reg.backend_column(id_type, 'uniprot')
        tgt_field = reg.backend_column(target_id_type, 'uniprot')

        if not src_field or not tgt_field:
            _log.debug(
                'UniProt backend does not support %s -> %s',
                id_type,
                target_id_type,
            )
            return {}

        # UniprotQuery.perform() returns {accession: value} when exactly
        # one field is requested.  We need to figure out which side is
        # the accession (the dict key) and which is the queried field.

        if src_field == 'accession' and tgt_field == 'accession':
            # Identity: both sides are uniprot AC — nothing to query
            _log.debug('UniProt backend: both sides are accession')
            return {}

        if src_field == 'accession':
            field = tgt_field
            swap = False
        elif tgt_field == 'accession':
            field = src_field
            swap = True
        else:
            # Neither side is accession — need two fields in the TSV.
            # UniprotQuery supports this via its __iter__ but perform()
            # returns a nested dict.  Fall back to direct HTTP which
            # already handles arbitrary pairs.
            _log.debug(
                'UniProt/pypath: neither side is accession for '
                '%s -> %s, falling back to direct HTTP',
                id_type,
                target_id_type,
            )
            return self._read_direct(
                id_type, target_id_type, ncbi_tax_id, **kwargs,
            )

        # Determine reviewed filter from id_type semantics
        swissprot_only = id_type == 'swissprot' or target_id_type == 'swissprot'
        trembl_only = id_type == 'trembl' or target_id_type == 'trembl'
        reviewed = True if swissprot_only else (False if trembl_only else None)

        _log.info(
            'UniProt query (pypath): field=%s, organism=%d, reviewed=%s',
            field,
            ncbi_tax_id,
            reviewed,
        )

        query = UniprotQuery(
            reviewed=reviewed,
            organism=ncbi_tax_id,
            fields=field,
        )
        query.name_process = True
        raw_data = query.perform()

        if not raw_data:
            return {}

        # raw_data is {accession: value} where value can be str or list
        data = {k: to_set(v) for k, v in raw_data.items()}

        if swap:
            return swap_dict(data, force_sets=True)

        return data

    # ------------------------------------------------------------------
    # Direct HTTP fallback
    # ------------------------------------------------------------------

    def _read_direct(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        """Query UniProt REST API directly (no pypath dependency)."""

        import requests

        reg = IdTypeRegistry.get()

        src_field = reg.backend_column(id_type, 'uniprot')
        tgt_field = reg.backend_column(target_id_type, 'uniprot')

        if not src_field or not tgt_field:
            _log.debug(
                'UniProt backend does not support %s -> %s',
                id_type,
                target_id_type,
            )
            return {}

        # Build the fields list; accession is always included by UniProt
        # but we request only the specific fields we need.
        field_set = sorted({src_field, tgt_field})
        fields_str = ','.join(field_set)

        _log.info(
            'Querying UniProt (direct HTTP): %s -> %s '
            '(organism %d, fields: %s)',
            id_type,
            target_id_type,
            ncbi_tax_id,
            fields_str,
        )

        params = {
            'query': f'(organism_id:{ncbi_tax_id})',
            'fields': fields_str,
            'format': 'tsv',
        }

        resp = requests.get(
            UNIPROT_STREAM,
            params=params,
            timeout=300,
            stream=True,
        )
        resp.raise_for_status()

        data: dict[str, set[str]] = defaultdict(set)

        # Decompress and parse TSV
        text = resp.text
        lines = text.split('\n')

        if not lines:
            return {}

        # Parse header to find column indices
        header = lines[0].split('\t')

        # Map our requested fields to column indices.
        # UniProt returns column headers that are human-readable labels,
        # not the field names we requested.  We match by position:
        # the columns appear in the same order as the fields parameter.
        src_idx = None
        tgt_idx = None

        if len(field_set) == 1:
            # Same field for both (e.g. accession -> accession)
            src_idx = 0
            tgt_idx = 0
        else:
            # Fields appear in the order we requested (alphabetical).
            for i, requested_field in enumerate(field_set):
                if requested_field == src_field:
                    src_idx = i
                if requested_field == tgt_field:
                    tgt_idx = i

        if src_idx is None or tgt_idx is None:
            _log.warning(
                'Could not match columns in UniProt response '
                '(header: %s, fields: %s)',
                header,
                field_set,
            )
            return {}

        n_entries = 0

        for line in lines[1:]:
            line = line.strip()

            if not line:
                continue

            parts = line.split('\t')

            if max(src_idx, tgt_idx) >= len(parts):
                continue

            src_raw = parts[src_idx].strip()
            tgt_raw = parts[tgt_idx].strip()

            if not src_raw or not tgt_raw:
                continue

            # UniProt fields can contain multiple values separated
            # by '; ' (e.g. gene synonyms) or by ' ' for some xref
            # fields.  We split on '; ' which is the standard.
            src_vals = _RE_MULTI_SEP.split(src_raw)
            tgt_vals = _RE_MULTI_SEP.split(tgt_raw)

            for src in src_vals:
                src = src.strip()

                if not src:
                    continue

                for tgt in tgt_vals:
                    tgt = tgt.strip()

                    if tgt:
                        data[src].add(tgt)
                        n_entries += 1

        _log.info(
            'UniProt: loaded %d source IDs, %d total pairs '
            'for %s -> %s (organism %d)',
            len(data),
            n_entries,
            id_type,
            target_id_type,
            ncbi_tax_id,
        )

        return dict(data)


register('uniprot', UniProtBackend)
