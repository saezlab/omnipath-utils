"""UniProt REST API mapping backend."""

from __future__ import annotations

import re
import logging
from collections import defaultdict

import requests

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend
from omnipath_utils.mapping._id_types import IdTypeRegistry

_log = logging.getLogger(__name__)

UNIPROT_STREAM = 'https://rest.uniprot.org/uniprotkb/stream'

# Pattern for stripping qualifiers UniProt sometimes puts in parentheses
# e.g. "BRCA1 (Fragment)" -> we keep the full value as-is for now
_RE_MULTI_SEP = re.compile(r';\s*')


class UniProtBackend(MappingBackend):
    """Fetch ID mappings from the UniProt REST API.

    Uses the /uniprotkb/stream endpoint which returns all results
    without pagination.  The response is a TSV with a header line,
    where the first column is always "accession" (the UniProt AC).
    """

    def read(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        """Query UniProt and build a source -> target mapping dict."""

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
            'Querying UniProt: %s -> %s (organism %d, fields: %s)',
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
        header_lower = [h.strip().lower() for h in header]

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
