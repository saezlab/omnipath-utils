"""Database query helpers."""

from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

from omnipath_utils.db._connection import SCHEMA

_log = logging.getLogger(__name__)


def translate_ids(
    session: Session,
    identifiers: list[str],
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
) -> dict[str, set[str]]:
    """Translate IDs via the database.

    Returns dict mapping source IDs to sets of target IDs.
    """
    result = defaultdict(set)

    rows = session.execute(
        text(f'''
            SELECT m.source_id, m.target_id
            FROM {SCHEMA}.id_mapping m
            JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
            JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
            WHERE st.name = :src_type
            AND tt.name = :tgt_type
            AND m.ncbi_tax_id = :tax
            AND m.source_id = ANY(:ids)
        '''),
        {
            'src_type': source_type,
            'tgt_type': target_type,
            'tax': ncbi_tax_id,
            'ids': identifiers,
        },
    )

    for row in rows:
        result[row[0]].add(row[1])

    return dict(result)


def get_full_table(
    session: Session,
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
) -> dict[str, set[str]]:
    """Get a full mapping table from the database."""
    result = defaultdict(set)

    rows = session.execute(
        text(f'''
            SELECT m.source_id, m.target_id
            FROM {SCHEMA}.id_mapping m
            JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
            JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
            WHERE st.name = :src_type
            AND tt.name = :tgt_type
            AND m.ncbi_tax_id = :tax
        '''),
        {
            'src_type': source_type,
            'tgt_type': target_type,
            'tax': ncbi_tax_id,
        },
    )

    for row in rows:
        result[row[0]].add(row[1])

    return dict(result)
