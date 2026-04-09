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
) -> tuple[dict[str, set[str]], set[str]]:
    """Translate IDs via the database.

    Returns:
        Tuple of (results dict, set of backend names used).
    """
    result = defaultdict(set)
    backends_used = set()

    rows = session.execute(
        text(f"""
            SELECT m.source_id, m.target_id, b.name
            FROM {SCHEMA}.id_mapping m
            JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
            JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
            JOIN {SCHEMA}.backend b ON m.backend_id = b.id
            WHERE st.name = :src_type
            AND tt.name = :tgt_type
            AND m.ncbi_tax_id = :tax
            AND m.source_id = ANY(:ids)
        """),
        {
            'src_type': source_type,
            'tgt_type': target_type,
            'tax': ncbi_tax_id,
            'ids': identifiers,
        },
    )

    for row in rows:
        result[row[0]].add(row[1])
        backends_used.add(row[2])

    # If no results, try the reverse direction
    # (we may have target→source but not source→target)
    missing = [i for i in identifiers if i not in result]
    if missing:
        rev_rows = session.execute(
            text(f"""
                SELECT m.target_id, m.source_id, b.name
                FROM {SCHEMA}.id_mapping m
                JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
                JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
                JOIN {SCHEMA}.backend b ON m.backend_id = b.id
                WHERE st.name = :tgt_type
                AND tt.name = :src_type
                AND (m.ncbi_tax_id = :tax OR m.ncbi_tax_id = 0)
                AND m.target_id = ANY(:ids)
            """),
            {
                'src_type': source_type,
                'tgt_type': target_type,
                'tax': ncbi_tax_id,
                'ids': missing,
            },
        )

        for row in rev_rows:
            result[row[0]].add(row[1])
            backends_used.add(f'{row[2]}(rev)')

    return dict(result), backends_used


def get_full_table(
    session: Session,
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
) -> dict[str, set[str]]:
    """Get a full mapping table from the database."""
    result = defaultdict(set)

    rows = session.execute(
        text(f"""
            SELECT m.source_id, m.target_id
            FROM {SCHEMA}.id_mapping m
            JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
            JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
            WHERE st.name = :src_type
            AND tt.name = :tgt_type
            AND m.ncbi_tax_id = :tax
        """),
        {
            'src_type': source_type,
            'tgt_type': target_type,
            'tax': ncbi_tax_id,
        },
    )

    for row in rows:
        result[row[0]].add(row[1])

    return dict(result)


def identify_ids(
    session: Session,
    identifiers: list[str],
    ncbi_tax_id: int,
) -> dict[str, list[dict]]:
    """Identify what type(s) each identifier belongs to.

    Searches the id_mapping table as both source and target to find
    which ID types contain each identifier.

    Returns:
        Dict mapping each identifier to a list of dicts with
        "id_type", "role" ("source" or "target"), and "count".
    """
    result = {}

    for identifier in identifiers:
        matches = []

        # Search as source
        rows = session.execute(
            text(f"""
                SELECT st.name, count(DISTINCT m.target_id)
                FROM {SCHEMA}.id_mapping m
                JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
                WHERE m.source_id = :id
                AND (m.ncbi_tax_id = :tax OR m.ncbi_tax_id = 0)
                GROUP BY st.name
            """),
            {'id': identifier, 'tax': ncbi_tax_id},
        ).fetchall()

        for row in rows:
            matches.append({
                'id_type': row[0],
                'role': 'source',
                'mappings_count': row[1],
            })

        # Search as target
        rows = session.execute(
            text(f"""
                SELECT tt.name, count(DISTINCT m.source_id)
                FROM {SCHEMA}.id_mapping m
                JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
                WHERE m.target_id = :id
                AND (m.ncbi_tax_id = :tax OR m.ncbi_tax_id = 0)
                GROUP BY tt.name
            """),
            {'id': identifier, 'tax': ncbi_tax_id},
        ).fetchall()

        for row in rows:
            matches.append({
                'id_type': row[0],
                'role': 'target',
                'mappings_count': row[1],
            })

        result[identifier] = matches

    return result


def get_all_mappings(
    session: Session,
    identifiers: list[str],
    source_type: str,
    ncbi_tax_id: int,
) -> dict[str, dict[str, list[str]]]:
    """Get all mappings for identifiers across all target types.

    Returns:
        Dict mapping each identifier to a dict of {target_type: [target_ids]}.
    """
    result = {}

    rows = session.execute(
        text(f"""
            SELECT m.source_id, tt.name, m.target_id
            FROM {SCHEMA}.id_mapping m
            JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
            JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
            WHERE st.name = :src_type
            AND (m.ncbi_tax_id = :tax OR m.ncbi_tax_id = 0)
            AND m.source_id = ANY(:ids)
            ORDER BY m.source_id, tt.name
        """),
        {
            'src_type': source_type,
            'tax': ncbi_tax_id,
            'ids': identifiers,
        },
    ).fetchall()

    for row in rows:
        src_id, tgt_type, tgt_id = row
        result.setdefault(src_id, {}).setdefault(tgt_type, []).append(tgt_id)

    return result


def chain_translate(
    session: Session,
    identifiers: list[str],
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
    via: str = 'uniprot',
) -> tuple[dict[str, set[str]], set[str]]:
    """Chain translation: source -> via -> target, all in the DB.

    Uses two JOINs to find: source_id -> via_id -> target_id.
    Returns (result_dict, backends_used).
    """
    from collections import defaultdict

    result = defaultdict(set)
    backends_used = set()

    rows = session.execute(
        text(f"""
            SELECT m1.source_id, m2.target_id, b1.name, b2.name
            FROM {SCHEMA}.id_mapping m1
            JOIN {SCHEMA}.id_type st1 ON m1.source_type_id = st1.id
            JOIN {SCHEMA}.id_type vt ON m1.target_type_id = vt.id
            JOIN {SCHEMA}.id_mapping m2 ON m1.target_id = m2.source_id
                AND m2.ncbi_tax_id = m1.ncbi_tax_id
            JOIN {SCHEMA}.id_type st2 ON m2.source_type_id = st2.id
            JOIN {SCHEMA}.id_type tt2 ON m2.target_type_id = tt2.id
            JOIN {SCHEMA}.backend b1 ON m1.backend_id = b1.id
            JOIN {SCHEMA}.backend b2 ON m2.backend_id = b2.id
            WHERE st1.name = :src_type
            AND vt.name = :via_type
            AND st2.name = :via_type
            AND tt2.name = :tgt_type
            AND m1.source_id = ANY(:ids)
            AND (m1.ncbi_tax_id = :tax OR m1.ncbi_tax_id = 0)
            AND (m2.ncbi_tax_id = :tax OR m2.ncbi_tax_id = 0)
        """),
        {
            'src_type': source_type,
            'via_type': via,
            'tgt_type': target_type,
            'tax': ncbi_tax_id,
            'ids': identifiers,
        },
    ).fetchall()

    for row in rows:
        result[row[0]].add(row[1])
        backends_used.add(f'{row[2]}+{row[3]}')

    return dict(result), backends_used
