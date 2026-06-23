"""Database query helpers."""

from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

from omnipath_utils.db._connection import SCHEMA

_log = logging.getLogger(__name__)


_FTP_TABLE = 'id_mapping_ftp'
_LONG_TABLE = 'id_mapping_long'
_ftp_exists_cache: dict = {}
_ftp_types_cache: dict = {}

# Long-value id_types -- names and structures -- live in ``id_mapping_long``
# (text values), not the ``varchar(64)`` database-ID ``id_mapping`` table (R2).
NAME_TYPES: frozenset[str] = frozenset(
    {'name', 'synonym', 'iupac', 'traditional_iupac'}
)
STRUCTURE_TYPES: frozenset[str] = frozenset({'inchi', 'smiles'})
LONG_VALUE_TYPES: frozenset[str] = NAME_TYPES | STRUCTURE_TYPES


def _is_long_query(source_type: str, target_type: str) -> bool:
    """A query is long-valued if either side is a name or structure type."""
    return source_type in LONG_VALUE_TYPES or target_type in LONG_VALUE_TYPES


def _normalise_long_ids(
    identifiers: list[str] | None,
    source_type: str,
) -> list[str] | None:
    """Normalise query keys for the long-value table.

    Name source types are lowercased + whitespace-stripped (case-insensitive,
    FR-002); structure types (and any database-ID source) are matched verbatim
    (FR-019).
    """
    if identifiers is None:
        return None
    if source_type in NAME_TYPES:
        return [i.strip().lower() for i in identifiers]
    return identifiers


def _ftp_table_exists(session: Session) -> bool:
    """Whether the comprehensive FTP table is present (cached per process)."""
    if 'v' not in _ftp_exists_cache:
        present = session.execute(
            text(f"SELECT to_regclass('{SCHEMA}.{_FTP_TABLE}')")
        ).scalar()
        _ftp_exists_cache['v'] = present is not None
    return _ftp_exists_cache['v']


def _ftp_relevant(
    source_type: str,
    target_type: str,
    ftp_types: frozenset[str],
) -> bool:
    """FR-018 gate predicate for the full-UniProt fallback.

    ``id_mapping_ftp`` can only contain a mapping when **both** sides are
    id_types present in that table (it is 100% UniProt-family). A pair with a
    non-FTP type on either side has zero rows there, so consulting it is
    fruitless -- skipping is result-identical (R8).
    """
    return source_type in ftp_types and target_type in ftp_types


def _ftp_types(session: Session) -> frozenset[str]:
    """The set of id_types present in ``id_mapping_ftp`` (cached per process).

    Read cheaply from a precomputed ``build_info`` row (``table_name =
    'ftp_types'``); if that is absent (a build predating FR-018) it is derived
    once from the table's distinct type ids and cached. Empty when the FTP
    table is not present.
    """
    if 'v' in _ftp_types_cache:
        return _ftp_types_cache['v']

    if not _ftp_table_exists(session):
        _ftp_types_cache['v'] = frozenset()
        return _ftp_types_cache['v']

    # Precomputed by DatabaseBuilder.record_ftp_types as one row per type
    # (build_info.source_type is varchar(64), so a row per name, not a joined
    # string).
    precomputed = session.execute(
        text(
            f"SELECT source_type FROM {SCHEMA}.build_info "
            "WHERE table_name = 'ftp_types' AND source_type IS NOT NULL"
        )
    ).scalars().all()
    if precomputed:
        types = frozenset(t for t in precomputed if t)
    else:
        _log.warning(
            'ftp_types not precomputed in build_info; deriving once from '
            '%s.%s (run DatabaseBuilder.record_ftp_types to cache it)',
            SCHEMA,
            _FTP_TABLE,
        )
        rows = session.execute(
            text(
                f'SELECT name FROM {SCHEMA}.id_type WHERE id IN ('
                f' SELECT DISTINCT source_type_id FROM {SCHEMA}.{_FTP_TABLE}'
                f' UNION'
                f' SELECT DISTINCT target_type_id FROM {SCHEMA}.{_FTP_TABLE})'
            )
        ).scalars().all()
        types = frozenset(rows)

    _ftp_types_cache['v'] = types
    return types


def _query_table(
    session: Session,
    table: str,
    identifiers: list[str] | None,
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
) -> tuple[defaultdict, set]:
    """Forward + reverse-for-missing lookup against a single mapping table."""
    result = defaultdict(set)
    backends_used = set()
    params: dict = {
        'src_type': source_type,
        'tgt_type': target_type,
        'tax': ncbi_tax_id,
    }
    id_filter = ''
    if identifiers is not None:
        id_filter = 'AND m.source_id = ANY(:ids)'
        params['ids'] = identifiers

    rows = session.execute(
        text(f"""
            SELECT m.source_id, m.target_id, b.name
            FROM {table} m
            JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
            JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
            JOIN {SCHEMA}.backend b ON m.backend_id = b.id
            WHERE st.name = :src_type
            AND tt.name = :tgt_type
            AND m.ncbi_tax_id = :tax
            {id_filter}
        """),
        params,
    )
    for row in rows:
        result[row[0]].add(row[1])
        backends_used.add(row[2])

    if identifiers is not None:
        missing = [i for i in identifiers if i not in result]
        if missing:
            rev_rows = session.execute(
                text(f"""
                    SELECT m.target_id, m.source_id, b.name
                    FROM {table} m
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

    return result, backends_used


def translate_ids(
    session: Session,
    identifiers: list[str] | None,
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
    full_uniprot: str = 'fallback',
) -> tuple[dict[str, set[str]], set[str]]:
    """Translate IDs via the database.

    Queries the curated ``id_mapping`` first and, per ``full_uniprot``, the
    comprehensive full-UniProt table ``id_mapping_ftp`` (the complete UniProt
    idmapping, all organisms). Results are deduplicated (set union).

    Args:
        session: SQLAlchemy session.
        identifiers: Source IDs to translate, or ``None`` for the full table.
        source_type / target_type: ID type names.
        ncbi_tax_id: NCBI Taxonomy ID.
        full_uniprot: How to use the comprehensive full-UniProt table:
            ``'fallback'`` (default — curated, then full-UniProt only for
            still-unresolved identifiers), ``'never'`` (curated only),
            ``'both'`` (curated + full-UniProt), ``'only'`` (full-UniProt only).
            The full table is only consulted on explicit request
            (``both``/``only``) for a whole-table query, never as a blanket
            fallback.

    Returns:
        Tuple of (results dict, set of backend names used).
    """
    # Long-value (name / structure) queries route to the separate
    # ``id_mapping_long`` table (R2). Names are matched case-insensitively,
    # structures verbatim; chemicals are organism-agnostic (rows at tax 0), and
    # this path never touches the UniProt FTP table (FR-018 holds trivially).
    if _is_long_query(source_type, target_type):
        long_ids = _normalise_long_ids(identifiers, source_type)
        result, backends_used = _query_table(
            session, f'{SCHEMA}.{_LONG_TABLE}',
            long_ids, source_type, target_type, 0,
        )
        # Names are stored/queried lowercased; re-key the response back to each
        # caller's original-case input (so translate(['Taurine']) is keyed
        # 'Taurine', not 'taurine'). Structures are verbatim -> no re-keying.
        if identifiers is not None and source_type in NAME_TYPES:
            rekeyed: dict[str, set[str]] = {}
            for orig in identifiers:
                hits = result.get(orig.strip().lower())
                if hits:
                    rekeyed.setdefault(orig, set()).update(hits)
            return rekeyed, backends_used
        return dict(result), backends_used

    # Normalise HMDB IDs (old 5-digit → 7-digit format)
    if source_type == 'hmdb' and identifiers is not None:
        from omnipath_utils.mapping._special import normalise_hmdb
        identifiers = [normalise_hmdb(i) for i in identifiers]

    if full_uniprot == 'only':
        result, backends_used = defaultdict(set), set()
    else:
        result, backends_used = _query_table(
            session, f'{SCHEMA}.id_mapping',
            identifiers, source_type, target_type, ncbi_tax_id,
        )

    # Decide whether (and for which identifiers) to consult the full table.
    ftp_ids = identifiers
    want_ftp = full_uniprot in ('both', 'only')
    if full_uniprot == 'fallback' and identifiers is not None:
        missing = [i for i in identifiers if i not in result]
        if missing:
            want_ftp = True
            ftp_ids = missing

    # FR-018 gate: the ~744 M-row FTP table is 100% UniProt-family, so a pair
    # with a non-FTP type on either side (e.g. ``name → chebi``, ``chebi →
    # hmdb``) has zero rows there. Skip the fruitless scan -- result-identical
    # to the prior unconditional fallback, a pure latency win (R8).
    if want_ftp and not _ftp_relevant(
        source_type, target_type, _ftp_types(session)
    ):
        want_ftp = False

    if want_ftp and _ftp_table_exists(session):
        ftp_result, ftp_backends = _query_table(
            session, f'{SCHEMA}.{_FTP_TABLE}',
            ftp_ids, source_type, target_type, ncbi_tax_id,
        )
        for key, vals in ftp_result.items():
            result[key] |= vals  # set union -> deduplicated
        backends_used |= ftp_backends

    return dict(result), backends_used


def get_full_table(
    session: Session,
    source_type: str,
    target_type: str,
    ncbi_tax_id: int,
) -> dict[str, set[str]]:
    """Get a full mapping table from the database."""

    result, _ = translate_ids(session, None, source_type, target_type, ncbi_tax_id)

    return result


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

        # Also consult the long-value table (names / structures). Names are
        # stored lowercased, so match both verbatim and lowercased keys.
        long_rows = session.execute(
            text(f"""
                SELECT st.name, 'source' AS role, count(DISTINCT m.target_id)
                FROM {SCHEMA}.{_LONG_TABLE} m
                JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
                WHERE m.source_id IN (:id, :id_lower)
                GROUP BY st.name
                UNION ALL
                SELECT tt.name, 'target' AS role, count(DISTINCT m.source_id)
                FROM {SCHEMA}.{_LONG_TABLE} m
                JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
                WHERE m.target_id = :id
                GROUP BY tt.name
            """),
            {'id': identifier, 'id_lower': identifier.strip().lower()},
        ).fetchall()

        for row in long_rows:
            matches.append({
                'id_type': row[0],
                'role': row[1],
                'mappings_count': row[2],
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

    # Union the long-value table (names / structures). For a name source the
    # stored key is lowercased, so match verbatim or lowercased; results are
    # keyed back under the caller's original identifier.
    if source_type in LONG_VALUE_TYPES:
        key_map: dict[str, str] = {}
        for ident in identifiers:
            key_map[ident] = ident
            key_map[ident.strip().lower()] = ident
        long_rows = session.execute(
            text(f"""
                SELECT m.source_id, tt.name, m.target_id
                FROM {SCHEMA}.{_LONG_TABLE} m
                JOIN {SCHEMA}.id_type st ON m.source_type_id = st.id
                JOIN {SCHEMA}.id_type tt ON m.target_type_id = tt.id
                WHERE st.name = :src_type
                AND m.source_id = ANY(:ids)
                ORDER BY m.source_id, tt.name
            """),
            {'src_type': source_type, 'ids': list(key_map.keys())},
        ).fetchall()
        for src_id, tgt_type, tgt_id in long_rows:
            orig = key_map.get(src_id, src_id)
            bucket = result.setdefault(orig, {}).setdefault(tgt_type, [])
            if tgt_id not in bucket:
                bucket.append(tgt_id)

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
