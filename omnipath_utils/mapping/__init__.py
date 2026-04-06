"""ID translation -- the core service of omnipath-utils.

Example::

    from omnipath_utils.mapping import map_name, map_names

    # Translate gene symbol to UniProt
    map_name('TP53', 'genesymbol', 'uniprot')
    # {'P04637'}

    # Translate multiple
    map_names(['TP53', 'EGFR'], 'genesymbol', 'uniprot')
    # {'P04637', 'P00533'}

    # Translate a DataFrame column
    import pandas as pd
    from omnipath_utils.mapping import translate_column

    df = pd.DataFrame({'protein': ['P04637', 'P00533']})
    translate_column(df, 'protein', 'uniprot', 'genesymbol')
"""

from __future__ import annotations

from typing import Any, Iterable

from omnipath_utils.mapping._mapper import Mapper


def map_name(
    name: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
    strict: bool = False,
    raw: bool = False,
    backend: str | None = None,
) -> set[str]:
    """Translate a single identifier.

    Args:
        name: The identifier to translate.
        id_type: Source ID type (e.g. 'genesymbol').
        target_id_type: Target ID type (e.g. 'uniprot').
        ncbi_tax_id: Organism (default: 9606).
        strict: Skip fuzzy fallbacks (gene symbol case, append "1", etc.).
        raw: Skip special-case handling. Direct table lookup only.
        backend: Force a specific backend (e.g. 'uniprot', 'biomart').

    Returns:
        Set of target identifiers.
    """

    from omnipath_utils.mapping._translate import translate_core

    result = translate_core(
        [name], id_type, target_id_type, ncbi_tax_id,
        strict=strict, raw=raw, backend=backend,
    )
    return result.get(name, set())


def map_names(
    names: object,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
    raw: bool = False,
    backend: str | None = None,
) -> set[str]:
    """Translate multiple identifiers, return union of results.

    Args:
        names: Iterable of identifiers.
        id_type: Source ID type.
        target_id_type: Target ID type.
        ncbi_tax_id: Organism (default: 9606).
        raw: Skip special-case handling. Direct table lookup only.
        backend: Force a specific backend.

    Returns:
        Union of all target identifiers.
    """

    from omnipath_utils.mapping._translate import translate_core

    result = translate_core(
        list(names), id_type, target_id_type, ncbi_tax_id,
        raw=raw, backend=backend,
    )
    merged = set()
    for targets in result.values():
        merged.update(targets)
    return merged


def map_name0(
    name: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
    raw: bool = False,
    backend: str | None = None,
) -> str | None:
    """Translate, returning a single result.

    Args:
        name: The identifier to translate.
        id_type: Source ID type.
        target_id_type: Target ID type.
        ncbi_tax_id: Organism (default: 9606).
        raw: Skip special-case handling.
        backend: Force a specific backend.

    Returns:
        A single target identifier, or None.
    """

    result = map_name(
        name, id_type, target_id_type, ncbi_tax_id,
        raw=raw, backend=backend,
    )
    return next(iter(result)) if result else None


def translate(
    identifiers: Iterable[str],
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
    raw: bool = False,
    backend: str | None = None,
) -> dict[str, set[str]]:
    """Batch translate. Returns dict mapping source -> set of targets.

    Uses vectorized table lookup when available. Falls back to per-ID
    map_name (with full special-case handling) when the table is not
    available.

    Args:
        identifiers: Iterable of source identifiers.
        id_type: Source ID type.
        target_id_type: Target ID type.
        ncbi_tax_id: Organism (default: 9606).
        raw: Skip special-case handling. Direct table lookup only.
        backend: Force a specific backend.

    Returns:
        Dict mapping each source ID to a set of target IDs.
    """

    from omnipath_utils.mapping._translate import translate_core

    return translate_core(
        list(identifiers), id_type, target_id_type, ncbi_tax_id,
        raw=raw, backend=backend,
    )


def translation_table(
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
) -> dict[str, set[str]]:
    """Get the full translation table."""

    return Mapper.get().translation_table(
        id_type, target_id_type, ncbi_tax_id,
    )


def id_types() -> list[str]:
    """List all known ID types."""

    return Mapper.get().id_types()


def translate_column(
    df: Any,
    column: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
    new_column: str | None = None,
    keep_untranslated: bool = True,
    expand: bool = True,
    raw: bool = False,
    backend: str | None = None,
) -> Any:
    """Translate a column of identifiers in a DataFrame.

    Performs vectorized lookup using the full mapping table,
    not per-row translation.

    Args:
        df: pandas DataFrame.
        column: Column name with source IDs.
        id_type: Source ID type.
        target_id_type: Target ID type.
        ncbi_tax_id: Organism (default: 9606).
        new_column: Output column name (default: target_id_type).
        keep_untranslated: Keep rows with no translation (default: True).
        expand: Expand one-to-many to multiple rows (default: True).
        raw: Skip special-case handling. Direct table lookup only.
        backend: Force a specific backend.

    Returns:
        pandas DataFrame with translated column added.

    Raises:
        ImportError: If pandas is not installed.
    """

    try:
        import pandas as pd
    except ImportError:
        raise ImportError(
            'pandas is required for translate_column. '
            'Install: pip install pandas'
        )

    from omnipath_utils.mapping._translate import translate_core

    mapper = Mapper.get()
    ncbi_tax_id = ncbi_tax_id or mapper.ncbi_tax_id
    new_col = new_column or target_id_type

    # Get unique IDs for batch translation
    unique_ids = [
        str(v) for v in df[column].dropna().unique()
    ]

    trans = translate_core(
        unique_ids, id_type, target_id_type, ncbi_tax_id,
        raw=raw, backend=backend,
    )

    if expand:
        # Map each value to a list of targets
        df = df.copy()
        df['_targets'] = df[column].map(
            lambda x: sorted(trans.get(x, set()))
            if x and trans.get(x)
            else ([None] if keep_untranslated else [])
        )
        # Explode to multiple rows
        df = df.explode('_targets').rename(
            columns={'_targets': new_col},
        )

        if not keep_untranslated:
            df = df.dropna(subset=[new_col])

        df = df.reset_index(drop=True)
    else:
        # Pick first result
        df = df.copy()
        df[new_col] = df[column].map(
            lambda x: next(iter(trans.get(x, set())))
            if x and trans.get(x)
            else None
        )

        if not keep_untranslated:
            df = df.dropna(subset=[new_col])

    return df


def translate_columns(
    df: Any,
    *translations: tuple[str, ...],
    ncbi_tax_id: int | None = None,
    keep_untranslated: bool = True,
    expand: bool = True,
    raw: bool = False,
    backend: str | None = None,
) -> Any:
    """Translate multiple columns in a DataFrame.

    Each translation is a tuple of (column, id_type, target_id_type)
    or (column, id_type, target_id_type, new_column_name).

    Args:
        df: pandas DataFrame.
        *translations: Tuples defining translations.
        ncbi_tax_id: Organism (default: 9606).
        keep_untranslated: Keep rows with no translation.
        expand: Expand one-to-many mappings.
        raw: Skip special-case handling. Direct table lookup only.
        backend: Force a specific backend.

    Returns:
        pandas DataFrame with all translated columns added.

    Example::

        translate_columns(
            df,
            ('uniprot', 'uniprot', 'genesymbol'),
            ('uniprot', 'uniprot', 'entrez', 'entrez_id'),
        )
    """

    for t in translations:
        col, src, tgt = t[0], t[1], t[2]
        new_col = t[3] if len(t) > 3 else None
        df = translate_column(
            df, col, src, tgt,
            ncbi_tax_id=ncbi_tax_id,
            new_column=new_col,
            keep_untranslated=keep_untranslated,
            expand=expand,
            raw=raw,
            backend=backend,
        )

    return df
