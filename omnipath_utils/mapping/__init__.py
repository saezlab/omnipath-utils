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
) -> set[str]:
    """Translate a single identifier."""

    return Mapper.get().map_name(
        name, id_type, target_id_type, ncbi_tax_id,
    )


def map_names(
    names: object,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
) -> set[str]:
    """Translate multiple identifiers, return union of results."""

    return Mapper.get().map_names(
        names, id_type, target_id_type, ncbi_tax_id,
    )


def map_name0(
    name: str,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
) -> str | None:
    """Translate, returning a single result."""

    return Mapper.get().map_name0(
        name, id_type, target_id_type, ncbi_tax_id,
    )


def translate(
    identifiers: Iterable[str],
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
) -> dict[str, set[str]]:
    """Batch translate. Returns dict mapping source -> set of targets.

    Uses vectorized table lookup when available. Falls back to per-ID
    map_name (with full special-case handling) when the table is not
    available.
    """

    mapper = Mapper.get()
    ncbi_tax_id = ncbi_tax_id or mapper.ncbi_tax_id
    id_type_r = mapper._id_types.resolve(id_type) or id_type
    target_r = (
        mapper._id_types.resolve(target_id_type) or target_id_type
    )

    # Try vectorized: get table and do dict lookups
    table = mapper.which_table(id_type_r, target_r, ncbi_tax_id)

    if table:
        return {name: table[name] for name in identifiers}

    # Fallback: per-ID with full special-case handling
    return {
        name: mapper.map_name(
            name, id_type, target_id_type, ncbi_tax_id,
        )
        for name in identifiers
    }


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

    mapper = Mapper.get()
    ncbi_tax_id = ncbi_tax_id or mapper.ncbi_tax_id
    new_col = new_column or target_id_type
    id_type_r = mapper._id_types.resolve(id_type) or id_type
    target_r = (
        mapper._id_types.resolve(target_id_type) or target_id_type
    )

    # Vectorized: get the full translation table once
    trans = mapper.translation_table(id_type_r, target_r, ncbi_tax_id)

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
        )

    return df
