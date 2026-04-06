"""Orthology -- cross-species gene translation.

Translates identifiers between organisms using orthologous gene pairs
from OMA, Ensembl Compara, HomoloGene, and HCOP.

Example::

    from omnipath_utils.orthology import translate, translate_column

    # Human TP53 -> mouse orthologs
    translate(['TP53'], source=9606, target=10090)
    # {'TP53': {'Trp53'}}

    # Translate a DataFrame column
    translate_column(df, 'gene', source=9606, target=10090)
"""

from __future__ import annotations

from omnipath_utils._constants import DEFAULT_ORGANISM


def translate(
    identifiers: list[str] | set[str],
    source: int = 9606,
    target: int = 10090,
    id_type: str = 'genesymbol',
    only_swissprot: bool = True,
    resource: str | None = None,
    min_sources: int = 1,
    rel_type: set[str] | None = None,
    raw: bool = False,
) -> dict[str, set[str]]:
    """Translate identifiers to orthologs in another organism.

    Args:
        identifiers: Source identifiers.
        source: Source organism NCBI Taxonomy ID (default: 9606 human).
        target: Target organism NCBI Taxonomy ID (default: 10090 mouse).
        id_type: Identifier type (default: 'genesymbol').
        only_swissprot: Prefer SwissProt for UniProt IDs.
        resource: Force specific resource ('oma', 'ensembl', 'homologene', 'hcop').
        min_sources: For HCOP, minimum number of supporting databases.
        rel_type: Filter by relationship type (e.g. {'1:1'} for one-to-one).
        raw: Skip post-processing (ID translation, SwissProt preference).

    Returns:
        Dict mapping source IDs to sets of ortholog IDs.
    """
    from omnipath_utils.orthology._manager import OrthologyManager

    mgr = OrthologyManager.get()
    return mgr.translate(
        identifiers=list(identifiers),
        source=source,
        target=target,
        id_type=id_type,
        only_swissprot=only_swissprot,
        resource=resource,
        min_sources=min_sources,
        rel_type=rel_type,
        raw=raw,
    )


def translate_column(
    df,
    column: str,
    source: int = 9606,
    target: int = 10090,
    id_type: str = 'genesymbol',
    new_column: str | None = None,
    keep_untranslated: bool = True,
    expand: bool = True,
    only_swissprot: bool = True,
    resource: str | None = None,
    min_sources: int = 1,
):
    """Translate a DataFrame column to orthologs.

    Args:
        df: pandas DataFrame.
        column: Column with source identifiers.
        source: Source organism.
        target: Target organism.
        id_type: Identifier type.
        new_column: Output column name (default: '{column}_{target}').
        keep_untranslated: Keep rows with no ortholog.
        expand: Expand one-to-many to multiple rows.
        only_swissprot: Prefer SwissProt for UniProt IDs.
        resource: Force specific resource.
        min_sources: For HCOP, minimum supporting databases.
    """
    import pandas as pd

    new_col = new_column or f'{column}_{target}'

    # Get translation dict
    source_ids = df[column].dropna().unique().tolist()
    trans = translate(
        source_ids,
        source=source,
        target=target,
        id_type=id_type,
        only_swissprot=only_swissprot,
        resource=resource,
        min_sources=min_sources,
    )

    if expand:
        df = df.copy()
        df['_orthologs'] = df[column].map(
            lambda x: (
                sorted(trans.get(x, set()))
                if x and trans.get(x)
                else [None]
                if keep_untranslated
                else []
            )
        )
        df = df.explode('_orthologs').rename(columns={'_orthologs': new_col})
        if not keep_untranslated:
            df = df.dropna(subset=[new_col])
        df = df.reset_index(drop=True)
    else:
        df = df.copy()
        df[new_col] = df[column].map(
            lambda x: (
                next(iter(trans.get(x, set()))) if x and trans.get(x) else None
            )
        )
        if not keep_untranslated:
            df = df.dropna(subset=[new_col])

    return df


def get_table(
    source: int = 9606,
    target: int = 10090,
    id_type: str = 'genesymbol',
    resource: str | None = None,
    min_sources: int = 1,
) -> dict[str, set[str]]:
    """Get the full orthology table for a pair of organisms."""
    from omnipath_utils.orthology._manager import OrthologyManager

    return OrthologyManager.get().get_table(
        source=source,
        target=target,
        id_type=id_type,
        resource=resource,
        min_sources=min_sources,
    )
