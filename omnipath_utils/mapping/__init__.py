"""ID translation -- the core service of omnipath-utils.

Example::

    from omnipath_utils.mapping import map_name, map_names

    # Translate gene symbol to UniProt
    map_name('TP53', 'genesymbol', 'uniprot')
    # {'P04637'}

    # Translate multiple
    map_names(['TP53', 'EGFR'], 'genesymbol', 'uniprot')
    # {'P04637', 'P00533'}
"""

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
    identifiers: object,
    id_type: str,
    target_id_type: str,
    ncbi_tax_id: int | None = None,
) -> dict[str, set[str]]:
    """Batch translate. Returns dict mapping source -> set of targets."""

    return Mapper.get().translate(
        identifiers, id_type, target_id_type, ncbi_tax_id,
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
