"""Mapper -- central ID translation manager."""

from __future__ import annotations

import os
import re
import logging
import threading
from typing import Iterable

from omnipath_utils._constants import DEFAULT_ORGANISM
from omnipath_utils.mapping._table import MappingTable, MappingTableKey
from omnipath_utils.mapping._reader import MapReader
from omnipath_utils.mapping._id_types import IdTypeRegistry

_log = logging.getLogger(__name__)

# UniProt AC format
RE_UNIPROT = re.compile(
    r'^[OPQ][0-9][A-Z0-9]{3}[0-9]$'
    r'|^[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2}$',
)


class Mapper:
    """Central ID translation manager.

    Manages loaded MappingTables, dispatches loading to MapReader,
    and provides the public translation API.

    In memory mode (default): downloads data from upstream APIs,
    caches locally in pickle files, holds translation dicts in memory.
    """

    _instance: Mapper | None = None
    _lock = threading.Lock()

    def __init__(
        self,
        ncbi_tax_id: int | None = None,
        lifetime: int = 300,
        cachedir: str | None = None,
    ):
        self.ncbi_tax_id = ncbi_tax_id or DEFAULT_ORGANISM
        self.lifetime = lifetime
        self._cachedir = cachedir or self._default_cachedir()
        self.tables: dict[MappingTableKey, MappingTable] = {}
        self._id_types = IdTypeRegistry.get()

    @classmethod
    def get(cls, **kwargs: object) -> Mapper:
        """Singleton access."""

        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(**kwargs)

        return cls._instance

    @staticmethod
    def _default_cachedir() -> str:
        """Default cache directory for mapping pickles."""

        import platformdirs

        return os.path.join(
            platformdirs.user_cache_dir('omnipath_utils'),
            'mapping',
        )

    def map_name(
        self,
        name: str,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int | None = None,
    ) -> set[str]:
        """Translate a single identifier.

        Args:
            name: The identifier to translate.
            id_type: Source ID type (e.g. 'genesymbol').
            target_id_type: Target ID type (e.g. 'uniprot').
            ncbi_tax_id: Organism. Defaults to human (9606).

        Returns:
            Set of target identifiers. Empty set if no mapping found.
        """

        ncbi_tax_id = ncbi_tax_id or self.ncbi_tax_id

        id_type = self._id_types.resolve(id_type) or id_type
        target_id_type = (
            self._id_types.resolve(target_id_type) or target_id_type
        )

        if id_type == target_id_type:
            return {name}

        # Forward lookup
        table = self.which_table(id_type, target_id_type, ncbi_tax_id)

        if table:
            result = table[name]

            if result:
                return result

        # Reverse lookup
        table = self.which_table(
            target_id_type,
            id_type,
            ncbi_tax_id,
        )

        if table:
            result = set()

            for src, targets in table.data.items():
                if name in targets:
                    result.add(src)

            if result:
                return result

        return set()

    def map_names(
        self,
        names: Iterable[str],
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int | None = None,
    ) -> set[str]:
        """Translate multiple identifiers, return union of results."""

        result = set()

        for name in names:
            result.update(
                self.map_name(name, id_type, target_id_type, ncbi_tax_id),
            )

        return result

    def map_name0(
        self,
        name: str,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int | None = None,
    ) -> str | None:
        """Translate, returning a single result.

        If the mapping is ambiguous, an arbitrary result is picked.
        """

        result = self.map_name(
            name, id_type, target_id_type, ncbi_tax_id,
        )

        return next(iter(result)) if result else None

    def translate(
        self,
        identifiers: Iterable[str],
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int | None = None,
    ) -> dict[str, set[str]]:
        """Batch translate. Returns dict mapping source -> set of targets."""

        ncbi_tax_id = ncbi_tax_id or self.ncbi_tax_id
        id_type = self._id_types.resolve(id_type) or id_type
        target_id_type = (
            self._id_types.resolve(target_id_type) or target_id_type
        )

        table = self.which_table(id_type, target_id_type, ncbi_tax_id)

        if not table:
            return {name: set() for name in identifiers}

        return {name: table[name] for name in identifiers}

    def which_table(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int | None = None,
        load: bool = True,
    ) -> MappingTable | None:
        """Find or load the appropriate mapping table."""

        ncbi_tax_id = ncbi_tax_id or self.ncbi_tax_id
        key = MappingTableKey(id_type, target_id_type, ncbi_tax_id)

        if key in self.tables:
            return self.tables[key]

        if not load:
            return None

        table = self._load_table(id_type, target_id_type, ncbi_tax_id)

        if table:
            self.tables[key] = table

        return table

    def _load_table(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
    ) -> MappingTable | None:
        """Try loading a table from available backends."""

        backends = self._find_backends(id_type, target_id_type)

        for backend_name in backends:
            try:
                reader = MapReader(
                    id_type=id_type,
                    target_id_type=target_id_type,
                    ncbi_tax_id=ncbi_tax_id,
                    backend=backend_name,
                    cachedir=self._cachedir,
                )
                data = reader.load()

                if data:
                    return MappingTable(
                        data=data,
                        id_type=id_type,
                        target_id_type=target_id_type,
                        ncbi_tax_id=ncbi_tax_id,
                        lifetime=self.lifetime,
                    )
            except Exception as e:
                _log.warning(
                    'Failed to load %s -> %s from %s: %s',
                    id_type,
                    target_id_type,
                    backend_name,
                    e,
                )

        return None

    def _find_backends(
        self,
        id_type: str,
        target_id_type: str,
    ) -> list[str]:
        """Find backends that can provide this ID type pair."""

        backends = []

        for backend_name in ('uniprot',):
            src_col = self._id_types.backend_column(
                id_type, backend_name,
            )
            tgt_col = self._id_types.backend_column(
                target_id_type, backend_name,
            )

            if src_col and tgt_col:
                backends.append(backend_name)

        return backends

    def translation_table(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int | None = None,
    ) -> dict[str, set[str]]:
        """Get the full translation table as a dict."""

        ncbi_tax_id = ncbi_tax_id or self.ncbi_tax_id
        id_type = self._id_types.resolve(id_type) or id_type
        target_id_type = (
            self._id_types.resolve(target_id_type) or target_id_type
        )

        table = self.which_table(id_type, target_id_type, ncbi_tax_id)

        return dict(table.data) if table else {}

    def remove_expired(self) -> None:
        """Remove tables that have not been used recently."""

        expired = [k for k, t in self.tables.items() if t.expired]

        for k in expired:
            _log.debug('Removing expired table: %s', k)
            del self.tables[k]

    def id_types(self) -> list[str]:
        """List all known ID types."""

        return self._id_types.all_names()

    def __repr__(self) -> str:
        return (
            f'<Mapper tables={len(self.tables)}, '
            f'organism={self.ncbi_tax_id}>'
        )
