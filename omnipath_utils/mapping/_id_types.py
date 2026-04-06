"""ID type registry -- loads and queries id_types.yaml."""

from __future__ import annotations

from typing import Any
import logging

from pkg_infra.data import load

_log = logging.getLogger(__name__)


class IdTypeRegistry:
    """Registry of biological identifier types.

    Loads id_types.yaml and provides lookup by name, alias,
    backend column name, or entity type.
    """

    _instance: IdTypeRegistry | None = None

    def __init__(self) -> None:
        self._types: dict[str, dict] = load(
            'id_types.yaml',
            module='omnipath_utils',
        )
        self._build_indices()

    @classmethod
    def get(cls) -> IdTypeRegistry:
        """Singleton access."""

        if cls._instance is None:
            cls._instance = cls()

        return cls._instance

    def _build_indices(self) -> None:
        """Build lookup indices for fast queries."""

        self._by_alias: dict[str, str] = {}
        self._by_backend: dict[str, dict[str, str]] = {}

        for name, info in self._types.items():

            # Index aliases
            for alias in info.get('aliases', []):
                self._by_alias[alias] = name

            self._by_alias[name] = name

            # Also index with underscores/dashes normalized
            self._by_alias[name.replace('-', '_')] = name
            self._by_alias[name.replace('_', '-')] = name

            # Index backend columns -> our name
            for backend, col in info.get('backends', {}).items():
                if col:
                    self._by_backend.setdefault(backend, {})[col] = name

    def resolve(self, name: str) -> str | None:
        """Resolve an ID type name or alias to canonical name.

        Args:
            name: ID type name or alias.

        Returns:
            Canonical name, or None if not found.
        """

        return self._by_alias.get(name) or self._by_alias.get(name.lower())

    def info(self, name: str) -> dict[str, Any] | None:
        """Get full info dict for an ID type.

        Args:
            name: ID type name or alias.

        Returns:
            Info dict with label, entity_type, curie_prefix, backends,
            aliases; or None if not found.
        """

        canonical = self.resolve(name)

        return self._types.get(canonical) if canonical else None

    def entity_type(self, name: str) -> str | None:
        """Get entity type for an ID type.

        Args:
            name: ID type name or alias.

        Returns:
            Entity type string, or None.
        """

        info = self.info(name)

        return info.get('entity_type') if info else None

    def curie_prefix(self, name: str) -> str | None:
        """Get Bioregistry CURIE prefix.

        Args:
            name: ID type name or alias.

        Returns:
            CURIE prefix string, or None.
        """

        info = self.info(name)

        return info.get('curie_prefix') if info else None

    def backend_column(self, name: str, backend: str) -> str | None:
        """Get the backend-specific column name for an ID type.

        Args:
            name: ID type name or alias.
            backend: Backend name (e.g. 'uniprot', 'ensembl').

        Returns:
            Backend column name, or None.
        """

        info = self.info(name)

        if info:
            return info.get('backends', {}).get(backend)

        return None

    def from_backend_column(
        self,
        backend: str,
        column: str,
    ) -> str | None:
        """Look up our canonical name from a backend column name.

        Args:
            backend: Backend name.
            column: Backend-specific column name.

        Returns:
            Canonical ID type name, or None.
        """

        return self._by_backend.get(backend, {}).get(column)

    def by_entity_type(self, entity_type: str) -> list[str]:
        """List all ID types for a given entity type.

        Args:
            entity_type: One of protein, gene, transcript,
                small_molecule, mirna, probe.

        Returns:
            List of canonical ID type names.
        """

        return [
            name
            for name, info in self._types.items()
            if info.get('entity_type') == entity_type
        ]

    def by_backend(self, backend: str) -> dict[str, str]:
        """Map of our ID type names to backend column names for a backend.

        Args:
            backend: Backend name.

        Returns:
            Dict mapping canonical name to backend column name.
        """

        return {
            name: info['backends'][backend]
            for name, info in self._types.items()
            if backend in info.get('backends', {})
            and info['backends'][backend]
        }

    def all_names(self) -> list[str]:
        """List all canonical ID type names."""

        return list(self._types.keys())

    def __contains__(self, name: str) -> bool:
        return self.resolve(name) is not None

    def __len__(self) -> int:
        return len(self._types)

    def __repr__(self) -> str:
        return f'<IdTypeRegistry [{len(self)} types]>'
