"""Base class for mapping backends."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MappingBackend(ABC):
    """Abstract base for ID mapping data sources."""

    @abstractmethod
    def read(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        **kwargs: object,
    ) -> dict[str, set[str]]:
        """Read mapping data from this backend.

        Returns:
            Dict mapping source IDs to sets of target IDs.
        """

        ...
