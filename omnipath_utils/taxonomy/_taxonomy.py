#!/usr/bin/env python

#
# This file is part of the `omnipath_utils` Python module
#
# Copyright 2026
# Heidelberg University Hospital
#
# File author(s): Denes Turei (turei.denes@gmail.com)
#
# Distributed under the GPL-3.0-or-later license
# See the file `LICENSE` or read a copy at
# https://www.gnu.org/licenses/gpl-3.0.txt
#

"""Taxonomy manager -- organism name resolution and conversion."""

from __future__ import annotations

import logging

from pkg_infra.data import load

_log = logging.getLogger(__name__)

# Type alias for anything that can identify an organism
TaxonInput = int | str


class TaxonomyManager:
    """Resolve organism identifiers across naming systems.

    Loads organisms.yaml as seed data. Additional organisms can be
    loaded lazily from UniProt/Ensembl/KEGG APIs on demand.
    """

    _instance: TaxonomyManager | None = None

    def __init__(self) -> None:
        raw = load('organisms.yaml', module='omnipath_utils')
        self._by_taxid: dict[int, dict] = {int(k): v for k, v in raw.items()}
        self._build_indices()

    @classmethod
    def get(cls) -> TaxonomyManager:
        """Return the singleton instance, creating it if needed."""

        if cls._instance is None:
            cls._instance = cls()

        return cls._instance

    def _build_indices(self) -> None:
        """Build reverse lookup indices from all name forms."""

        self._to_taxid: dict[str, int] = {}

        for taxid, info in self._by_taxid.items():
            for field in (
                'common_name',
                'latin_name',
                'short_latin',
                'ensembl_name',
                'kegg_code',
                'mirbase_code',
                'oma_code',
                'uniprot_code',
                'dbptm_code',
            ):
                val = info.get(field, '')

                if val:
                    self._to_taxid[val.lower()] = taxid

            # Also index the taxid as string
            self._to_taxid[str(taxid)] = taxid

    def ensure_ncbi_tax_id(self, taxon: TaxonInput) -> int | None:
        """Convert any organism representation to NCBI Taxonomy ID.

        Args:
            taxon:
                Organism name or NCBI Taxonomy ID in any supported
                form: common name, latin name, Ensembl name, KEGG
                code, miRBase code, OMA code, UniProt code, dbPTM
                code, or integer/string taxonomy ID.

        Returns:
            NCBI Taxonomy ID, or None if the organism is not found.
        """

        if isinstance(taxon, int):
            return taxon if taxon in self._by_taxid else None

        if isinstance(taxon, str):
            # Try as integer string
            try:
                tid = int(taxon)
                return tid if tid in self._by_taxid else None
            except ValueError:
                pass

            # Try all name forms (case-insensitive)
            return self._to_taxid.get(taxon.lower())

        return None

    def _get_field(
        self,
        taxon: TaxonInput,
        field: str,
    ) -> str | None:
        """Get a specific field for an organism.

        Args:
            taxon:
                Organism identifier in any supported form.
            field:
                Field name from organisms.yaml.

        Returns:
            The field value, or None if not found.
        """

        taxid = self.ensure_ncbi_tax_id(taxon)

        if taxid is None:
            return None

        info = self._by_taxid.get(taxid)

        return info.get(field) if info else None

    def ensure_common_name(self, taxon: TaxonInput) -> str | None:
        """Common English name for an organism.

        Args:
            taxon:
                Organism identifier in any supported form.

        Returns:
            Common name string, or None if not found.
        """

        return self._get_field(taxon, 'common_name')

    def ensure_latin_name(self, taxon: TaxonInput) -> str | None:
        """Full latin (scientific) name for an organism.

        Args:
            taxon:
                Organism identifier in any supported form.

        Returns:
            Latin name string, or None if not found.
        """

        return self._get_field(taxon, 'latin_name')

    def ensure_ensembl_name(self, taxon: TaxonInput) -> str | None:
        """Ensembl organism name.

        Args:
            taxon:
                Organism identifier in any supported form.

        Returns:
            Ensembl name string, or None if not found.
        """

        return self._get_field(taxon, 'ensembl_name')

    def ensure_kegg_code(self, taxon: TaxonInput) -> str | None:
        """KEGG three-letter organism code.

        Args:
            taxon:
                Organism identifier in any supported form.

        Returns:
            KEGG code string, or None if not found.
        """

        return self._get_field(taxon, 'kegg_code')

    def ensure_mirbase_name(self, taxon: TaxonInput) -> str | None:
        """MiRBase three-letter organism code.

        Args:
            taxon:
                Organism identifier in any supported form.

        Returns:
            miRBase code string, or None if not found.
        """

        return self._get_field(taxon, 'mirbase_code')

    def ensure_oma_code(self, taxon: TaxonInput) -> str | None:
        """OMA five-letter organism code.

        Args:
            taxon:
                Organism identifier in any supported form.

        Returns:
            OMA code string, or None if not found.
        """

        return self._get_field(taxon, 'oma_code')

    def all_organisms(self) -> dict[int, dict]:
        """Return all known organisms.

        Returns:
            Dict mapping NCBI Taxonomy IDs to organism info dicts.
        """

        return dict(self._by_taxid)

    def __contains__(self, taxon: TaxonInput) -> bool:
        return self.ensure_ncbi_tax_id(taxon) is not None

    def __len__(self) -> int:
        return len(self._by_taxid)
