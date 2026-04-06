"""Tests for the mapping module."""

import time

import pytest
from unittest.mock import patch, MagicMock
from omnipath_utils.mapping._table import MappingTable, MappingTableKey
from omnipath_utils.mapping._mapper import Mapper
from omnipath_utils.mapping._cleanup import is_uniprot_ac, uniprot_cleanup
from omnipath_utils.mapping._special import (
    map_genesymbol_fallbacks,
    map_refseq,
    map_ensembl_strip_version,
    chain_map,
)


# ---- MappingTable tests ----


class TestMappingTable:

    def test_create(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        assert len(table) == 4

    def test_lookup(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        assert table["P04637"] == {"TP53"}
        assert table["NONEXISTENT"] == set()

    def test_contains(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        assert "P04637" in table
        assert "FAKE" not in table

    def test_key(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        assert table.key == MappingTableKey("uniprot", "genesymbol", 9606)

    def test_expiry(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
            lifetime=0,
        )
        time.sleep(0.01)
        assert table.expired

    def test_no_expiry(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
            lifetime=3600,
        )
        assert not table.expired

    def test_repr(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        r = repr(table)
        assert "uniprot" in r
        assert "genesymbol" in r
        assert "9606" in r

    def test_items_keys_values(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        assert set(table.keys()) == {"P04637", "P00533", "P38398", "Q13315"}
        assert {"TP53"} in list(table.values())
        assert len(list(table.items())) == 4

    def test_last_used_updates_on_access(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
            lifetime=3600,
        )
        before = table._last_used
        time.sleep(0.01)
        _ = table["P04637"]
        assert table._last_used > before

    def test_last_used_updates_on_contains(self, sample_mapping_data):
        table = MappingTable(
            data=sample_mapping_data,
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
            lifetime=3600,
        )
        before = table._last_used
        time.sleep(0.01)
        _ = "P04637" in table
        assert table._last_used > before


# ---- MappingTableKey tests ----


class TestMappingTableKey:

    def test_named_tuple(self):
        key = MappingTableKey("uniprot", "genesymbol", 9606)
        assert key.id_type == "uniprot"
        assert key.target_id_type == "genesymbol"
        assert key.ncbi_tax_id == 9606

    def test_equality(self):
        k1 = MappingTableKey("uniprot", "genesymbol", 9606)
        k2 = MappingTableKey("uniprot", "genesymbol", 9606)
        assert k1 == k2

    def test_inequality(self):
        k1 = MappingTableKey("uniprot", "genesymbol", 9606)
        k2 = MappingTableKey("uniprot", "genesymbol", 10090)
        assert k1 != k2

    def test_hashable(self):
        k = MappingTableKey("uniprot", "genesymbol", 9606)
        d = {k: "value"}
        assert d[k] == "value"


# ---- Cleanup tests ----


class TestUniProtCleanup:

    def test_valid_ac(self):
        assert is_uniprot_ac("P04637")
        assert is_uniprot_ac("Q9Y6K9")
        assert is_uniprot_ac("A0A024R1R8")

    def test_invalid_ac(self):
        assert not is_uniprot_ac("TP53")
        assert not is_uniprot_ac("ENSG00000141510")
        assert not is_uniprot_ac("")
        assert not is_uniprot_ac("12345")

    def test_cleanup_filters_invalid(self):
        mock_mapper = MagicMock()
        mock_mapper._direct_lookup = MagicMock(return_value=set())
        result = uniprot_cleanup({"P04637", "TP53", "FAKE"}, 9606, mapper=mock_mapper)
        assert "P04637" in result
        assert "TP53" not in result
        assert "FAKE" not in result

    def test_cleanup_empty_input(self):
        result = uniprot_cleanup(set(), 9606)
        assert result == set()


# ---- Special case tests ----


class TestSpecialCases:

    def _make_mock_mapper(self, tables):
        """Create a mock mapper with preloaded tables."""
        mapper = MagicMock()

        def direct_lookup(name, id_type, target_id_type, ncbi_tax_id):
            key = (id_type, target_id_type, ncbi_tax_id)
            data = tables.get(key, {})
            return data.get(name, set())

        mapper._direct_lookup = direct_lookup
        mapper.map_name = MagicMock(return_value=set())
        return mapper

    def test_genesymbol_case_fallback_upper(self):
        tables = {
            ("genesymbol", "uniprot", 9606): {
                "TP53": {"P04637"},
                "EGFR": {"P00533"},
            },
            ("genesymbol-syn", "uniprot", 9606): {},
        }
        mapper = self._make_mock_mapper(tables)

        # lowercase should find uppercase via .upper()
        result = map_genesymbol_fallbacks("tp53", "uniprot", 9606, mapper)
        assert "P04637" in result

    def test_genesymbol_case_fallback_capitalize(self):
        tables = {
            ("genesymbol", "uniprot", 10090): {
                "Trp53": {"Q01279"},
            },
            ("genesymbol-syn", "uniprot", 10090): {},
        }
        mapper = self._make_mock_mapper(tables)

        # all-lower should capitalize and match rodent-style Trp53
        result = map_genesymbol_fallbacks("trp53", "uniprot", 10090, mapper)
        assert "Q01279" in result

    def test_genesymbol_synonym_fallback(self):
        tables = {
            ("genesymbol", "uniprot", 9606): {},
            ("genesymbol-syn", "uniprot", 9606): {
                "P53": {"P04637"},
            },
        }
        mapper = self._make_mock_mapper(tables)

        result = map_genesymbol_fallbacks("P53", "uniprot", 9606, mapper)
        assert "P04637" in result

    def test_genesymbol_strict_mode(self):
        tables = {
            ("genesymbol", "uniprot", 9606): {},
            ("genesymbol-syn", "uniprot", 9606): {},
        }
        mapper = self._make_mock_mapper(tables)

        # strict mode should not try appending "1" or truncating
        result = map_genesymbol_fallbacks(
            "SOMEGENE", "uniprot", 9606, mapper, strict=True,
        )
        assert result == set()

    def test_refseq_version_strip(self):
        tables = {
            ("refseqp", "uniprot", 9606): {
                "NP_000537": {"P04637"},
            },
        }
        mapper = self._make_mock_mapper(tables)

        # with version should strip and find
        result = map_refseq("NP_000537.3", "refseqp", "uniprot", 9606, mapper)
        assert "P04637" in result

    def test_refseq_exact_match(self):
        tables = {
            ("refseqp", "uniprot", 9606): {
                "NP_000537.3": {"P04637"},
            },
        }
        mapper = self._make_mock_mapper(tables)

        # exact match with version present
        result = map_refseq(
            "NP_000537.3", "refseqp", "uniprot", 9606, mapper,
        )
        assert "P04637" in result

    def test_ensembl_version_strip(self):
        tables = {
            ("ensg", "genesymbol", 9606): {
                "ENSG00000141510": {"TP53"},
            },
        }
        mapper = self._make_mock_mapper(tables)

        result = map_ensembl_strip_version(
            "ENSG00000141510.12", "ensg", "genesymbol", 9606, mapper,
        )
        assert "TP53" in result

    def test_ensembl_no_version_returns_empty(self):
        tables = {
            ("ensg", "genesymbol", 9606): {},
        }
        mapper = self._make_mock_mapper(tables)

        # No dot in the name -> no stripping, returns empty
        result = map_ensembl_strip_version(
            "ENSG00000141510", "ensg", "genesymbol", 9606, mapper,
        )
        assert result == set()

    def test_chain_map(self):
        mapper = MagicMock()
        # First call: genesymbol -> uniprot
        # Second call: uniprot -> entrez
        mapper.map_name = MagicMock(
            side_effect=[
                {"P04637"},  # TP53 -> uniprot
                {"7157"},  # P04637 -> entrez
            ],
        )

        result = chain_map("TP53", "genesymbol", "entrez", 9606, mapper)
        assert "7157" in result

    def test_chain_map_no_intermediate(self):
        mapper = MagicMock()
        # No intermediate result
        mapper.map_name = MagicMock(return_value=set())

        result = chain_map(
            "NONEXIST", "genesymbol", "entrez", 9606, mapper,
        )
        assert result == set()


# ---- Mapper tests with mocked backends ----


class TestMapper:

    def setup_method(self):
        """Reset singleton for test isolation."""
        Mapper._instance = None

    def test_mapper_creation(self):
        mapper = Mapper(ncbi_tax_id=9606)
        assert mapper.ncbi_tax_id == 9606
        assert len(mapper.tables) == 0

    def test_default_organism(self):
        mapper = Mapper()
        assert mapper.ncbi_tax_id == 9606

    def test_same_type_returns_self(self):
        mapper = Mapper()
        result = mapper.map_name("P04637", "uniprot", "uniprot")
        assert result == {"P04637"}

    def test_empty_name_returns_empty(self):
        mapper = Mapper()
        result = mapper.map_name("", "uniprot", "genesymbol")
        assert result == set()

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_map_name_with_preloaded_table(self, mock_load):
        mapper = Mapper()

        # Preload a table
        table = MappingTable(
            data={"P04637": {"TP53"}, "P00533": {"EGFR"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        mapper.tables[table.key] = table

        result = mapper.map_name("P04637", "uniprot", "genesymbol")
        assert result == {"TP53"}

        # Backend should not have been called
        mock_load.assert_not_called()

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_map_names(self, mock_load):
        mapper = Mapper()

        table = MappingTable(
            data={"P04637": {"TP53"}, "P00533": {"EGFR"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        mapper.tables[table.key] = table

        result = mapper.map_names(
            ["P04637", "P00533"], "uniprot", "genesymbol",
        )
        assert "TP53" in result
        assert "EGFR" in result

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_translate_batch(self, mock_load):
        mapper = Mapper()

        table = MappingTable(
            data={"P04637": {"TP53"}, "P00533": {"EGFR"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        mapper.tables[table.key] = table

        result = mapper.translate(
            ["P04637", "P00533", "FAKE"], "uniprot", "genesymbol",
        )
        assert result["P04637"] == {"TP53"}
        assert result["P00533"] == {"EGFR"}
        assert result["FAKE"] == set()

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_map_name0(self, mock_load):
        mapper = Mapper()

        table = MappingTable(
            data={"P04637": {"TP53"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        mapper.tables[table.key] = table

        result = mapper.map_name0("P04637", "uniprot", "genesymbol")
        assert result == "TP53"

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_map_name0_missing(self, mock_load):
        mock_load.return_value = None
        mapper = Mapper()

        result = mapper.map_name0("FAKE", "uniprot", "genesymbol")
        assert result is None

    def test_remove_expired(self):
        mapper = Mapper()

        table = MappingTable(
            data={"P04637": {"TP53"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
            lifetime=0,
        )
        mapper.tables[table.key] = table

        time.sleep(0.01)
        mapper.remove_expired()
        assert len(mapper.tables) == 0

    def test_remove_expired_keeps_fresh(self):
        mapper = Mapper()

        table = MappingTable(
            data={"P04637": {"TP53"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
            lifetime=3600,
        )
        mapper.tables[table.key] = table

        mapper.remove_expired()
        assert len(mapper.tables) == 1

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_which_table_no_load(self, mock_load):
        mapper = Mapper()
        result = mapper.which_table(
            "uniprot", "genesymbol", 9606, load=False,
        )
        assert result is None
        mock_load.assert_not_called()

    @patch("omnipath_utils.mapping._mapper.Mapper._load_table")
    def test_translation_table(self, mock_load):
        mapper = Mapper()

        table = MappingTable(
            data={"P04637": {"TP53"}, "P00533": {"EGFR"}},
            id_type="uniprot",
            target_id_type="genesymbol",
            ncbi_tax_id=9606,
        )
        mapper.tables[table.key] = table

        result = mapper.translation_table("uniprot", "genesymbol")
        assert isinstance(result, dict)
        assert result["P04637"] == {"TP53"}

    def test_repr(self):
        mapper = Mapper()
        r = repr(mapper)
        assert "Mapper" in r
        assert "9606" in r

    def test_id_types(self):
        mapper = Mapper()
        types = mapper.id_types()
        assert "uniprot" in types
        assert "genesymbol" in types
        assert len(types) > 50
