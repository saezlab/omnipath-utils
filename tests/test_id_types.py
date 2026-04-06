"""Tests for the ID type registry."""

import pytest

from omnipath_utils.mapping._id_types import IdTypeRegistry


@pytest.fixture
def registry():
    return IdTypeRegistry.get()


class TestIdTypeRegistry:
    def test_loads_types(self, registry):
        assert len(registry) > 50

    def test_resolve_canonical(self, registry):
        assert registry.resolve('uniprot') == 'uniprot'
        assert registry.resolve('genesymbol') == 'genesymbol'

    def test_resolve_alias(self, registry):
        assert registry.resolve('gene_symbol') == 'genesymbol'
        assert registry.resolve('uniprotkb') == 'uniprot'

    def test_resolve_dash_underscore(self, registry):
        # genesymbol-syn and genesymbol_syn should both resolve
        result = registry.resolve('genesymbol-syn')
        assert result is not None

    def test_resolve_underscore_variant(self, registry):
        result = registry.resolve('genesymbol_syn')
        assert result is not None

    def test_resolve_unknown(self, registry):
        assert registry.resolve('completely_fake_type') is None

    def test_entity_type(self, registry):
        assert registry.entity_type('uniprot') == 'protein'
        assert registry.entity_type('hmdb') == 'small_molecule'
        assert registry.entity_type('ensg') == 'gene'

    def test_curie_prefix(self, registry):
        assert registry.curie_prefix('uniprot') == 'uniprot'
        assert registry.curie_prefix('chebi') == 'chebi'
        assert registry.curie_prefix('genesymbol') == 'hgnc.symbol'

    def test_backend_column(self, registry):
        assert registry.backend_column('uniprot', 'uniprot') == 'accession'
        assert (
            registry.backend_column('genesymbol', 'uniprot') == 'gene_primary'
        )
        assert (
            registry.backend_column('genesymbol', 'ensembl')
            == 'external_gene_name'
        )

    def test_by_entity_type(self, registry):
        proteins = registry.by_entity_type('protein')
        assert 'uniprot' in proteins
        assert 'swissprot' in proteins
        assert len(proteins) >= 5

    def test_by_backend(self, registry):
        uniprot_types = registry.by_backend('uniprot')
        assert 'genesymbol' in uniprot_types
        assert uniprot_types['genesymbol'] == 'gene_primary'

    def test_all_names(self, registry):
        names = registry.all_names()
        assert 'uniprot' in names
        assert 'genesymbol' in names
        assert len(names) > 50

    def test_contains(self, registry):
        assert 'uniprot' in registry
        assert 'fake_type' not in registry


    def test_chembl_type(self, registry):
        assert registry.resolve('chembl') == 'chembl'
        assert registry.resolve('chembl_id') == 'chembl'
        assert registry.resolve('chembl_compound') == 'chembl'
        assert registry.entity_type('chembl') == 'small_molecule'
        assert registry.curie_prefix('chembl') == 'chembl.compound'
        assert registry.backend_column('chembl', 'unichem') == 'chembl'

    def test_singleton(self):
        r1 = IdTypeRegistry.get()
        r2 = IdTypeRegistry.get()
        assert r1 is r2
