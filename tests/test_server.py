"""Tests for the web service."""

import pytest
from unittest.mock import patch

litestar = pytest.importorskip("litestar", reason="litestar not installed")
from litestar.testing import TestClient

from omnipath_utils.server._app import create_app


@pytest.fixture
def app():
    """Create test app with mocked session factory.

    The translate endpoints need a real SQLAlchemy Session to pass
    Litestar type validation, but the actual DB queries are mocked.
    We use a lightweight in-memory SQLite session factory.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine('sqlite://')
    factory = sessionmaker(bind=engine)

    with patch(
        'omnipath_utils.server._app.create_session_factory'
    ) as mock_sf:
        mock_sf.return_value = factory
        yield create_app(db_url='sqlite://')


@pytest.fixture
def client(app):
    """Create test client."""
    return TestClient(app)


class TestHealthEndpoint:

    def test_health(self, client):
        resp = client.get('/health')
        assert resp.status_code == 200
        data = resp.json()
        assert data['status'] == 'ok'


class TestTaxonomyEndpoints:

    def test_resolve_human(self, client):
        resp = client.get('/taxonomy/resolve', params={'organism': 'human'})
        assert resp.status_code == 200
        data = resp.json()
        assert data['ncbi_tax_id'] == 9606
        assert data['common_name'] == 'human'
        assert data['kegg_code'] == 'hsa'

    def test_resolve_by_taxid(self, client):
        resp = client.get('/taxonomy/resolve', params={'organism': '10090'})
        assert resp.status_code == 200
        assert resp.json()['common_name'] == 'mouse'

    def test_resolve_unknown(self, client):
        resp = client.get('/taxonomy/resolve', params={'organism': 'alien'})
        assert resp.status_code == 200
        assert resp.json()['ncbi_tax_id'] is None

    def test_organisms_list(self, client):
        resp = client.get('/taxonomy/organisms')
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 20
        taxids = {o['ncbi_tax_id'] for o in data}
        assert 9606 in taxids


class TestMappingEndpoints:

    def test_id_types(self, client):
        resp = client.get('/mapping/id-types')
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) > 50
        names = {t['name'] for t in data}
        assert 'uniprot' in names

    def test_translate_get(self, client):
        """Test GET translate with mocked translate_ids."""
        with patch(
            'omnipath_utils.server._routes_mapping.translate_ids'
        ) as mock_tr:
            mock_tr.return_value = {'TP53': {'P04637'}}

            resp = client.get('/mapping/translate', params={
                'identifiers': 'TP53',
                'id_type': 'genesymbol',
                'target_id_type': 'uniprot',
            })
            assert resp.status_code == 200
            data = resp.json()
            assert 'results' in data
            assert 'meta' in data
            assert 'TP53' in data['results']

    def test_translate_post(self, client):
        """Test POST translate."""
        with patch(
            'omnipath_utils.server._routes_mapping.translate_ids'
        ) as mock_tr:
            mock_tr.return_value = {
                'TP53': {'P04637'},
                'EGFR': {'P00533'},
            }

            resp = client.post('/mapping/translate', json={
                'identifiers': ['TP53', 'EGFR'],
                'id_type': 'genesymbol',
                'target_id_type': 'uniprot',
            })
            assert resp.status_code in (200, 201)
            data = resp.json()
            assert data['meta']['total_mapped'] == 2


class TestReflistEndpoints:

    def test_list_names(self, client):
        resp = client.get('/reflists/list-names')
        assert resp.status_code == 200
        assert 'swissprot' in resp.json()
