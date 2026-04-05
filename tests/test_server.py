"""Tests for the web service."""

import pytest
from unittest.mock import patch

litestar = pytest.importorskip("litestar", reason="litestar not installed")
from litestar.testing import TestClient

from omnipath_utils.server._app import create_app

from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


@pytest.fixture
def app():
    """Create test app with an in-memory SQLite session."""

    engine = create_engine(
        "sqlite://",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # Create tables that the health endpoint queries
    with engine.connect() as conn:
        for ddl in [
            "CREATE TABLE IF NOT EXISTS id_type (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE IF NOT EXISTS backend (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE IF NOT EXISTS organism (ncbi_tax_id INTEGER PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS id_mapping (id INTEGER PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS reflist (id INTEGER PRIMARY KEY)",
            "CREATE TABLE IF NOT EXISTS build_info (id INTEGER PRIMARY KEY, source_type TEXT, target_type TEXT, ncbi_tax_id INTEGER, backend TEXT, row_count INTEGER, built_at TEXT, duration_secs REAL, status TEXT)",
        ]:
            conn.execute(sa_text(ddl))
        conn.commit()

    TestSessionLocal = sessionmaker(bind=engine)

    with patch("omnipath_utils.server._app.create_session_factory") as mock_sf:
        mock_sf.return_value = TestSessionLocal
        _app = create_app(db_url="sqlite://")
        yield _app


@pytest.fixture
def client(app):
    """Create test client."""
    return TestClient(app)


class TestHealthEndpoint:

    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "database" in data
        assert "stats" in data
        assert "backends" in data  # empty list when schema unavailable
        assert "builds" in data


class TestTaxonomyEndpoints:

    def test_resolve_human(self, client):
        resp = client.get("/taxonomy/resolve", params={"organism": "human"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ncbi_tax_id"] == 9606
        assert data["common_name"] == "human"
        assert data["kegg_code"] == "hsa"

    def test_resolve_by_taxid(self, client):
        resp = client.get("/taxonomy/resolve", params={"organism": "10090"})
        assert resp.status_code == 200
        assert resp.json()["common_name"] == "mouse"

    def test_resolve_unknown(self, client):
        resp = client.get("/taxonomy/resolve", params={"organism": "alien"})
        assert resp.status_code == 200
        assert resp.json()["ncbi_tax_id"] is None

    def test_organisms_list(self, client):
        resp = client.get("/taxonomy/organisms")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 20
        taxids = {o["ncbi_tax_id"] for o in data}
        assert 9606 in taxids


class TestMappingEndpoints:

    def test_id_types(self, client):
        resp = client.get("/mapping/id-types")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) > 50
        names = {t["name"] for t in data}
        assert "uniprot" in names

    @patch("omnipath_utils.server._routes_mapping.translate_ids")
    def test_translate_get(self, mock_translate, client):
        mock_translate.return_value = {"TP53": {"P04637"}}
        resp = client.get("/mapping/translate", params={
            "identifiers": "TP53",
            "id_type": "genesymbol",
            "target_id_type": "uniprot",
        })
        assert resp.status_code == 200
        assert "results" in resp.json()

    @patch("omnipath_utils.server._routes_mapping.translate_ids")
    def test_translate_post(self, mock_translate, client):
        mock_translate.return_value = {"TP53": {"P04637"}}
        resp = client.post("/mapping/translate", json={
            "identifiers": ["TP53"],
            "id_type": "genesymbol",
            "target_id_type": "uniprot",
        })
        assert resp.status_code in (200, 201)


class TestReflistEndpoints:

    def test_list_names(self, client):
        resp = client.get("/reflists/list-names")
        assert resp.status_code == 200
        assert "swissprot" in resp.json()
