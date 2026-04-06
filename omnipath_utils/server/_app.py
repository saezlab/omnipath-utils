"""Litestar web service for omnipath-utils."""

from __future__ import annotations

import logging
from typing import Generator

from litestar import Litestar, get
from litestar.config.cors import CORSConfig
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import SwaggerRenderPlugin
from litestar.di import Provide
from litestar.response import Response

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from omnipath_utils.db._connection import get_db_url, SCHEMA

_log = logging.getLogger(__name__)


def create_session_factory(db_url: str | None = None) -> sessionmaker:
    """Create a SQLAlchemy session factory."""
    url = db_url or get_db_url()
    engine = create_engine(url)
    return sessionmaker(bind=engine)


# Global session factory (set during app creation)
_session_factory: sessionmaker | None = None


def get_session() -> Generator[Session, None, None]:
    """Dependency: provide a DB session."""
    session = _session_factory()
    try:
        yield session
    finally:
        session.close()


def create_app(db_url: str | None = None) -> Litestar:
    """Create the Litestar application."""
    global _session_factory
    _session_factory = create_session_factory(db_url)

    from omnipath_utils.server._routes_mapping import MappingController
    from omnipath_utils.server._routes_taxonomy import TaxonomyController
    from omnipath_utils.server._routes_reflists import ReflistController
    from omnipath_utils.server._routes_orthology import OrthologyController

    app = Litestar(
        route_handlers=[
            MappingController,
            TaxonomyController,
            ReflistController,
            OrthologyController,
            health_check,
            landing_page,
        ],
        openapi_config=OpenAPIConfig(
            title='omnipath-utils',
            version='0.0.1',
            description=(
                'ID translation, taxonomy, orthology and reference lists'
                ' for molecular biology'
            ),
            render_plugins=[SwaggerRenderPlugin()],
        ),
        cors_config=CORSConfig(allow_origins=['*']),
        dependencies={'session': Provide(get_session)},
    )

    return app


@get('/health')
async def health_check(session: Session) -> dict:
    """Health check with service info and database statistics."""

    stats = {}
    backends = []
    builds = []

    try:
        for table in ('id_type', 'backend', 'organism', 'id_mapping', 'reflist', 'build_info'):
            row = session.execute(text(f'SELECT count(*) FROM {SCHEMA}.{table}')).scalar()
            stats[table] = row
        db_status = 'connected'
    except Exception as e:
        db_status = f'error: {e}'

    try:
        rows = session.execute(text(f'SELECT name FROM {SCHEMA}.backend ORDER BY name')).fetchall()
        backends = [r[0] for r in rows]
    except Exception:
        pass

    try:
        rows = session.execute(
            text(
                f'SELECT source_type, target_type, ncbi_tax_id, backend, '
                f'row_count, built_at, duration_secs, status '
                f'FROM {SCHEMA}.build_info ORDER BY built_at DESC'
            )
        ).fetchall()
        for r in rows:
            builds.append({
                'source_type': r[0],
                'target_type': r[1],
                'ncbi_tax_id': r[2],
                'backend': r[3],
                'row_count': r[4],
                'built_at': r[5].isoformat() if r[5] else None,
                'duration_secs': float(r[6]) if r[6] is not None else None,
                'status': r[7],
            })
    except Exception:
        pass

    return {
        'status': 'ok',
        'service': 'omnipath-utils',
        'version': '0.0.1',
        'database': db_status,
        'stats': stats,
        'backends': backends,
        'builds': builds,
    }


@get('/')
async def landing_page() -> Response:
    """HTML landing page."""
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OmniPath Utils</title>
    <link href="https://fonts.googleapis.com/css?family=Raleway:400,300,600" rel="stylesheet">
    <link rel="stylesheet" href="https://omnipathdb.org/css/normalize.css">
    <link rel="stylesheet" href="https://omnipathdb.org/css/barebones.css">
    <link rel="stylesheet" href="https://omnipathdb.org/css/omnipath.css">
    <link rel="icon" type="image/vnd.microsoft.icon" href="https://omnipathdb.org/favicon.ico">
    <style>
        h5 { font-weight: 600; }
        .box code, .box a.code { font-size: 85%; }
        pre code { font-size: 85%; }
    </style>
</head>
<body>
    <!-- Header: logo + title -->
    <div class="grid-container u-align-left thirds">
        <div>
            <img src="https://omnipathdb.org/img/omnipath_logo.png"
                 title="OmniPath" class="full-width" />
        </div>
        <div class="span2 u-align-right">
            <h2>Utilities for molecular prior-knowledge processing</h2>
        </div>
    </div>

    <!-- Navigation -->
    <div class="grid-container u-align-left full">
        <nav>
            <a class="topmenu" href="#try"><span class="nav">try it</span></a>
            <a class="topmenu" href="#python"><span class="nav">Python</span></a>
            <a class="topmenu" href="/schema/swagger"><span class="nav">API docs</span></a>
            <a class="topmenu" href="/health"><span class="nav">status</span></a>
            <a class="topmenu" href="https://saezlab.github.io/omnipath-utils"><span class="nav">documentation</span></a>
            <a class="topmenu" href="https://github.com/saezlab/omnipath-utils"><span class="nav">GitHub</span></a>
            <a class="topmenu" href="https://omnipathdb.org"><span class="nav">OmniPath</span></a>
        </nav>
    </div>

    <!-- About -->
    <div class="grid-container u-align-left full">
        <div>
            <p>
                OmniPath Utils provides ID translation, taxonomy resolution, and
                reference lists for molecular biology. It translates between 97
                identifier types across UniProt, Ensembl, HGNC, Entrez, ChEBI, HMDB
                and more. Available as a Python library and as this HTTP API with
                <a href="/schema/swagger">interactive documentation</a>.
            </p>
            <ul>
                <li><strong>ID translation</strong> &mdash; translate between gene symbols, UniProt, Ensembl, Entrez, and 90+ other identifier types</li>
                <li><strong>Taxonomy</strong> &mdash; resolve organism names across NCBI, Ensembl, KEGG, OMA and miRBase naming systems</li>
                <li><strong>Reference lists</strong> &mdash; complete sets of identifiers, e.g. all human SwissProt IDs</li>
            </ul>
        </div>
    </div>

    <!-- Example queries -->
    <div class="grid-container u-align-left full" id="try">
        <div>
            <h5>Example queries</h5>
            <p>Translate gene symbols to UniProt:</p>
            <div class="box codebox">
                <a href="/mapping/translate?identifiers=TP53,EGFR,BRCA1&amp;id_type=genesymbol&amp;target_id_type=uniprot" class="no-uline code">/mapping/translate?identifiers=TP53,EGFR,BRCA1&amp;id_type=genesymbol&amp;target_id_type=uniprot</a>
            </div>
            <p>Translate Entrez Gene IDs to UniProt:</p>
            <div class="box codebox">
                <a href="/mapping/translate?identifiers=7157,1956&amp;id_type=entrez&amp;target_id_type=uniprot" class="no-uline code">/mapping/translate?identifiers=7157,1956&amp;id_type=entrez&amp;target_id_type=uniprot</a>
            </div>
            <p>Resolve an organism:</p>
            <div class="box codebox">
                <a href="/taxonomy/resolve?organism=human" class="no-uline code">/taxonomy/resolve?organism=human</a>
            </div>
            <p>More endpoints:</p>
            <ul>
                <li><a href="/mapping/id-types">/mapping/id-types</a> &mdash; all 97 supported identifier types</li>
                <li><a href="/taxonomy/organisms">/taxonomy/organisms</a> &mdash; all 22 organisms with name forms</li>
                <li><a href="/reflists/list-names">/reflists/list-names</a> &mdash; available reference lists</li>
                <li><a href="/schema/swagger">/schema/swagger</a> &mdash; interactive API documentation</li>
                <li><a href="/schema/openapi.json">/schema/openapi.json</a> &mdash; OpenAPI schema</li>
                <li><a href="/health">/health</a> &mdash; service status and database statistics</li>
            </ul>
        </div>
    </div>

    <!-- Python usage -->
    <div class="grid-container u-align-left full" id="python">
        <div>
            <h5>Python</h5>
            <div class="box codebox">
                <pre><code>from omnipath_utils.mapping import map_name
from omnipath_utils.taxonomy import ensure_ncbi_tax_id
from omnipath_utils.reflists import is_swissprot

map_name('TP53', 'genesymbol', 'uniprot')    # {'P04637', ...}
ensure_ncbi_tax_id('human')                   # 9606
is_swissprot('P04637')                        # True</code></pre>
            </div>
            <p>
                Install: <code>pip install omnipath-utils</code>
                &middot; <a href="https://saezlab.github.io/omnipath-utils">Read the docs</a>
            </p>
        </div>
    </div>

    <!-- Footer -->
    <div class="grid-container u-align-left full">
        <p class="small">
            Part of <a href="https://omnipathdb.org">OmniPath</a> &middot;
            Developed in <a href="https://saezlab.org">Saez Lab</a> &middot;
            <a href="https://github.com/saezlab/omnipath-utils">Source code</a>
        </p>
    </div>
</body>
</html>"""
    return Response(content=html, media_type='text/html')
