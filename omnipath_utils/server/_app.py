"""Litestar web service for omnipath-utils."""

from __future__ import annotations

import logging
from typing import Generator

from litestar import Litestar, get
from litestar.config.cors import CORSConfig
from litestar.openapi import OpenAPIConfig
from litestar.di import Provide

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from omnipath_utils.db._connection import get_db_url

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

    app = Litestar(
        route_handlers=[
            MappingController,
            TaxonomyController,
            ReflistController,
            health_check,
        ],
        openapi_config=OpenAPIConfig(
            title='omnipath-utils',
            version='0.0.1',
            description=(
                'ID translation, taxonomy, and reference lists'
                ' for molecular biology'
            ),
        ),
        cors_config=CORSConfig(allow_origins=['*']),
        dependencies={'session': Provide(get_session)},
    )

    return app


@get('/health')
async def health_check() -> dict:
    """Health check endpoint."""
    return {'status': 'ok', 'service': 'omnipath-utils'}
