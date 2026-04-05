"""Database build orchestrator."""

from __future__ import annotations

import time
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from omnipath_utils.db._connection import get_engine, ensure_schema, SCHEMA
from omnipath_utils.db._schema import Base, IdType, Backend, Organism, BuildInfo
from omnipath_utils.mapping._id_types import IdTypeRegistry
from omnipath_utils.taxonomy._taxonomy import TaxonomyManager

_log = logging.getLogger(__name__)


class DatabaseBuilder:
    """Orchestrates building the omnipath_utils database."""

    def __init__(self, db_url: str | None = None):
        self.engine = get_engine(db_url)
        ensure_schema(self.engine)

    def create_tables(self):
        """Create all tables."""
        Base.metadata.create_all(self.engine)
        _log.info('All tables created')

    def populate_id_types(self):
        """Populate id_type table from id_types.yaml."""
        registry = IdTypeRegistry.get()

        with Session(self.engine) as session:
            for name in registry.all_names():
                info = registry.info(name)
                existing = session.query(IdType).filter_by(name=name).first()
                if not existing:
                    session.add(IdType(
                        name=name,
                        label=info.get('label'),
                        entity_type=info.get('entity_type'),
                        curie_prefix=info.get('curie_prefix'),
                    ))
            session.commit()

        _log.info('Populated %d ID types', len(registry))

    def populate_backends(self):
        """Populate backend table."""
        backends = ['uniprot', 'uploadlists', 'biomart', 'pro', 'unichem',
                    'ramp', 'hmdb', 'array', 'mirbase', 'file', 'uniprot_ftp']

        with Session(self.engine) as session:
            for name in backends:
                existing = session.query(Backend).filter_by(name=name).first()
                if not existing:
                    session.add(Backend(name=name))
            session.commit()

        _log.info('Populated %d backends', len(backends))

    def populate_organisms(self):
        """Populate organism table from organisms.yaml."""
        tm = TaxonomyManager.get()
        orgs = tm.all_organisms()

        with Session(self.engine) as session:
            for taxid, info in orgs.items():
                existing = session.query(Organism).filter_by(ncbi_tax_id=taxid).first()
                if not existing:
                    session.add(Organism(
                        ncbi_tax_id=taxid,
                        **{k: v for k, v in info.items() if v}
                    ))
            session.commit()

        _log.info('Populated %d organisms', len(orgs))

    def populate_mapping(
        self,
        id_type: str,
        target_id_type: str,
        ncbi_tax_id: int,
        backend_name: str,
    ):
        """Populate id_mapping table for one (source, target, organism, backend) combination.

        Uses the mapping infrastructure to load data, then bulk inserts via COPY.
        """
        from omnipath_utils.db._schema import IdMapping
        from omnipath_utils.mapping._reader import MapReader

        _log.info(
            'Building mapping: %s -> %s, organism %d, backend %s',
            id_type, target_id_type, ncbi_tax_id, backend_name,
        )

        start = time.time()

        # Get type and backend IDs
        with Session(self.engine) as session:
            src_type = session.query(IdType).filter_by(name=id_type).first()
            tgt_type = session.query(IdType).filter_by(name=target_id_type).first()
            backend = session.query(Backend).filter_by(name=backend_name).first()

            if not src_type or not tgt_type or not backend:
                _log.error('Missing ID type or backend in database')
                return

            src_type_id = src_type.id
            tgt_type_id = tgt_type.id
            backend_id = backend.id

        # Load mapping data via the existing reader
        reader = MapReader(
            id_type=id_type,
            target_id_type=target_id_type,
            ncbi_tax_id=ncbi_tax_id,
            backend=backend_name,
        )
        data = reader.load()

        if not data:
            _log.warning('No data loaded for %s -> %s', id_type, target_id_type)
            return

        # Delete existing rows for this combination
        with Session(self.engine) as session:
            session.execute(
                text(
                    f'DELETE FROM {SCHEMA}.id_mapping'
                    ' WHERE source_type_id = :src AND target_type_id = :tgt'
                    ' AND ncbi_tax_id = :tax AND backend_id = :bk'
                ),
                {'src': src_type_id, 'tgt': tgt_type_id, 'tax': ncbi_tax_id, 'bk': backend_id},
            )
            session.commit()

        # Bulk insert via COPY using psycopg3
        from omnipath_utils.db._connection import get_connection, get_db_url
        conn = get_connection()

        row_count = 0
        with conn.cursor() as cur:
            with cur.copy(
                f"COPY {SCHEMA}.id_mapping (source_type_id, target_type_id, ncbi_tax_id, source_id, target_id, backend_id) FROM STDIN"
            ) as copy:
                for source_id, target_ids in data.items():
                    for target_id in target_ids:
                        copy.write_row((
                            src_type_id, tgt_type_id, ncbi_tax_id,
                            source_id[:64], target_id[:64], backend_id,
                        ))
                        row_count += 1

        conn.commit()
        conn.close()

        duration = time.time() - start

        # Record build info
        with Session(self.engine) as session:
            session.add(BuildInfo(
                table_name='id_mapping',
                source_type=id_type,
                target_type=target_id_type,
                ncbi_tax_id=ncbi_tax_id,
                backend=backend_name,
                row_count=row_count,
                duration_secs=duration,
                status='done',
            ))
            session.commit()

        _log.info(
            'Built %s -> %s: %d rows in %.1fs',
            id_type, target_id_type, row_count, duration,
        )

    def build_reference_tables(self):
        """Build all reference tables (id_types, backends, organisms)."""
        self.create_tables()
        self.populate_id_types()
        self.populate_backends()
        self.populate_organisms()

    def build_all(self, organisms: list[int] | None = None):
        """Full build: reference tables + mappings for specified organisms."""
        self.build_reference_tables()

        organisms = organisms or [9606]  # default: human only

        # Build key mappings for each organism
        mapping_pairs = [
            ('genesymbol', 'uniprot', 'uniprot'),
            ('entrez', 'uniprot', 'uniprot'),
            ('ensg', 'genesymbol', 'biomart'),
            ('ensp', 'ensg', 'biomart'),
            ('enst', 'ensg', 'biomart'),
        ]

        for src, tgt, backend in mapping_pairs:
            for org in organisms:
                try:
                    self.populate_mapping(src, tgt, org, backend)
                except Exception as e:
                    _log.error('Failed: %s -> %s (org %d): %s', src, tgt, org, e)
