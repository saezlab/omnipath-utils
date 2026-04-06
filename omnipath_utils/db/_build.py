"""Database build orchestrator."""

from __future__ import annotations

import os
import time
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from omnipath_utils.db._schema import Base, IdType, Backend, Organism, BuildInfo
from omnipath_utils.db._connection import SCHEMA, get_engine, ensure_schema
from omnipath_utils.mapping._id_types import IdTypeRegistry
from omnipath_utils.taxonomy._taxonomy import TaxonomyManager

_log = logging.getLogger(__name__)


class DatabaseBuilder:
    """Orchestrates building the omnipath_utils database."""

    def __init__(self, db_url: str | None = None):
        self._db_url = db_url
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
        from omnipath_utils.db._connection import get_connection
        conn = get_connection(self._db_url)

        row_count = 0
        with conn.cursor() as cur:
            with cur.copy(
                f'COPY {SCHEMA}.id_mapping (source_type_id, target_type_id, ncbi_tax_id, source_id, target_id, backend_id) FROM STDIN'
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

    def populate_reflists(self, ncbi_tax_id: int):
        """Populate reference lists for an organism.

        Loads SwissProt and TrEMBL ID sets via the reflists module and
        bulk-inserts them into the reflist table using COPY.
        """
        from omnipath_utils.reflists import all_trembls, all_swissprots
        from omnipath_utils.db._connection import get_connection

        start = time.time()

        with Session(self.engine) as session:
            uniprot_type = session.query(IdType).filter_by(name='uniprot').first()
            if not uniprot_type:
                _log.error('ID type "uniprot" not found, skipping reflists')
                return
            type_id = uniprot_type.id

        conn = get_connection(self._db_url)
        cur = conn.cursor()

        # Clear existing for this organism
        cur.execute(
            f'DELETE FROM {SCHEMA}.reflist WHERE ncbi_tax_id = %s',
            (ncbi_tax_id,),
        )
        conn.commit()

        total_rows = 0

        for list_name, loader in [('swissprot', all_swissprots), ('trembl', all_trembls)]:
            ids = loader(ncbi_tax_id)
            _log.info(
                'Loading %d %s IDs for organism %d',
                len(ids), list_name, ncbi_tax_id,
            )

            with cur.copy(
                f'COPY {SCHEMA}.reflist'
                ' (identifier, id_type_id, ncbi_tax_id, list_name)'
                ' FROM STDIN'
            ) as copy:
                for ac in ids:
                    copy.write_row((ac, type_id, ncbi_tax_id, list_name))

            conn.commit()
            total_rows += len(ids)

        duration = time.time() - start

        # Record build info
        with Session(self.engine) as session:
            session.add(BuildInfo(
                table_name='reflist',
                source_type='uniprot',
                target_type=None,
                ncbi_tax_id=ncbi_tax_id,
                backend=None,
                row_count=total_rows,
                duration_secs=duration,
                status='done',
            ))
            session.commit()

        conn.close()
        _log.info(
            'Reflists for organism %d: %d rows in %.1fs',
            ncbi_tax_id, total_rows, duration,
        )

    def build_reference_tables(self):
        """Build all reference tables (id_types, backends, organisms)."""
        self.create_tables()
        self.populate_id_types()
        self.populate_backends()
        self.populate_organisms()

    def build_all(self, organisms: list[int] | None = None):
        """Full build: reference tables + mappings + reflists.

        Mapping pairs cover all tables needed for the special-case
        cleanup pipeline:

        - Core protein mappings (genesymbol, entrez, hgnc, refseqp)
        - SwissProt/TrEMBL specific tables (for cleanup pipeline)
        - Gene symbol synonyms
        - Ensembl mappings (ensg, ensp, enst)

        Note: uniprot-sec -> uniprot-pri is skipped here because it has
        no backend column; that table requires a dedicated loader from
        UniProt sec_ac.txt or the FTP idmapping file.  The cleanup
        pipeline works without it (the secondary -> primary step is
        simply skipped).
        """
        self.build_reference_tables()

        organisms = organisms or [9606]  # default: human only

        # Build key mappings for each organism
        mapping_pairs = [
            # Core protein mappings
            ('genesymbol', 'uniprot', 'uniprot'),
            ('entrez', 'uniprot', 'uniprot'),
            ('hgnc', 'uniprot', 'uniprot'),
            ('refseqp', 'uniprot', 'uniprot'),
            # SwissProt/TrEMBL specific (for cleanup pipeline)
            ('genesymbol', 'swissprot', 'uniprot'),
            ('trembl', 'genesymbol', 'uniprot'),
            # Gene symbol synonyms
            ('genesymbol-syn', 'uniprot', 'uniprot'),
            # Ensembl mappings
            ('ensg', 'genesymbol', 'biomart'),
            ('ensp', 'ensg', 'biomart'),
            ('enst', 'ensg', 'biomart'),
            ('ensp', 'uniprot', 'biomart'),
            # TODO: uniprot-sec -> uniprot-pri (needs dedicated loader)
        ]

        for src, tgt, backend in mapping_pairs:
            for org in organisms:
                try:
                    self.populate_mapping(src, tgt, org, backend)
                except Exception as e:
                    _log.error('Failed: %s -> %s (org %d): %s', src, tgt, org, e)

        # Reference lists
        for org in organisms:
            try:
                self.populate_reflists(org)
            except Exception as e:
                _log.error('Failed reflists for org %d: %s', org, e)

    def populate_from_ftp(self, id_types: set[str] | None = None):
        """Populate id_mapping table from the full UniProt FTP idmapping file.

        This processes the complete ~18GB idmapping.dat.gz in a single pass,
        streaming directly into PostgreSQL via COPY.

        Strategy:
        1. Stream all records, insert mappings with ncbi_tax_id=0
        2. Collect NCBI_TaxID rows in a temp table
        3. UPDATE id_mapping.ncbi_tax_id from the temp table
        """
        from omnipath_utils.db._connection import SCHEMA, get_connection

        _log.info('Starting full FTP idmapping build')
        start = time.time()

        # Get or create the backend ID for 'uniprot_ftp'
        with Session(self.engine) as session:
            backend = session.query(Backend).filter_by(name='uniprot_ftp').first()
            if not backend:
                _log.error('Backend uniprot_ftp not found in database')
                return
            backend_id = backend.id

        # We need type IDs for each FTP ID type name
        # Build a mapping from FTP type names to our id_type table IDs
        from pypath.inputs.uniprot_ftp import IDTYPE_MAP, idmapping_full_stream

        with Session(self.engine) as session:
            type_name_to_id = {}
            for ftp_name, canonical_name in IDTYPE_MAP.items():
                if canonical_name.startswith('_'):  # skip _taxid etc
                    continue
                id_type = session.query(IdType).filter_by(name=canonical_name).first()
                if id_type:
                    type_name_to_id[ftp_name] = id_type.id

            # We also need the 'uniprot' type ID (source side is always uniprot AC)
            uniprot_type = session.query(IdType).filter_by(name='uniprot').first()
            if not uniprot_type:
                _log.error('ID type "uniprot" not found')
                return
            uniprot_type_id = uniprot_type.id

        _log.info('Mapped %d FTP ID types to database IDs', len(type_name_to_id))

        # Connect raw psycopg for COPY
        conn = get_connection(self._db_url)
        cur = conn.cursor()

        # Create temp table for taxonomy
        cur.execute(
            'CREATE TEMP TABLE IF NOT EXISTS ac_taxid '
            '(ac VARCHAR(16) PRIMARY KEY, taxid INTEGER)'
        )
        cur.execute('TRUNCATE ac_taxid')
        conn.commit()

        # Delete existing FTP-loaded data
        cur.execute(
            f'DELETE FROM {SCHEMA}.id_mapping WHERE backend_id = %s',
            (backend_id,),
        )
        conn.commit()
        _log.info('Cleared existing FTP data')

        # Stream and insert
        mapping_count = 0
        taxid_count = 0

        # Taxid assignments are written to a temp file during streaming
        # (to avoid holding ~250M entries in memory), then loaded into
        # the temp table in batches after the main COPY finishes.
        import tempfile
        taxid_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.tsv', delete=False, prefix='taxids_',
        )

        # Start COPY for id_mapping
        ftp_types = set(type_name_to_id.keys())
        ftp_types.add('NCBI_TaxID')  # always collect taxids

        with cur.copy(
            f'COPY {SCHEMA}.id_mapping '
            '(source_type_id, target_type_id, ncbi_tax_id, '
            'source_id, target_id, backend_id) FROM STDIN'
        ) as copy:
            for uniprot_ac, id_type_name, id_value in idmapping_full_stream(
                id_types=ftp_types
            ):
                if id_type_name == 'NCBI_TaxID':
                    try:
                        taxid_file.write(f'{uniprot_ac}\t{int(id_value)}\n')
                        taxid_count += 1
                    except ValueError:
                        pass
                    continue

                target_type_id = type_name_to_id.get(id_type_name)
                if not target_type_id:
                    continue

                copy.write_row((
                    uniprot_type_id,
                    target_type_id,
                    0,  # ncbi_tax_id filled in later
                    uniprot_ac[:64],
                    id_value[:64],
                    backend_id,
                ))
                mapping_count += 1

                if mapping_count % 10_000_000 == 0:
                    _log.info(
                        'Inserted %dM mapping rows, %d taxids written',
                        mapping_count // 1_000_000, taxid_count,
                    )

        conn.commit()
        taxid_file.close()
        _log.info('Inserted %d mapping rows, %d taxids to temp file', mapping_count, taxid_count)

        # Load taxids from temp file into temp table in batches
        _log.info('Loading taxids from %s', taxid_file.name)
        batch = []
        BATCH_SIZE = 1_000_000
        loaded = 0

        with open(taxid_file.name) as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) == 2:
                    batch.append((parts[0], int(parts[1])))
                    if len(batch) >= BATCH_SIZE:
                        with cur.copy(
                            'COPY ac_taxid (ac, taxid) FROM STDIN'
                        ) as taxid_copy:
                            for row in batch:
                                taxid_copy.write_row(row)
                        conn.commit()
                        loaded += len(batch)
                        _log.info('Loaded %dM / %dM taxids', loaded // 1_000_000, taxid_count // 1_000_000)
                        batch = []

        # Flush remaining
        if batch:
            with cur.copy('COPY ac_taxid (ac, taxid) FROM STDIN') as taxid_copy:
                for row in batch:
                    taxid_copy.write_row(row)
            conn.commit()
            loaded += len(batch)

        _log.info('Loaded all %d taxids into temp table', loaded)
        os.unlink(taxid_file.name)

        # Update ncbi_tax_id in id_mapping from the temp table
        _log.info('Updating ncbi_tax_id for %d mapping rows...', mapping_count)
        cur.execute(
            f'UPDATE {SCHEMA}.id_mapping m '
            'SET ncbi_tax_id = t.taxid '
            'FROM ac_taxid t '
            'WHERE m.source_id = t.ac '
            'AND m.backend_id = %s '
            'AND m.ncbi_tax_id = 0',
            (backend_id,),
        )
        updated = cur.rowcount
        conn.commit()
        _log.info('Updated %d rows with organism info', updated)

        # Clean up temp table
        cur.execute('DROP TABLE IF EXISTS ac_taxid')
        conn.commit()

        # ANALYZE for query planner
        cur.execute(f'ANALYZE {SCHEMA}.id_mapping')
        conn.commit()

        duration = time.time() - start
        _log.info(
            'Full FTP build complete: %d mappings, %d taxids, %.1f minutes',
            mapping_count,
            taxid_count,
            duration / 60,
        )

        # Record in build_info
        with Session(self.engine) as session:
            session.add(BuildInfo(
                table_name='id_mapping',
                source_type='uniprot (all)',
                target_type='all FTP types',
                ncbi_tax_id=0,
                backend='uniprot_ftp',
                row_count=mapping_count,
                duration_secs=duration,
                status='done',
            ))
            session.commit()

        conn.close()
