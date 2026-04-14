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
                    session.add(
                        IdType(
                            name=name,
                            label=info.get('label'),
                            entity_type=info.get('entity_type'),
                            curie_prefix=info.get('curie_prefix'),
                        )
                    )
            session.commit()

        _log.info('Populated %d ID types', len(registry))

    def populate_backends(self):
        """Populate backend table."""
        backends = [
            'uniprot',
            'uploadlists',
            'biomart',
            'pro',
            'unichem',
            'ramp',
            'hmdb',
            'array',
            'mirbase',
            'file',
            'uniprot_ftp',
            'metanetx',
            'bigg',
        ]

        with Session(self.engine) as session:
            for name in backends:
                existing = session.query(Backend).filter_by(name=name).first()
                if not existing:
                    session.add(Backend(name=name))
            session.commit()

        _log.info('Populated %d backends', len(backends))

    def populate_organisms(self):
        """Populate organism table from all available sources."""
        tm = TaxonomyManager.get()
        tm.load_all()
        orgs = tm.all_organisms()

        # Fields that exist in the Organism table
        _db_fields = {
            'common_name', 'latin_name', 'short_latin', 'ensembl_name',
            'kegg_code', 'mirbase_code', 'oma_code', 'uniprot_code',
            'dbptm_code',
        }

        with Session(self.engine) as session:
            for taxid, info in orgs.items():
                existing = (
                    session.query(Organism).filter_by(ncbi_tax_id=taxid).first()
                )
                if not existing:
                    session.add(
                        Organism(
                            ncbi_tax_id=taxid,
                            **{
                                k: v for k, v in info.items()
                                if v and k in _db_fields
                            },
                        )
                    )
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
            id_type,
            target_id_type,
            ncbi_tax_id,
            backend_name,
        )

        start = time.time()

        # Get type and backend IDs
        with Session(self.engine) as session:
            src_type = session.query(IdType).filter_by(name=id_type).first()
            tgt_type = (
                session.query(IdType).filter_by(name=target_id_type).first()
            )
            backend = (
                session.query(Backend).filter_by(name=backend_name).first()
            )

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
                {
                    'src': src_type_id,
                    'tgt': tgt_type_id,
                    'tax': ncbi_tax_id,
                    'bk': backend_id,
                },
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
                        copy.write_row(
                            (
                                src_type_id,
                                tgt_type_id,
                                ncbi_tax_id,
                                source_id[:64],
                                target_id[:64],
                                backend_id,
                            )
                        )
                        row_count += 1

        conn.commit()
        conn.close()

        duration = time.time() - start

        # Record build info
        with Session(self.engine) as session:
            session.add(
                BuildInfo(
                    table_name='id_mapping',
                    source_type=id_type,
                    target_type=target_id_type,
                    ncbi_tax_id=ncbi_tax_id,
                    backend=backend_name,
                    row_count=row_count,
                    duration_secs=duration,
                    status='done',
                )
            )
            session.commit()

        _log.info(
            'Built %s -> %s: %d rows in %.1fs',
            id_type,
            target_id_type,
            row_count,
            duration,
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
            uniprot_type = (
                session.query(IdType).filter_by(name='uniprot').first()
            )
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

        for list_name, loader in [
            ('swissprot', all_swissprots),
            ('trembl', all_trembls),
        ]:
            ids = loader(ncbi_tax_id)
            _log.info(
                'Loading %d %s IDs for organism %d',
                len(ids),
                list_name,
                ncbi_tax_id,
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
            session.add(
                BuildInfo(
                    table_name='reflist',
                    source_type='uniprot',
                    target_type='reflist (swissprot + trembl)',
                    ncbi_tax_id=ncbi_tax_id,
                    backend='reflists',
                    row_count=total_rows,
                    duration_secs=duration,
                    status='done',
                )
            )
            session.commit()

        conn.close()
        _log.info(
            'Reflists for organism %d: %d rows in %.1fs',
            ncbi_tax_id,
            total_rows,
            duration,
        )

    def build_reference_tables(self):
        """Build all reference tables (id_types, backends, organisms)."""
        self.create_tables()
        self.populate_id_types()
        self.populate_backends()
        self.populate_organisms()

    def build_all(self, organisms: list[int] | None = None):
        """Full build: reference tables + mappings + reflists.

        Uses preset infrastructure internally. Equivalent to a custom
        preset with the given organisms, protein core + Ensembl mappings,
        and reference lists.

        Note: uniprot-sec -> uniprot-pri is skipped here because it has
        no backend column; that table requires a dedicated loader from
        UniProt sec_ac.txt or the FTP idmapping file.  The cleanup
        pipeline works without it (the secondary -> primary step is
        simply skipped).
        """
        from omnipath_utils.db._presets import ENSEMBL, PROTEIN_CORE

        self.build_reference_tables()

        organisms = organisms or [9606]  # default: human only

        for src, tgt, backend in PROTEIN_CORE + ENSEMBL:
            for org in organisms:
                try:
                    self.populate_mapping(src, tgt, org, backend)
                except Exception as e:
                    _log.error(
                        'Failed: %s -> %s (org %d): %s', src, tgt, org, e
                    )

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
            backend = (
                session.query(Backend).filter_by(name='uniprot_ftp').first()
            )
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
                id_type = (
                    session.query(IdType).filter_by(name=canonical_name).first()
                )
                if id_type:
                    type_name_to_id[ftp_name] = id_type.id

            # We also need the 'uniprot' type ID (source side is always uniprot AC)
            uniprot_type = (
                session.query(IdType).filter_by(name='uniprot').first()
            )
            if not uniprot_type:
                _log.error('ID type "uniprot" not found')
                return
            uniprot_type_id = uniprot_type.id

        _log.info(
            'Mapped %d FTP ID types to database IDs', len(type_name_to_id)
        )

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
            mode='w',
            suffix='.tsv',
            delete=False,
            prefix='taxids_',
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

                copy.write_row(
                    (
                        uniprot_type_id,
                        target_type_id,
                        0,  # ncbi_tax_id filled in later
                        uniprot_ac[:64],
                        id_value[:64],
                        backend_id,
                    )
                )
                mapping_count += 1

                if mapping_count % 10_000_000 == 0:
                    _log.info(
                        'Inserted %dM mapping rows, %d taxids written',
                        mapping_count // 1_000_000,
                        taxid_count,
                    )

        conn.commit()
        taxid_file.close()
        _log.info(
            'Inserted %d mapping rows, %d taxids to temp file',
            mapping_count,
            taxid_count,
        )

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
                        _log.info(
                            'Loaded %dM / %dM taxids',
                            loaded // 1_000_000,
                            taxid_count // 1_000_000,
                        )
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
            session.add(
                BuildInfo(
                    table_name='id_mapping',
                    source_type='uniprot (all)',
                    target_type='all FTP types',
                    ncbi_tax_id=0,
                    backend='uniprot_ftp',
                    row_count=mapping_count,
                    duration_secs=duration,
                    status='done',
                )
            )
            session.commit()

        conn.close()

    def export_parquet(self, tables: list[tuple], output_dir: str):
        """Export mapping tables as Parquet files for fast API delivery."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        os.makedirs(output_dir, exist_ok=True)

        from omnipath_utils.db._query import get_full_table

        for src_type, tgt_type, ncbi_tax_id in tables:
            with Session(self.engine) as session:
                data = get_full_table(
                    session, src_type, tgt_type, ncbi_tax_id
                )

            if not data:
                _log.warning(
                    'No data for %s -> %s (org %d), skipping Parquet',
                    src_type,
                    tgt_type,
                    ncbi_tax_id,
                )
                continue

            # Convert to flat rows for Parquet
            rows = [
                (k, v) for k, targets in data.items() for v in targets
            ]
            if not rows:
                continue

            table = pa.table(
                {
                    src_type: [r[0] for r in rows],
                    tgt_type: [r[1] for r in rows],
                }
            )

            fname = f'{src_type}__{tgt_type}__{ncbi_tax_id}.parquet'
            path = os.path.join(output_dir, fname)
            pq.write_table(table, path, compression='snappy')
            _log.info('Exported %s: %d rows', fname, len(rows))

    def populate_metabolites(self):
        """Populate metabolite ID mappings from UniChem and RaMP.

        Auto-discovers all available ID types from both sources and builds
        all available pairwise mappings.
        """
        self._populate_unichem()
        self._populate_ramp()
        self._populate_metanetx()
        self._populate_bigg()

    # ------------------------------------------------------------------
    # UniChem auto-discovery
    # ------------------------------------------------------------------

    # Mapping from UniChem normalised labels to our canonical id_type
    # names where they differ.
    _UNICHEM_NAME_MAP: dict[str, str] = {
        'lipid_maps': 'lipidmaps',
        'probes&drugs': 'probes_drugs',
    }

    def _unichem_canonical(self, label: str) -> str | None:
        """Normalise a UniChem source label to a canonical id_type name."""
        import re

        raw = label.lower().replace(' ', '_')
        raw = re.sub(r'[^a-z0-9_&]', '', raw).strip('_')

        if not raw:
            return None

        return self._UNICHEM_NAME_MAP.get(raw, raw)

    def _populate_unichem(self):
        """Auto-discover and build all UniChem pairwise mappings."""
        try:
            from pypath.inputs.unichem import (
                unichem_mapping,
                unichem_sources,
            )
        except ImportError:
            _log.warning('pypath not available for UniChem')
            return

        sources = unichem_sources()
        _log.info('UniChem: %d sources available', len(sources))

        # Register any newly discovered sources as ID types
        self._register_unichem_types(sources)

        # Map UniChem source numbers to our canonical id_type names
        num_to_name: dict[int, str] = {}

        for num, label in sources.items():
            canonical = self._unichem_canonical(label)

            if canonical:
                num_to_name[num] = canonical

        # Get backend ID
        with Session(self.engine) as session:
            backend = (
                session.query(Backend).filter_by(name='unichem').first()
            )

            if not backend:
                _log.error('Backend unichem not found')
                return

            backend_id = backend.id

        from omnipath_utils.db._connection import get_connection

        built = 0
        failed = 0
        source_nums = sorted(num_to_name.keys())

        for i, src_num in enumerate(source_nums):
            for tgt_num in source_nums[i + 1:]:
                src_name = num_to_name[src_num]
                tgt_name = num_to_name[tgt_num]

                try:
                    data = unichem_mapping(src_num, tgt_num)

                    if not data:
                        continue

                    with Session(self.engine) as session:
                        src_type = (
                            session.query(IdType)
                            .filter_by(name=src_name)
                            .first()
                        )
                        tgt_type = (
                            session.query(IdType)
                            .filter_by(name=tgt_name)
                            .first()
                        )

                        if not src_type or not tgt_type:
                            _log.debug(
                                'UniChem: id_type not found'
                                ' for %s or %s',
                                src_name,
                                tgt_name,
                            )
                            continue

                        src_type_id = src_type.id
                        tgt_type_id = tgt_type.id

                    conn = get_connection(self._db_url)
                    row_count = 0

                    with conn.cursor() as cur:
                        with cur.copy(
                            f'COPY {SCHEMA}.id_mapping'
                            ' (source_type_id, target_type_id,'
                            ' ncbi_tax_id, source_id, target_id,'
                            ' backend_id) FROM STDIN'
                        ) as copy:
                            for src_id, tgt_ids in data.items():
                                for tgt_id in tgt_ids:
                                    copy.write_row((
                                        src_type_id,
                                        tgt_type_id,
                                        0,
                                        str(src_id)[:64],
                                        str(tgt_id)[:64],
                                        backend_id,
                                    ))
                                    row_count += 1

                    conn.commit()
                    conn.close()

                    if row_count:
                        built += 1
                        _log.info(
                            'UniChem %s -> %s: %d rows',
                            src_name,
                            tgt_name,
                            row_count,
                        )

                except Exception as e:
                    failed += 1
                    _log.debug(
                        'UniChem %s -> %s: %s',
                        src_name,
                        tgt_name,
                        e,
                    )

        _log.info(
            'UniChem: built %d tables, %d failed/empty',
            built,
            failed,
        )

    def _register_unichem_types(self, sources: dict[int, str]):
        """Register UniChem source IDs as id_types if not present."""
        with Session(self.engine) as session:
            for _num, label in sources.items():
                canonical = self._unichem_canonical(label)

                if not canonical:
                    continue

                existing = (
                    session.query(IdType)
                    .filter_by(name=canonical)
                    .first()
                )

                if not existing:
                    session.add(
                        IdType(
                            name=canonical,
                            label=label,
                            entity_type='small_molecule',
                        )
                    )
                    _log.info(
                        'Registered new ID type from UniChem:'
                        ' %s (%s)',
                        canonical,
                        label,
                    )

            session.commit()

    # ------------------------------------------------------------------
    # RaMP auto-discovery
    # ------------------------------------------------------------------

    # Mapping from RaMP IDtype strings to our canonical id_type names
    # where they differ.
    _RAMP_NAME_MAP: dict[str, str] = {
        'CAS': 'cas',
        'LIPIDMAPS': 'lipidmaps',
        'rhea-comp': 'rhea',
    }

    def _ramp_canonical(self, ramp_type: str) -> str:
        """Normalise a RaMP IDtype string to a canonical id_type name."""
        return self._RAMP_NAME_MAP.get(ramp_type, ramp_type.lower())

    def _populate_ramp(self):
        """Auto-discover and build RaMP pairwise mappings."""
        try:
            from pypath.inputs.ramp import ramp_mapping
            from pypath.inputs.ramp._sqlite import (
                id_types as ramp_id_types_fn,
            )
        except ImportError:
            _log.warning('pypath not available for RaMP')
            return

        # Discover available compound ID types
        try:
            ramp_types = sorted(
                ramp_id_types_fn(entity_type='compound')
            )
        except Exception as e:
            _log.error('RaMP discovery failed: %s', e)
            return

        _log.info(
            'RaMP: %d compound ID types available', len(ramp_types)
        )

        # Register any new types
        self._register_ramp_types(ramp_types)

        # Get backend ID
        with Session(self.engine) as session:
            backend = (
                session.query(Backend).filter_by(name='ramp').first()
            )

            if not backend:
                _log.error('Backend ramp not found')
                return

            backend_id = backend.id

        from omnipath_utils.db._connection import get_connection

        # Build important pairs: each type to chebi and hmdb (hub types)
        hubs = ['chebi', 'hmdb']
        built = 0

        for src_ramp_type in ramp_types:
            src_canonical = self._ramp_canonical(src_ramp_type)

            for hub in hubs:
                if src_canonical == hub:
                    continue

                try:
                    data = ramp_mapping(src_ramp_type, hub)

                    if not data:
                        continue

                    with Session(self.engine) as session:
                        src_t = (
                            session.query(IdType)
                            .filter_by(name=src_canonical)
                            .first()
                        )
                        tgt_t = (
                            session.query(IdType)
                            .filter_by(name=hub)
                            .first()
                        )

                        if not src_t or not tgt_t:
                            _log.debug(
                                'RaMP: id_type not found'
                                ' for %s or %s',
                                src_canonical,
                                hub,
                            )
                            continue

                        src_type_id = src_t.id
                        tgt_type_id = tgt_t.id

                    conn = get_connection(self._db_url)
                    row_count = 0

                    with conn.cursor() as cur:
                        with cur.copy(
                            f'COPY {SCHEMA}.id_mapping'
                            ' (source_type_id, target_type_id,'
                            ' ncbi_tax_id, source_id, target_id,'
                            ' backend_id) FROM STDIN'
                        ) as copy:
                            for src_id, tgt_ids in data.items():
                                for tgt_id in tgt_ids:
                                    copy.write_row((
                                        src_type_id,
                                        tgt_type_id,
                                        0,
                                        str(src_id)[:64],
                                        str(tgt_id)[:64],
                                        backend_id,
                                    ))
                                    row_count += 1

                    conn.commit()
                    conn.close()

                    if row_count:
                        built += 1
                        _log.info(
                            'RaMP %s -> %s: %d rows',
                            src_canonical,
                            hub,
                            row_count,
                        )

                except Exception as e:
                    _log.debug(
                        'RaMP %s -> %s: %s',
                        src_canonical,
                        hub,
                        e,
                    )

        _log.info('RaMP: built %d tables', built)

    def _register_ramp_types(self, ramp_types: list[str]):
        """Register RaMP ID types if not already present."""
        with Session(self.engine) as session:
            for rtype in ramp_types:
                canonical = self._ramp_canonical(rtype)

                existing = (
                    session.query(IdType)
                    .filter_by(name=canonical)
                    .first()
                )

                if not existing:
                    session.add(
                        IdType(
                            name=canonical,
                            label=rtype,
                            entity_type='small_molecule',
                        )
                    )
                    _log.info(
                        'Registered new ID type from RaMP: %s',
                        canonical,
                    )

            session.commit()

    def populate_mirna(self, organisms: list[int]):
        """Populate miRNA ID mappings."""
        for org in organisms:
            for src, tgt in [
                ('mir-pre', 'mirbase'),
                ('mir-mat-name', 'mirbase'),
            ]:
                try:
                    self.populate_mapping(src, tgt, org, 'mirbase')
                except Exception as e:
                    _log.error(
                        'miRBase %s -> %s (org %d) failed: %s',
                        src,
                        tgt,
                        org,
                        e,
                    )

    def populate_orthology(self, organisms: list[int]):
        """Populate orthology tables for organism pairs."""
        source = 9606  # human as source

        from omnipath_utils.db._connection import get_connection
        from omnipath_utils.orthology._manager import OrthologyManager

        for target in organisms:
            if target == source:
                continue

            _log.info('Building orthology: %d -> %d', source, target)

            mgr = OrthologyManager()
            try:
                table = mgr._get_table(
                    source,
                    target,
                    'genesymbol',
                    resource='hcop',
                    min_sources=1,
                )
                if not table:
                    _log.warning(
                        'No orthology data for %d -> %d', source, target
                    )
                    continue

                conn = get_connection(self._db_url)
                cur = conn.cursor()

                # Delete existing
                cur.execute(
                    f'DELETE FROM {SCHEMA}.orthology'
                    ' WHERE source_tax_id = %s AND target_tax_id = %s',
                    (source, target),
                )
                conn.commit()

                row_count = 0
                with cur.copy(
                    f'COPY {SCHEMA}.orthology'
                    ' (source_id, target_id, source_tax_id, target_tax_id,'
                    ' id_type, resource, n_sources, support)'
                    ' FROM STDIN'
                ) as copy:
                    for src_id, target_ids in table.data.items():
                        for tgt_id in target_ids:
                            meta = table.metadata.get(src_id, {}).get(
                                tgt_id, {}
                            )
                            copy.write_row(
                                (
                                    src_id[:64],
                                    tgt_id[:64],
                                    source,
                                    target,
                                    'genesymbol',
                                    'hcop',
                                    meta.get('n_sources'),
                                    meta.get('support', '')[:500],
                                )
                            )
                            row_count += 1

                conn.commit()
                conn.close()
                _log.info(
                    'Orthology %d -> %d: %d pairs',
                    source,
                    target,
                    row_count,
                )

            except Exception as e:
                _log.error(
                    'Orthology %d -> %d failed: %s', source, target, e
                )

    def build_preset(self, preset: str, parquet_dir: str | None = None):
        """Build using a named preset."""
        from omnipath_utils.db._presets import PRESETS, PARQUET_TABLES

        if preset not in PRESETS:
            raise ValueError(
                f'Unknown preset: {preset}. '
                f'Available: {list(PRESETS.keys())}'
            )

        config = PRESETS[preset]
        _log.info(
            'Building preset "%s": %s', preset, config['description']
        )

        self.build_reference_tables()

        organisms = config['organisms'] or [9606]

        if config.get('ftp'):
            self.populate_from_ftp()
        else:
            for src, tgt, backend in config['mappings']:
                for org in organisms:
                    try:
                        self.populate_mapping(src, tgt, org, backend)
                    except Exception as e:
                        _log.error(
                            'Failed: %s -> %s (org %d): %s',
                            src,
                            tgt,
                            org,
                            e,
                        )

        if config.get('metabolite'):
            self.populate_metabolites()

        if config.get('mirna'):
            self.populate_mirna(organisms)

        if config.get('orthology'):
            self.populate_orthology(organisms)

        if config.get('reflists'):
            for org in organisms:
                try:
                    self.populate_reflists(org)
                except Exception as e:
                    _log.error('Failed reflists org %d: %s', org, e)

        # Export Parquet files
        if parquet_dir:
            tables = PARQUET_TABLES.get(
                preset, PARQUET_TABLES.get('minimal', [])
            )
            self.export_parquet(tables, parquet_dir)

        _log.info('Preset "%s" build complete', preset)

    # ------------------------------------------------------------------
    # MetaNetX cross-references
    # ------------------------------------------------------------------

    _BIGG_PAIRS = [
        ("bigg", "chebi"),
        ("bigg", "hmdb"),
        ("bigg", "kegg"),
        ("bigg", "metanetx"),
    ]

    _METANETX_PAIRS = [
        ("bigg", "chebi"),
        ("bigg", "hmdb"),
        ("bigg", "kegg"),
        ("kegg", "chebi"),
        ("hmdb", "chebi"),
        ("lipidmaps", "chebi"),
        ("swisslipids", "chebi"),
        ("metanetx", "chebi"),
        ("metanetx", "hmdb"),
        ("metanetx", "kegg"),
        ("metanetx", "bigg"),
    ]

    def _populate_metanetx(self):
        """Build MetaNetX pairwise metabolite mappings."""

        _log.info("Building MetaNetX cross-reference mappings...")

    _BIGG_PAIRS = [
        ("bigg", "chebi"),
        ("bigg", "hmdb"),
        ("bigg", "kegg"),
        ("bigg", "metanetx"),
    ]

        for src, tgt in self._METANETX_PAIRS:
            try:
                self.populate_mapping(src, tgt, 0, "metanetx")
            except Exception as e:
                _log.warning(
                    "MetaNetX %s -> %s failed: %s", src, tgt, e,
                )

    def _populate_bigg(self):
        """Build BiGG metabolite ID mappings from BiGG Models TSV."""

        _log.info("Building BiGG metabolite cross-reference mappings...")

        for src, tgt in self._BIGG_PAIRS:
            try:
                self.populate_mapping(src, tgt, 0, "bigg")
            except Exception as e:
                _log.warning(
                    "BiGG %s -> %s failed: %s", src, tgt, e,
                )
