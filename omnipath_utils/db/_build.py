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

    def __init__(
        self,
        db_url: str | None = None,
        max_records: int | None = None,
        pubchem_max_records: int | None = None,
    ):
        self._db_url = db_url
        self._max_records = max_records
        self._pubchem_max_records = pubchem_max_records
        self.engine = get_engine(db_url)
        ensure_schema(self.engine)
        if max_records is not None:
            _log.warning(
                'max_records=%d: mapping tables are CAPPED for testing; '
                'this is NOT a complete build',
                max_records,
            )
        if pubchem_max_records is not None:
            _log.warning(
                'pubchem_max_records=%d: only the PubChem table is capped; '
                'all other resources load in full',
                pubchem_max_records,
            )

    def _effective_limit(self, backend_name: str) -> int | None:
        """Row cap for a backend: PubChem honours its own cap when set,
        every other backend uses the global ``max_records`` (if any)."""
        if backend_name == 'pubchem' and self._pubchem_max_records is not None:
            return self._pubchem_max_records
        return self._max_records

    def _run_mappings_parallel(self, pairs, organisms):
        """Run independent ``populate_mapping`` calls across a thread pool.

        Each ``(source, target, organism, backend)`` combination writes a
        disjoint slice of ``id_mapping`` and opens its own connection, so the
        jobs are safe to run concurrently. Worker count is controlled by
        ``OMNIPATH_BUILD_MAPPING_WORKERS`` (default 8); see
        ``omnipath-build/docs/build-tuning.md``.
        """
        from concurrent.futures import ThreadPoolExecutor

        jobs = [
            (src, tgt, org, backend)
            for src, tgt, backend in pairs
            for org in organisms
        ]
        if not jobs:
            return

        workers = int(os.environ.get('OMNIPATH_BUILD_MAPPING_WORKERS', '8'))
        workers = max(1, min(workers, len(jobs)))
        _log.info(
            'Building %d mapping tables with %d parallel workers',
            len(jobs),
            workers,
        )

        def _one(job):
            src, tgt, org, backend = job
            try:
                self.populate_mapping(src, tgt, org, backend)
            except Exception as e:
                _log.error(
                    'Failed: %s -> %s (org %d): %s', src, tgt, org, e,
                )

        with ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(_one, jobs))

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
            # Structure-bearing backends (inputs_v2 adapter + PubChem)
            'chebi',
            'chembl',
            'lipidmaps',
            'swisslipids',
            'pubchem',
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

        # Load mapping data via the existing reader. For inputs_v2 backends
        # the limit is honoured at load time (via islice); other backends
        # ignore the unknown kwarg and are capped at COPY time below.
        limit = self._effective_limit(backend_name)
        reader_params = {}
        if limit is not None:
            reader_params['limit'] = limit
        reader = MapReader(
            id_type=id_type,
            target_id_type=target_id_type,
            ncbi_tax_id=ncbi_tax_id,
            backend=backend_name,
            **reader_params,
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
                    if limit is not None and row_count >= limit:
                        break

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
            if self._max_records is not None:
                ids = set(list(ids)[: self._max_records])
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

        self._run_mappings_parallel(PROTEIN_CORE + ENSEMBL, organisms)

        # Reference lists
        for org in organisms:
            try:
                self.populate_reflists(org)
            except Exception as e:
                _log.error('Failed reflists for org %d: %s', org, e)

    # FTP idmapping labels NOT loaded by default (annotation/clustering, not ID
    # translation; they 10x the table). Opt in via OMNIPATH_BUILD_FTP_HEAVY_TYPES.
    _FTP_HEAVY_LABELS = {'GO', 'UniRef100', 'UniRef90', 'UniRef50', 'PDB', 'STRING'}

    def _pg_set(self, cur):
        """Apply session-level (not global) tuning for the in-DB transform."""
        gucs = {
            'work_mem': os.environ.get('OMNIPATH_BUILD_WORK_MEM', '1GB'),
            'maintenance_work_mem': os.environ.get(
                'OMNIPATH_BUILD_MAINTENANCE_WORK_MEM', '2GB'
            ),
            'max_parallel_workers_per_gather': os.environ.get(
                'OMNIPATH_BUILD_MAX_PARALLEL_WORKERS_PER_GATHER', '8'
            ),
            'max_parallel_maintenance_workers': os.environ.get(
                'OMNIPATH_BUILD_MAX_PARALLEL_MAINTENANCE_WORKERS', '4'
            ),
            'synchronous_commit': 'off',
        }
        for k, v in gucs.items():
            # SET does not accept bound parameters; values are operator-controlled
            # env/defaults. Quote as a string literal (PG coerces ints/sizes).
            safe = str(v).replace("'", "")
            cur.execute(f"SET {k} = '{safe}'")

    def populate_from_ftp(self, id_types: set[str] | None = None):
        """Load the full UniProt FTP idmapping into a swappable ``id_mapping_ftp``.

        The complete ``idmapping.dat.gz`` (~18 GB, all organisms incl. the long
        tail with no per-organism file) is loaded by **streaming it into a staging
        table and transforming in-database** — no per-row Python, no awk/split:

        1. ``pypath.inputs.uniprot_ftp.stream_full_idmapping`` downloads (dlmachine,
           cached) and streams the **decompressed** bytes; the build client pipes
           them straight into a client-side ``COPY … FROM STDIN`` of the raw
           ``(ac, id_type_label, id_value)`` rows into an UNLOGGED staging table
           (portable: the DB server needs no file access).
        2. In-DB, set-based: build ``ac → taxid`` from the ``NCBI_TaxID`` rows and a
           ``id_type_label → id_type_id`` map (synonyms included), then build the
           new table resolving ``id_type`` and ``ncbi_tax_id`` as **foreign keys**.
        3. Build its indexes **offline**, then **atomically swap** it in
           (``id_mapping_ftp``) behind the ``id_mapping_all`` view — the curated
           ``id_mapping`` keeps serving with full indexes throughout.

        Heavy non-translation labels (GO/UniRef/PDB/STRING) are excluded unless
        ``OMNIPATH_BUILD_FTP_HEAVY_TYPES=1``. Session tuning via ``OMNIPATH_BUILD_*``
        (see ``omnipath-build/docs/build-tuning.md``).
        """
        from omnipath_utils.db._connection import SCHEMA, get_connection
        from pypath.inputs.uniprot_ftp import IDTYPE_MAP, stream_full_idmapping

        _log.info('Starting full FTP idmapping build (staging + in-DB transform)')
        start = time.time()

        # Backend id + uniprot source type id + label -> id_type_id map.
        load_heavy = os.environ.get('OMNIPATH_BUILD_FTP_HEAVY_TYPES') == '1'
        with Session(self.engine) as session:
            backend = (
                session.query(Backend).filter_by(name='uniprot_ftp').first()
            )
            if not backend:
                _log.error('Backend uniprot_ftp not found in database')
                return
            backend_id = backend.id
            uniprot_type = (
                session.query(IdType).filter_by(name='uniprot').first()
            )
            if not uniprot_type:
                _log.error('ID type "uniprot" not found')
                return
            uniprot_type_id = uniprot_type.id

            label_to_id = {}
            for ftp_label, canonical_name in IDTYPE_MAP.items():
                if canonical_name.startswith('_'):  # _taxid handled separately
                    continue
                if ftp_label in self._FTP_HEAVY_LABELS and not load_heavy:
                    continue
                id_type = (
                    session.query(IdType).filter_by(name=canonical_name).first()
                )
                if id_type:
                    label_to_id[ftp_label] = id_type.id
        _log.info(
            'FTP label map: %d ID-type labels (heavy types %s)',
            len(label_to_id),
            'included' if load_heavy else 'excluded',
        )

        stg = 'idmapping_staging'
        tax = 'idmapping_ac_taxid'
        lmap = 'idmapping_label_map'
        new = f'{SCHEMA}.id_mapping_ftp_new'

        conn = get_connection(self._db_url)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                self._pg_set(cur)

                # 1. Stream decompressed file -> raw COPY into UNLOGGED staging.
                cur.execute(f'DROP TABLE IF EXISTS {stg}')
                cur.execute(
                    f'CREATE UNLOGGED TABLE {stg} '
                    '(ac text, id_type_label text, id_value text)'
                )
                _log.info('Streaming idmapping into staging via COPY...')
                with cur.copy(
                    f'COPY {stg} (ac, id_type_label, id_value) FROM STDIN'
                ) as copy:
                    for block in stream_full_idmapping():
                        copy.write(block)
                conn.commit()
                cur.execute(f'SELECT count(*) FROM {stg}')
                staged = cur.fetchone()[0]
                _log.info('Staged %d raw idmapping rows', staged)

                # 2a. label -> id_type_id map table (for the FK join).
                cur.execute(f'DROP TABLE IF EXISTS {lmap}')
                cur.execute(
                    f'CREATE UNLOGGED TABLE {lmap} '
                    '(id_type_label text PRIMARY KEY, id_type_id smallint)'
                )
                with cur.copy(
                    f'COPY {lmap} (id_type_label, id_type_id) FROM STDIN'
                ) as copy:
                    for label, tid in label_to_id.items():
                        copy.write_row((label, tid))

                # 2b. ac -> taxid from the NCBI_TaxID rows.
                cur.execute(f'DROP TABLE IF EXISTS {tax}')
                cur.execute(
                    f'CREATE UNLOGGED TABLE {tax} AS '
                    f"SELECT ac, id_value::int AS taxid FROM {stg} "
                    "WHERE id_type_label = 'NCBI_TaxID' AND id_value ~ '^[0-9]+$'"
                )
                cur.execute(f'CREATE INDEX ON {tax} (ac)')
                cur.execute(f'ANALYZE {tax}')
                conn.commit()

                # 2c. Build the new table with id_type + taxon resolved as FKs.
                limit = (
                    f' LIMIT {int(self._max_records)}'
                    if self._max_records else ''
                )
                cur.execute(f'DROP TABLE IF EXISTS {new}')
                cur.execute(
                    f'CREATE TABLE {new} AS '
                    f'SELECT {uniprot_type_id}::smallint AS source_type_id, '
                    'lm.id_type_id::smallint AS target_type_id, '
                    'COALESCE(t.taxid, 0)::integer AS ncbi_tax_id, '
                    'left(s.ac, 64)::varchar(64) AS source_id, '
                    'left(s.id_value, 64)::varchar(64) AS target_id, '
                    f'{backend_id}::smallint AS backend_id '
                    f'FROM {stg} s '
                    f'JOIN {lmap} lm USING (id_type_label) '
                    f'LEFT JOIN {tax} t USING (ac)'
                    f'{limit}'
                )
                conn.commit()
                cur.execute(f'SELECT count(*) FROM {new}')
                mapping_count = cur.fetchone()[0]
                _log.info('Transformed %d FTP mapping rows', mapping_count)

                # 3a. Build indexes offline (mirror id_mapping's secondaries).
                cur.execute(
                    f'CREATE INDEX ON {new} '
                    '(source_type_id, target_type_id, ncbi_tax_id, source_id)'
                )
                cur.execute(
                    f'CREATE INDEX ON {new} '
                    '(target_type_id, source_type_id, ncbi_tax_id, target_id)'
                )
                cur.execute(f'ANALYZE {new}')
                conn.commit()

                # 3b. Atomic swap behind the id_mapping_all view.
                cur.execute(f'DROP VIEW IF EXISTS {SCHEMA}.id_mapping_all')
                cur.execute(f'DROP TABLE IF EXISTS {SCHEMA}.id_mapping_ftp')
                cur.execute(
                    f'ALTER TABLE {new} RENAME TO id_mapping_ftp'
                )
                cols = (
                    'source_type_id, target_type_id, ncbi_tax_id, '
                    'source_id, target_id, backend_id'
                )
                cur.execute(
                    f'CREATE VIEW {SCHEMA}.id_mapping_all AS '
                    f'SELECT {cols} FROM {SCHEMA}.id_mapping '
                    f'UNION ALL SELECT {cols} FROM {SCHEMA}.id_mapping_ftp'
                )

                # 4. Cleanup staging.
                for t in (stg, tax, lmap):
                    cur.execute(f'DROP TABLE IF EXISTS {t}')
                conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
        conn.close()

        duration = time.time() - start
        _log.info(
            'Full FTP build complete: %d mappings into id_mapping_ftp, %.1f min',
            mapping_count,
            duration / 60,
        )
        with Session(self.engine) as session:
            session.add(
                BuildInfo(
                    table_name='id_mapping_ftp',
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
        """Populate metabolite ID mappings from UniChem, RaMP, MetaNetX and BiGG.

        Auto-discovers all available ID types from all sources and builds
        all available pairwise mappings.
        """
        self._populate_unichem()
        self._populate_ramp()
        self._populate_metanetx()
        self._populate_bigg()
        self._populate_structures()

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
                                if (
                                    self._max_records is not None
                                    and row_count >= self._max_records
                                ):
                                    break

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
                                if (
                                    self._max_records is not None
                                    and row_count >= self._max_records
                                ):
                                    break

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
            self._run_mappings_parallel(config['mappings'], organisms)

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

    # ------------------------------------------------------------------
    # Structure-bearing namespaces (inputs_v2 adapter backends + PubChem)
    # ------------------------------------------------------------------

    # Each (source_namespace, backend) maps the namespace's own ID to the
    # Standard InChIKey -- the chemical resolver's canonical target (see
    # resolver_policy.yaml). InChIKey is the only structure id that fits the
    # 64-char id_mapping column (InChI/SMILES are kept in the metabo layer,
    # not here). Organism-agnostic (ncbi_tax_id=0).
    _STRUCTURE_BACKENDS = [
        "chebi",
        "chembl",
        "lipidmaps",
        "swisslipids",
        "pubchem",
    ]

    def _populate_structures(self):
        """Build namespace -> Standard InChIKey mappings via the structure
        backends (ChEBI, ChEMBL, LIPID MAPS, SwissLipids, PubChem).

        These feed the chemical resolver's canonical InChIKey projection.
        PubChem is the largest by far and honours ``--pubchem-max-records``.
        """

        _log.info("Building structure-bearing (-> InChIKey) mappings...")

        for backend in self._STRUCTURE_BACKENDS:
            try:
                self.populate_mapping(backend, "inchikey", 0, backend)
            except Exception as e:
                _log.warning(
                    "%s -> inchikey failed: %s", backend, e,
                )
