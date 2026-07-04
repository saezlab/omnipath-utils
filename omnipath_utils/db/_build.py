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
            'gene2ensembl',
            'metanetx',
            'bigg',
            # Structure-bearing backends (inputs_v2 adapter + PubChem)
            'chebi',
            'chembl',
            'lipidmaps',
            'swisslipids',
            'pubchem',
            'kegg_compound',
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

    def populate_reflists_global_swissprot(self):
        """Load the global reviewed (SwissProt) AC set under ncbi_tax_id 0.

        SwissProt membership is organism-agnostic — an AC is reviewed or not,
        independent of taxon. One ``reviewed:true`` query (no organism) yields
        the complete reviewed set (~570k ACs), which drives SwissProt-preference
        for *every* organism in the resolver projection (FR-003), not just the
        handful with per-organism reflists.
        """
        from omnipath_utils.reflists import all_swissprots_global
        from omnipath_utils.db._connection import get_connection

        start = time.time()
        with Session(self.engine) as session:
            uniprot_type = (
                session.query(IdType).filter_by(name='uniprot').first()
            )
            if not uniprot_type:
                _log.error('ID type "uniprot" not found, skipping global reflist')
                return
            type_id = uniprot_type.id

        ids = all_swissprots_global()
        if self._max_records is not None:
            ids = set(list(ids)[: self._max_records])

        conn = get_connection(self._db_url)
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {SCHEMA}.reflist WHERE ncbi_tax_id = 0 "
            f"AND list_name = 'swissprot'"
        )
        with cur.copy(
            f'COPY {SCHEMA}.reflist'
            ' (identifier, id_type_id, ncbi_tax_id, list_name)'
            ' FROM STDIN'
        ) as copy:
            for ac in ids:
                copy.write_row((ac, type_id, 0, 'swissprot'))
        conn.commit()
        conn.close()
        _log.info(
            'Global SwissProt reflist: %d ACs in %.1fs',
            len(ids),
            time.time() - start,
        )

    def load_uniprot_sec_ac(self):
        """Load the secondary -> primary UniProt AC map, organism-agnostic (tax 0).

        ``sec_ac.txt`` is a single global UniProt file (no organism), so it is
        loaded once at ``ncbi_tax_id = 0`` rather than per-organism (adding it to
        the per-organism ``PROTEIN_CORE`` fan-out would run it once per taxon and
        proteome-filter each, which is wrong). Every UniProt-bearing build needs
        it: the resolver views normalise every UniProt to its primary accession
        via this slice so the delivered mapping tables are primary-only (ADR 0006
        -- Utils owns normalization; the "uniprot cleanup" is always part of
        dealing with UniProts). Idempotent (DELETE + COPY the tax-0 slice).
        """
        from pypath.inputs.uniprot import get_uniprot_sec
        from omnipath_utils.db._connection import get_connection

        with Session(self.engine) as session:
            src = session.query(IdType).filter_by(name='uniprot-sec').first()
            tgt = session.query(IdType).filter_by(name='uniprot-pri').first()
            backend = session.query(Backend).filter_by(name='uniprot').first()
            if not src or not tgt or not backend:
                _log.error(
                    'load_uniprot_sec_ac: uniprot-sec/uniprot-pri id_type or '
                    'uniprot backend missing; skipping'
                )
                return
            src_id, tgt_id, backend_id = src.id, tgt.id, backend.id

        start = time.time()
        # organism=None -> every secondary/primary pair, no proteome filter.
        pairs = list(get_uniprot_sec(organism=None))
        if self._max_records is not None:
            pairs = pairs[: self._max_records]

        with Session(self.engine) as session:
            session.execute(
                text(
                    f'DELETE FROM {SCHEMA}.id_mapping WHERE source_type_id = :s '
                    'AND target_type_id = :t AND ncbi_tax_id = 0 '
                    'AND backend_id = :b'
                ),
                {'s': src_id, 't': tgt_id, 'b': backend_id},
            )
            session.commit()

        conn = get_connection(self._db_url)
        n = 0
        try:
            with conn.cursor() as cur:
                with cur.copy(
                    f'COPY {SCHEMA}.id_mapping (source_type_id, target_type_id, '
                    'ncbi_tax_id, source_id, target_id, backend_id) FROM STDIN'
                ) as copy:
                    for sec, pri in pairs:
                        copy.write_row(
                            (src_id, tgt_id, 0, sec[:64], pri[:64], backend_id)
                        )
                        n += 1
            conn.commit()
        finally:
            conn.close()

        _log.info(
            'Loaded %d secondary->primary UniProt AC pairs (tax 0) in %.1fs',
            n, time.time() - start,
        )

    def load_gene2ensembl(self):
        """Load NCBI ``gene2ensembl`` — authoritative ensp/ensg -> entrez, all taxa.

        NCBI is the authority for Entrez Gene; this single all-organism file gives,
        per taxon, ENSP->Entrez and ENSG->Entrez DIRECT and for **every** transcript
        (unlike the UniProt idmapping, which cross-references only one canonical ENSP
        per protein and so misses the other-transcript ENSPs resources supply, e.g.
        STITCH). ``resolver_gene`` anchors Ensembl/gene ids to Entrez through THIS
        (gene space), not through UniProt. Ensembl ids are stored versionless.
        Idempotent (DELETE + COPY the ``gene2ensembl`` backend slice).
        """
        from pypath.inputs.ncbi_gene import gene2ensembl
        from omnipath_utils.db._connection import get_connection

        with Session(self.engine) as session:
            ensg = session.query(IdType).filter_by(name='ensg').first()
            ensp = session.query(IdType).filter_by(name='ensp').first()
            entrez = session.query(IdType).filter_by(name='entrez').first()
            backend = (
                session.query(Backend).filter_by(name='gene2ensembl').first()
            )
            if not backend:
                backend = Backend(name='gene2ensembl')
                session.add(backend)
                session.commit()
            if not ensg or not ensp or not entrez:
                _log.error(
                    'load_gene2ensembl: ensg/ensp/entrez id_type missing; skipping'
                )
                return
            ensg_id, ensp_id, entrez_id, backend_id = (
                ensg.id, ensp.id, entrez.id, backend.id,
            )

        start = time.time()
        with Session(self.engine) as session:
            session.execute(
                text(
                    f'DELETE FROM {SCHEMA}.id_mapping WHERE backend_id = :b'
                ),
                {'b': backend_id},
            )
            session.commit()

        # Deduplicate on the wire (the file has one row per transcript, so ENSG and
        # ENSP<->Entrez pairs repeat heavily).
        seen: set[tuple] = set()
        conn = get_connection(self._db_url)
        n = 0
        try:
            with conn.cursor() as cur:
                with cur.copy(
                    f'COPY {SCHEMA}.id_mapping (source_type_id, target_type_id, '
                    'ncbi_tax_id, source_id, target_id, backend_id) FROM STDIN'
                ) as copy:
                    for rec in gene2ensembl():
                        if rec.entrez is None:
                            continue
                        for src_id, sid in (
                            (ensp_id, rec.ensembl_protein),
                            (ensg_id, rec.ensembl_gene),
                        ):
                            if not sid:
                                continue
                            key = (src_id, rec.ncbi_tax_id, sid, rec.entrez)
                            if key in seen:
                                continue
                            seen.add(key)
                            copy.write_row((
                                src_id, entrez_id, rec.ncbi_tax_id,
                                sid[:64], rec.entrez[:64], backend_id,
                            ))
                            n += 1
                            if self._max_records and n >= self._max_records:
                                break
                        if self._max_records and n >= self._max_records:
                            break
            conn.commit()
        finally:
            conn.close()

        _log.info(
            'Loaded %d gene2ensembl ensp/ensg->entrez rows in %.1fs',
            n, time.time() - start,
        )

    def create_resolver_views(self):
        """(Re)create the canonical resolver projection views (SQL DDL).

        Read by omnipath-build via DuckDB ATTACH (spec 002/003; idempotent):

        * ``resolver_gene`` — maps any in-scope source id (genesymbol / ensg / ensp
          / uniprot / entrez) to its **NCBI Gene (Entrez) anchor** per taxon (the
          gene-anchored canonical identity, US7). ~0.65 s/taxon.
        * ``resolver_protein`` — per-taxon ``source_id -> UniProt`` (primary
          SwissProt where available) — the representative UniProt + the SQL
          replacement for the Python uniprot cleanup in DB-backed mode.
        * ``resolver_gene_protein_global`` — taxon-agnostic UniProt/Entrez ->
          Entrez (the full global slice for the showcase/full build, T069/R25).
        * ``resolver_chemical`` — chemical ``source_id -> InChIKey`` (full PubChem
          + UniChem cross-refs, spec 003 R7) — the authoritative chemical
          structure resolution consumed by the full build.

        Applied at the end of every build mode (full / preset / ftp / metabolites)
        so an additive/incremental load never leaves the views stale or missing.
        """
        import importlib.resources as ir

        from omnipath_utils.db._connection import get_connection

        conn = get_connection(self._db_url)
        cur = conn.cursor()
        for sql_file in ('sql/resolver_protein.sql', 'sql/resolver_chemical.sql'):
            sql = (
                ir.files('omnipath_utils.db')
                .joinpath(sql_file)
                .read_text(encoding='utf-8')
            )
            cur.execute(sql)
        conn.commit()
        conn.close()
        _log.info('Created resolver projection views (protein + chemical)')

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

        # Global reviewed (SwissProt) set — drives organism-agnostic
        # SwissProt-preference in the resolver projection (FR-003).
        try:
            self.populate_reflists_global_swissprot()
        except Exception as e:
            _log.error('Failed global SwissProt reflist: %s', e)

        # Secondary->primary UniProt normalization, always part of handling
        # proteins (ADR 0006). Organism-agnostic, loaded once at tax 0.
        try:
            self.load_uniprot_sec_ac()
        except Exception as e:
            _log.error('Failed to load sec_ac (secondary->primary UniProt): %s', e)

        # Authoritative ensp/ensg->entrez (NCBI gene2ensembl) — gene-space anchor,
        # all organisms, every transcript (the ENSP-coverage fix).
        try:
            self.load_gene2ensembl()
        except Exception as e:
            _log.error('Failed to load gene2ensembl: %s', e)

    # FTP idmapping labels NOT loaded by default (annotation/clustering, not ID
    # translation; they 10x the table). Opt in via OMNIPATH_BUILD_FTP_HEAVY_TYPES.
    _FTP_HEAVY_LABELS = {'GO', 'UniRef100', 'UniRef90', 'UniRef50', 'PDB', 'STRING'}

    def _pg_set(self, cur):
        """Apply session-level (not global) tuning for the in-DB transform."""
        gucs = {
            'work_mem': os.environ.get('OMNIPATH_BUILD_WORK_MEM', '1GB'),
            'maintenance_work_mem': os.environ.get(
                'OMNIPATH_BUILD_MAINTENANCE_WORK_MEM', '4GB'
            ),
            'max_parallel_workers_per_gather': os.environ.get(
                'OMNIPATH_BUILD_MAX_PARALLEL_WORKERS_PER_GATHER', '8'
            ),
            'max_parallel_maintenance_workers': os.environ.get(
                'OMNIPATH_BUILD_MAX_PARALLEL_MAINTENANCE_WORKERS', '8'
            ),
            # Per-worker private hashes instead of one shared hash table in
            # /dev/shm: keeps the billion-row hash join within a modest
            # container shm regardless of deployment.
            'enable_parallel_hash': os.environ.get(
                'OMNIPATH_BUILD_ENABLE_PARALLEL_HASH', 'off'
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
                # OMNIPATH_BUILD_FTP_FILE: stream a pre-staged .gz directly
                # (skips the download-manager lookup).
                ftp_file = os.environ.get('OMNIPATH_BUILD_FTP_FILE')
                with cur.copy(
                    f'COPY {stg} (ac, id_type_label, id_value) FROM STDIN'
                ) as copy:
                    for block in stream_full_idmapping(path=ftp_file):
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
                    # UNLOGGED: skip WAL on this large rebuildable build output.
                    f'CREATE UNLOGGED TABLE {new} AS '
                    f'SELECT {uniprot_type_id}::smallint AS source_type_id, '
                    'lm.id_type_id::smallint AS target_type_id, '
                    'COALESCE(t.taxid, 0)::integer AS ncbi_tax_id, '
                    'left(s.ac, 64)::varchar(64) AS source_id, '
                    # Normalise Ensembl IDs to versionless (ADR 0006): the FTP
                    # file carries `.N` version suffixes (ENSG/ENST/ENSP) but
                    # resources supply versionless IDs, so a versioned target
                    # would never join. Strip the trailing version here so the
                    # delivered mapping table is already canonical.
                    "left(CASE WHEN s.id_type_label IN "
                    "('Ensembl', 'Ensembl_TRS', 'Ensembl_PRO') "
                    "THEN regexp_replace(s.id_value, '\\.[0-9]+$', '') "
                    "ELSE s.id_value END, 64)::varchar(64) AS target_id, "
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

                # 3a. Build covering indexes offline (mirror id_mapping's
                # secondaries, + INCLUDE the payload so lookups are index-only
                # scans on this billion-row table). Parallel (unlogged -> no WAL).
                cur.execute(
                    f'CREATE INDEX ON {new} '
                    '(source_type_id, target_type_id, ncbi_tax_id, source_id) '
                    'INCLUDE (target_id)'
                )
                cur.execute(
                    f'CREATE INDEX ON {new} '
                    '(target_type_id, source_type_id, ncbi_tax_id, target_id) '
                    'INCLUDE (source_id)'
                )
                cur.execute(f'ANALYZE {new}')
                conn.commit()

                # 3b. Atomic swap behind the id_mapping_all view. CASCADE drops
                # the resolver views (resolver_gene / resolver_protein /
                # resolver_gene_protein_global) that depend on id_mapping_ftp;
                # create_resolver_views() below rebuilds them all from
                # resolver_protein.sql.
                cur.execute(f'DROP VIEW IF EXISTS {SCHEMA}.id_mapping_all CASCADE')
                cur.execute(f'DROP TABLE IF EXISTS {SCHEMA}.id_mapping_ftp CASCADE')
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
                # 5. Crash-safety: a crash TRUNCATEs an UNLOGGED table on
                # recovery — the 744M map was lost to an OOM reboot once. The
                # load + index builds above stay unlogged (fast, no per-row WAL);
                # this one-time SET LOGGED (own txn, after the swap commits)
                # writes the table to WAL so it survives crashes. Reads are
                # unaffected. Opt out for a throwaway build: set
                # OMNIPATH_BUILD_FTP_UNLOGGED=1.
                if os.environ.get('OMNIPATH_BUILD_FTP_UNLOGGED', '') not in (
                    '1', 'true', 'True', 'yes',
                ):
                    _log.info('SET LOGGED on id_mapping_ftp (crash-safety)')
                    cur.execute(
                        f'ALTER TABLE {SCHEMA}.id_mapping_ftp SET LOGGED'
                    )
                    conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
        conn.close()

        # Secondary->primary UniProt normalization (ADR 0006) must exist before
        # the resolver views, which read it to canonicalise every UniProt.
        try:
            self.load_uniprot_sec_ac()
        except Exception as e:
            _log.error('Failed to load sec_ac (secondary->primary UniProt): %s', e)

        # Authoritative ensp/ensg->entrez (NCBI gene2ensembl) — resolver_gene
        # anchors Ensembl/gene ids through this gene-space map, not UniProt.
        try:
            self.load_gene2ensembl()
        except Exception as e:
            _log.error('Failed to load gene2ensembl: %s', e)

        # The resolver projection reads id_mapping_ftp, so (re)create it now.
        self.create_resolver_views()

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
        self._populate_chemical_long()

    # ------------------------------------------------------------------
    # Long-value chemical mappings: names + structures (id_mapping_long)
    # ------------------------------------------------------------------

    #: Values longer than this (bytes) are skipped, not truncated, to stay
    #: within the B-tree index-key bound and never forge a wrong key
    #: (FR-017/FR-019 edge case). Full names/IUPAC and typical InChI/SMILES are
    #: far below this; only pathological structure strings exceed it.
    _MAX_LONG_KEY = 2000

    @staticmethod
    def _long_rows(
        data: dict,
        src_type_id: int,
        tgt_type_id: int,
        backend_id: int,
        is_name: bool,
        max_key: int = _MAX_LONG_KEY,
        limit: int | None = None,
    ) -> tuple[list[tuple], int]:
        """Pure projection of a ``{source: {targets}}`` mapping into
        ``id_mapping_long`` COPY rows ``(src_type_id, tgt_type_id, 0, source_id,
        source_label, target_id, backend_id)``.

        Name source keys are lowercased + whitespace-stripped with the original
        kept as ``source_label`` (FR-002); structure/db-id keys are verbatim
        (``source_label`` is ``None``; FR-019). Empty and over-``max_key`` values
        are skipped (counted), never truncated. ``limit`` caps the row count
        (FR-012). Returns ``(rows, skipped)``.
        """
        rows: list[tuple] = []
        skipped = 0
        for source_id, target_ids in data.items():
            raw = str(source_id).strip()
            if is_name:
                key, label = raw.lower(), raw
            else:
                key, label = raw, None
            if not key or len(key.encode('utf-8')) > max_key:
                skipped += 1
                continue
            for target_id in target_ids:
                tgt = str(target_id).strip()
                if not tgt or len(tgt.encode('utf-8')) > max_key:
                    skipped += 1
                    continue
                rows.append(
                    (src_type_id, tgt_type_id, 0, key, label, tgt, backend_id)
                )
                if limit is not None and len(rows) >= limit:
                    return rows, skipped
        return rows, skipped

    def _long_type_class(self, type_name: str) -> str:
        """Classify an id_type for ``id_mapping_long`` case handling."""
        from omnipath_utils.db._query import NAME_TYPES, STRUCTURE_TYPES

        if type_name in NAME_TYPES:
            return 'name'
        if type_name in STRUCTURE_TYPES:
            return 'structure'
        return 'id'

    def _populate_long_slice(
        self,
        data: dict,
        source_type: str,
        target_type: str,
        backend_name: str,
    ) -> int:
        """Load one ``(source_type, target_type, backend)`` slice into
        ``id_mapping_long`` -- the long-value (name/structure) sibling table.

        Name source keys are lowercased + whitespace-stripped (case-insensitive
        matching, FR-002) with the original case kept in ``source_label``;
        structure source keys (``inchi``/``smiles``) are stored verbatim
        (FR-019). Idempotent: the slice is DELETEd then COPYd (R6). Honours the
        backend's effective ``--max-records`` cap (FR-012). Over-long values are
        skipped and counted, never truncated. Returns the row count written.
        """
        if not data:
            return 0

        with Session(self.engine) as session:
            src_type = session.query(IdType).filter_by(name=source_type).first()
            tgt_type = session.query(IdType).filter_by(name=target_type).first()
            backend = session.query(Backend).filter_by(name=backend_name).first()
            if not src_type or not tgt_type or not backend:
                _log.error(
                    'id_mapping_long: missing id_type/backend for %s -> %s (%s)',
                    source_type, target_type, backend_name,
                )
                return 0
            src_type_id, tgt_type_id, backend_id = (
                src_type.id, tgt_type.id, backend.id,
            )

        is_name = self._long_type_class(source_type) == 'name'
        limit = self._effective_limit(backend_name)
        start = time.time()

        # Idempotent: replace this slice (organism-agnostic, tax 0).
        with Session(self.engine) as session:
            session.execute(
                text(
                    f'DELETE FROM {SCHEMA}.id_mapping_long'
                    ' WHERE source_type_id = :src AND target_type_id = :tgt'
                    ' AND ncbi_tax_id = 0 AND backend_id = :bk'
                ),
                {'src': src_type_id, 'tgt': tgt_type_id, 'bk': backend_id},
            )
            session.commit()

        rows, skipped = self._long_rows(
            data, src_type_id, tgt_type_id, backend_id, is_name,
            self._MAX_LONG_KEY, limit,
        )

        from omnipath_utils.db._connection import get_connection

        conn = get_connection(self._db_url)
        row_count = 0
        try:
            with conn.cursor() as cur:
                with cur.copy(
                    f'COPY {SCHEMA}.id_mapping_long'
                    ' (source_type_id, target_type_id, ncbi_tax_id,'
                    ' source_id, source_label, target_id, backend_id)'
                    ' FROM STDIN'
                ) as copy:
                    for row in rows:
                        copy.write_row(row)
                        row_count += 1
            conn.commit()
        finally:
            conn.close()

        duration = time.time() - start
        with Session(self.engine) as session:
            session.add(
                BuildInfo(
                    table_name='id_mapping_long',
                    source_type=source_type,
                    target_type=target_type,
                    ncbi_tax_id=0,
                    backend=backend_name,
                    row_count=row_count,
                    duration_secs=duration,
                    status='done',
                )
            )
            session.commit()

        _log.info(
            'id_mapping_long %s -> %s (%s): %d rows%s in %.1fs',
            source_type, target_type, backend_name, row_count,
            f' ({skipped} skipped)' if skipped else '', duration,
        )
        return row_count

    def _populate_chemical_long(self):
        """Build the long-value chemical layer in ``id_mapping_long``:
        names (name/synonym/iupac/traditional_iupac) and structures
        (inchi/smiles) for every targeted resource, with the full pairwise set
        across the three identifier classes (US1/US2).

        Per-resource builders are dispatched here; each writes its slices via
        :meth:`_populate_long_slice` and its counts to ``build_info``. The
        dispatcher records per-class roll-ups at the end (FR-011).
        """
        _log.info('Building long-value chemical mappings (names + structures)...')

        builders = [
            ('chebi', self._long_chebi),
            ('hmdb', self._long_hmdb),
            ('chembl', self._long_chembl),
            ('kegg_compound', self._long_kegg),
            ('ramp', self._long_ramp),
        ]
        for label, fn in builders:
            try:
                fn()
            except Exception as e:
                _log.warning('chemical_long: %s builder failed: %s', label, e)

        self._record_long_rollups()

    def _emit_long_relations(
        self,
        rows,
        backend: str,
        name_cols: dict,
        id_cols: dict,
        struct_cols: dict | None = None,
    ) -> int:
        """Materialise the full cross-class relation set for one resource (R3).

        From each raw row's name column(s) (``name_cols``: long-type -> column),
        database-ID column(s) (``id_cols``; the first is the resource's own
        primary id) and structure column(s) (``struct_cols``: inchi/smiles ->
        column), emit every derivable pair across the three identifier classes
        into ``id_mapping_long`` -- names<->ids, names<->structures,
        id<->structures, structure<->structure -- not hub-only (FR-004/FR-019).
        Database-ID<->database-ID pairs are intentionally excluded (they belong
        in ``id_mapping`` via UniChem). Loads each slice via the idempotent
        long-value COPY helper and returns the total row count.
        """
        from collections import defaultdict

        from omnipath_utils.mapping.backends._inputs_v2_adapter import _as_values

        struct_cols = struct_cols or {}
        primary = next(iter(id_cols), None)
        struct_types = list(struct_cols)
        pairs: dict = defaultdict(lambda: defaultdict(set))

        for row in rows:
            names = {nt: _as_values(row.get(c)) for nt, c in name_cols.items()}
            ids = {it: _as_values(row.get(c)) for it, c in id_cols.items()}
            structs = {st: _as_values(row.get(c)) for st, c in struct_cols.items()}

            # names -> every database id and structure (forward)
            for nt, nvals in names.items():
                for nv in nvals:
                    for it, ivals in ids.items():
                        if ivals:
                            pairs[(nt, it)][nv].update(ivals)
                    for st, svals in structs.items():
                        if svals:
                            pairs[(nt, st)][nv].update(svals)

            # reverse from the resource's primary id -> names and structures
            for pv in ids.get(primary, []):
                for nt, nvals in names.items():
                    if nvals:
                        pairs[(primary, nt)][pv].update(nvals)
                for st, svals in structs.items():
                    if svals:
                        pairs[(primary, st)][pv].update(svals)
                        for sv in svals:
                            pairs[(st, primary)][sv].add(pv)

            # structure <-> structure (inchi <-> smiles)
            if len(struct_types) == 2:
                a, b = struct_types
                for av in structs[a]:
                    if structs[b]:
                        pairs[(a, b)][av].update(structs[b])
                for bv in structs[b]:
                    if structs[a]:
                        pairs[(b, a)][bv].update(structs[a])

        total = 0
        for (st, tt), data in pairs.items():
            data = {k: v for k, v in data.items() if v}
            if data:
                total += self._populate_long_slice(data, st, tt, backend)
        return total

    # Per-resource long-value builders (US1 T015/T016, US2 T024/T025/T026/T027a).
    def _long_chebi(self):
        """ChEBI names/synonyms <-> chebi + xref ids + structures. US1 (T015)."""
        from omnipath_utils.mapping.backends._inputs_v2_adapter import raw_rows

        rows = raw_rows('chebi', 'molecules', self._effective_limit('chebi'))
        n = self._emit_long_relations(
            rows, 'chebi',
            name_cols={'name': 'name', 'synonym': 'synonyms'},
            id_cols={
                'chebi': 'chebi_id', 'hmdb': 'hmdb', 'kegg': 'kegg_compound',
                'pubchem': 'pubchem_compound', 'lipidmaps': 'lipidmaps',
            },
            struct_cols={'inchi': 'inchi', 'smiles': 'smiles'},
        )
        _log.info('chemical_long chebi: %d rows', n)

    def _long_hmdb(self):
        """HMDB synonyms -> chebi/hmdb. US1 (T016).

        HMDB's native ``hmdb_metabolites.zip`` is behind Cloudflare and may fail
        to download; the dispatcher isolates that failure and HMDB coverage
        still arrives via ChEBI's ``hmdb`` xref and RaMP's HMDB-derived
        synonyms.
        """
        from pypath.inputs.hmdb import metabolites as hmdb_meta

        sc = hmdb_meta.synonyms_chebi()  # {synonym: chebi or {chebi}}
        syn_chebi: dict = {}
        for syn, chebi in sc.items():
            vals = chebi if isinstance(chebi, (set, list, tuple)) else {chebi}
            syn_chebi.setdefault(str(syn), set()).update(
                f'CHEBI:{v}' if not str(v).startswith('CHEBI:') else str(v)
                for v in vals if v
            )
        n = self._populate_long_slice(syn_chebi, 'synonym', 'chebi', 'hmdb')
        _log.info('chemical_long hmdb: %d synonym->chebi rows', n)

    def _long_chembl(self):
        """ChEMBL name/synonym <-> chembl/chebi + structures. US2 (T024)."""
        from omnipath_utils.mapping.backends._inputs_v2_adapter import raw_rows

        rows = raw_rows('chembl', 'molecules', self._effective_limit('chembl'))
        name_cols = {'name': 'pref_name'}
        if rows and 'synonyms' in rows[0]:
            name_cols['synonym'] = 'synonyms'
        n = self._emit_long_relations(
            rows, 'chembl',
            name_cols=name_cols,
            id_cols={'chembl': 'chembl_id', 'chebi': 'chebi_id'},
            struct_cols={'inchi': 'standard_inchi', 'smiles': 'canonical_smiles'},
        )
        _log.info('chemical_long chembl: %d rows', n)

    def _long_kegg(self):
        """KEGG compound names <-> kegg/chebi. US2 (T025)."""
        from pypath.inputs.kegg import kegg_compound_chebi, kegg_compound_names

        names = kegg_compound_names()      # {kegg_id: [names]}
        kc = kegg_compound_chebi()         # {kegg_id: 'CHEBI:nnn'}
        name_kegg: dict = {}
        name_chebi: dict = {}
        kegg_name: dict = {}
        for kid, nlist in names.items():
            nlist = nlist if isinstance(nlist, (list, set, tuple)) else [nlist]
            kegg_name.setdefault(kid, set()).update(str(n) for n in nlist if n)
            for nm in nlist:
                if not nm:
                    continue
                name_kegg.setdefault(str(nm), set()).add(kid)
                if kid in kc:
                    name_chebi.setdefault(str(nm), set()).add(kc[kid])
        total = 0
        total += self._populate_long_slice(name_kegg, 'name', 'kegg', 'kegg_compound')
        total += self._populate_long_slice(kegg_name, 'kegg', 'name', 'kegg_compound')
        total += self._populate_long_slice(name_chebi, 'name', 'chebi', 'kegg_compound')
        _log.info('chemical_long kegg_compound: %d rows', total)

    @staticmethod
    def _norm_chem_id(id_type: str, value: str) -> str:
        """Normalise a RaMP curie to the DB's id format (e.g. CHEBI:15377)."""
        v = str(value)
        if ':' in v:
            v = v.split(':', 1)[1]
        if id_type == 'chebi':
            return f'CHEBI:{v}'
        return v

    def _long_ramp(self):
        """RaMP synonyms <-> chebi/hmdb (analytesynonym). US2 (T026).

        ``ramp_synonym_mapping(hub)`` returns ``{hub_id: {synonyms}}``; emit both
        directions (synonym->hub and hub->synonym), normalising the hub id to the
        DB format.
        """
        from pypath.inputs.ramp import ramp_synonym_mapping

        total = 0
        for hub in ('chebi', 'hmdb'):
            try:
                data = ramp_synonym_mapping(hub, curies=True)  # {hub_id:{syn}}
            except Exception as e:
                _log.debug('RaMP synonym<->%s failed: %s', hub, e)
                continue
            syn_hub: dict = {}
            hub_syn: dict = {}
            for hid, syns in data.items():
                hid_n = self._norm_chem_id(hub, hid)
                clean = {str(s) for s in syns if s}
                hub_syn.setdefault(hid_n, set()).update(clean)
                for s in clean:
                    syn_hub.setdefault(s, set()).add(hid_n)
            total += self._populate_long_slice(syn_hub, 'synonym', hub, 'ramp')
            total += self._populate_long_slice(hub_syn, hub, 'synonym', 'ramp')
        _log.info('chemical_long ramp: %d synonym rows', total)

    def _record_long_rollups(self):
        """Write per-class roll-up counts for the long-value layer to
        ``build_info`` (``names.total`` / ``structures.total``) from the slices
        already recorded this build."""
        from omnipath_utils.db._query import NAME_TYPES, STRUCTURE_TYPES

        with Session(self.engine) as session:
            rows = session.execute(
                text(
                    f"SELECT source_type, target_type, row_count"
                    f" FROM {SCHEMA}.build_info"
                    " WHERE table_name = 'id_mapping_long'"
                    " AND status = 'done'"
                )
            ).fetchall()
            names_total = sum(
                r[2] or 0 for r in rows
                if r[0] in NAME_TYPES or r[1] in NAME_TYPES
            )
            struct_total = sum(
                r[2] or 0 for r in rows
                if r[0] in STRUCTURE_TYPES or r[1] in STRUCTURE_TYPES
            )
            for kind, total in (
                ('names', names_total), ('structures', struct_total),
            ):
                session.add(
                    BuildInfo(
                        table_name='id_mapping_long',
                        source_type=kind,
                        target_type='total',
                        ncbi_tax_id=0,
                        backend='rollup',
                        row_count=total,
                        status='done',
                    )
                )
            session.commit()
        _log.info(
            'id_mapping_long roll-up: %d name rows, %d structure rows',
            names_total, struct_total,
        )

    def record_ftp_types(self):
        """Precompute and cache the set of id_types present in
        ``id_mapping_ftp`` into ``build_info`` (``table_name = 'ftp_types'``).

        Read at query time by the FR-018 fallback gate so it never has to scan
        the 744 M-row table. Safe/idempotent: replaces any prior row. A no-op if
        the FTP table is absent.
        """
        with Session(self.engine) as session:
            present = session.execute(
                text(f"SELECT to_regclass('{SCHEMA}.id_mapping_ftp')")
            ).scalar()
            if present is None:
                _log.info('record_ftp_types: id_mapping_ftp absent, skipping')
                return
            names = session.execute(
                text(
                    f'SELECT name FROM {SCHEMA}.id_type WHERE id IN ('
                    f' SELECT DISTINCT source_type_id FROM {SCHEMA}.id_mapping_ftp'
                    f' UNION'
                    f' SELECT DISTINCT target_type_id FROM {SCHEMA}.id_mapping_ftp)'
                    ' ORDER BY name'
                )
            ).scalars().all()
            session.execute(
                text(
                    f"DELETE FROM {SCHEMA}.build_info WHERE table_name='ftp_types'"
                )
            )
            # One row per type name (build_info.source_type is varchar(64)).
            for name in names:
                session.add(
                    BuildInfo(
                        table_name='ftp_types',
                        source_type=name,
                        target_type=None,
                        ncbi_tax_id=0,
                        backend='ftp_types',
                        row_count=len(names),
                        status='done',
                    )
                )
            session.commit()
        _log.info('record_ftp_types: cached %d FTP id_types', len(names))

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
            self.populate_from_ftp()  # loads sec_ac + resolver views internally
        else:
            self._run_mappings_parallel(config['mappings'], organisms)
            # Secondary->primary UniProt normalization, always part of handling
            # proteins (ADR 0006); organism-agnostic (tax 0). Skipped for the
            # chemical-only preset (no protein mappings).
            if config['mappings']:
                try:
                    self.load_uniprot_sec_ac()
                except Exception as e:
                    _log.error('Failed to load sec_ac: %s', e)

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
                if backend == "pubchem":
                    # PubChem cid->inchikey is ~119M rows -- the single largest
                    # chemical mapping. Use the parallel, streamed loader rather
                    # than the serial materialise-dict-then-one-COPY path.
                    self._populate_pubchem_inchikey()
                else:
                    self.populate_mapping(backend, "inchikey", 0, backend)
            except Exception as e:
                _log.warning(
                    "%s -> inchikey failed: %s", backend, e,
                )

    def _copy_pairs_parallel(
        self,
        pairs,
        src_type_id: int,
        tgt_type_id: int,
        ncbi_tax_id: int,
        backend_id: int,
        n_workers: int | None = None,
        batch_size: int = 20000,
    ) -> int:
        """Fan a stream of ``(source_id, target_id)`` pairs into N concurrent COPYs.

        One feeder (this call) pulls from ``pairs`` -- typically a generator that
        streams + decompresses straight off disk -- batches the rows, and
        dispatches them over a bounded queue to ``n_workers`` worker threads,
        each holding its own connection and a single open ``COPY ... FROM
        STDIN``. This parallelises the write (the bottleneck for a heavily
        indexed target like ``id_mapping``: 3 indexes => ~45k rows/s on one
        COPY) and keeps memory bounded (no full ``{source: {targets}}`` dict).

        Each worker commits its own portion, so an interrupted load can leave a
        partial slice; callers DELETE the slice first and a re-run is idempotent.
        Returns the total row count written.
        """
        import queue
        import threading
        from omnipath_utils.db._connection import get_connection

        if n_workers is None:
            n_workers = min(
                int(os.environ.get("OMNIPATH_BUILD_MAPPING_WORKERS", "8")),
                16,
            )
        n_workers = max(1, n_workers)

        q: queue.Queue = queue.Queue(maxsize=n_workers * 4)
        _SENTINEL = object()
        counts = [0] * n_workers
        errors: list[Exception] = []
        copy_sql = (
            f"COPY {SCHEMA}.id_mapping "
            "(source_type_id, target_type_id, ncbi_tax_id, "
            "source_id, target_id, backend_id) FROM STDIN"
        )

        def worker(idx: int):
            try:
                conn = get_connection(self._db_url)
                with conn.cursor() as cur:
                    cur.execute("SET synchronous_commit = off")
                    with cur.copy(copy_sql) as cp:
                        while True:
                            batch = q.get()
                            if batch is _SENTINEL:
                                q.task_done()
                                break
                            for src_id, tgt_id in batch:
                                cp.write_row((
                                    src_type_id, tgt_type_id, ncbi_tax_id,
                                    src_id, tgt_id, backend_id,
                                ))
                            counts[idx] += len(batch)
                            q.task_done()
                conn.commit()
                conn.close()
            except Exception as e:  # noqa: BLE001
                errors.append(e)
                _log.error("COPY worker %d failed: %s", idx, e)

        threads = [
            threading.Thread(target=worker, args=(i,), daemon=True)
            for i in range(n_workers)
        ]
        for t in threads:
            t.start()

        batch: list = []
        for pair in pairs:
            batch.append(pair)
            if len(batch) >= batch_size:
                q.put(batch)
                batch = []
                if errors:
                    break
        if batch and not errors:
            q.put(batch)
        for _ in range(n_workers):
            q.put(_SENTINEL)
        for t in threads:
            t.join()

        if errors:
            raise errors[0]

        return sum(counts)

    def _populate_pubchem_inchikey(self, n_workers: int | None = None):
        """Parallel, streamed load of the full PubChem CID -> InChIKey mapping.

        The single largest chemical namespace (~119M rows). Rather than
        materialising the whole ``{cid: {inchikey}}`` dict in RAM (~11 GB) and
        draining it through one serial COPY into the 3-index ``id_mapping``
        table, this streams ``pubchem_mapping`` straight into N concurrent COPY
        workers -- bounded memory, ~N x the write throughput. Honours
        ``--pubchem-max-records`` (the per-backend effective limit). (T020/T021)
        """
        from itertools import islice
        from pypath.inputs.pubchem import pubchem_mapping

        with Session(self.engine) as session:
            src = session.query(IdType).filter_by(name="pubchem").first()
            tgt = session.query(IdType).filter_by(name="inchikey").first()
            bk = session.query(Backend).filter_by(name="pubchem").first()
            if not src or not tgt or not bk:
                _log.error("pubchem/inchikey id_type or backend missing")
                return
            src_id, tgt_id, bk_id = src.id, tgt.id, bk.id

        # Replace the existing pubchem -> inchikey slice: delete (committed),
        # then the parallel COPY appends. A re-run is idempotent.
        with Session(self.engine) as session:
            session.execute(
                text(
                    f"DELETE FROM {SCHEMA}.id_mapping WHERE source_type_id = :s "
                    "AND target_type_id = :t AND ncbi_tax_id = 0 "
                    "AND backend_id = :b"
                ),
                {"s": src_id, "t": tgt_id, "b": bk_id},
            )
            session.commit()

        limit = self._effective_limit("pubchem")
        rows = pubchem_mapping("inchikey", source="cid")  # yields (cid, inchikey)
        if limit is not None:
            rows = islice(rows, limit)

        start = time.time()
        n = self._copy_pairs_parallel(rows, src_id, tgt_id, 0, bk_id, n_workers)
        _log.info(
            "PubChem cid->inchikey: %d rows in %.1fs (parallel streamed COPY)",
            n, time.time() - start,
        )
