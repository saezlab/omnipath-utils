-- Canonical chemical resolution projection (spec 003 US4 / R7, chemical-resolver
-- repoint). Maps a chemical source identifier -> standard InChIKey (the canonical
-- structure key the build resolves chemicals to), so omnipath-build consumes the
-- authoritative utils mappings (full PubChem cid->InChIKey + UniChem cross-refs)
-- instead of streaming a capped/sharded PubChem SDF natively.
--
-- Three contributions, all (source_type, source_id) -> InChIKey:
--   * direct   X -> InChIKey            (pubchem 123.9M, chembl, chebi, lipidmaps,
--                                         swisslipids — sources that carry a key)
--   * pubchem-hub  X -> pubchem -> InChIKey  (UniChem bridge; brings hmdb/drugbank/
--                                         chembl/chebi into structure resolution)
--   * chebi-hub    X -> chebi   -> InChIKey  (kegg/hmdb via the ChEBI hub)
--
-- Bridges are restricted to build-known source types (so the projection stays
-- bounded and every row maps to a build identifier type). UNION ALL — exact dups
-- across contributions are deduplicated downstream by omnipath-build's
-- resolver_canonical_entity / needed_resolver_lookup. Read by omnipath-build at
-- resolver-export time via DuckDB ATTACH (streamed straight to parquet).

CREATE SCHEMA IF NOT EXISTS omnipath_utils;

-- MATERIALIZED (2026-07-02): as a plain view this ~124M-row projection (PubChem
-- cid->InChIKey + bridges) expands its UNION ALL + id_mapping joins on every read,
-- which stalled STITCH canonicalization (the build COPYs the whole chemical
-- resolver for CID endpoints). Materialise + index so the read is a sequential
-- table scan and keyed lookups (by source_type, source_id) are index probes.
-- Rebuilt on reload alongside resolver_gene (create_resolver_views). Drop whichever
-- relkind exists (IF EXISTS does not cover a view<->matview mismatch).
DO $$
DECLARE k "char";
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'omnipath_utils' AND c.relname = 'resolver_chemical';
  IF k = 'v' THEN EXECUTE 'DROP VIEW omnipath_utils.resolver_chemical CASCADE';
  ELSIF k = 'm' THEN EXECUTE 'DROP MATERIALIZED VIEW omnipath_utils.resolver_chemical CASCADE';
  END IF;
END $$;
CREATE MATERIALIZED VIEW omnipath_utils.resolver_chemical AS
WITH ik AS (SELECT id FROM omnipath_utils.id_type WHERE name = 'inchikey'),
     pc AS (SELECT id FROM omnipath_utils.id_type WHERE name = 'pubchem'),
     ce AS (SELECT id FROM omnipath_utils.id_type WHERE name = 'chebi'),
pubchem_ik AS (
    SELECT m.source_id AS hub, m.target_id AS inchikey
    FROM omnipath_utils.id_mapping m
    WHERE m.source_type_id = (SELECT id FROM pc)
      AND m.target_type_id = (SELECT id FROM ik)
),
chebi_ik AS (
    SELECT m.source_id AS hub, m.target_id AS inchikey
    FROM omnipath_utils.id_mapping m
    WHERE m.source_type_id = (SELECT id FROM ce)
      AND m.target_type_id = (SELECT id FROM ik)
),
direct AS (
    SELECT st.name AS source_type, m.source_id, m.target_id AS inchikey
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON st.id = m.source_type_id
    WHERE m.target_type_id = (SELECT id FROM ik)
),
bridge_pubchem AS (
    SELECT st.name AS source_type, m.source_id, h.inchikey
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON st.id = m.source_type_id
    JOIN pubchem_ik h ON h.hub = m.target_id
    WHERE m.target_type_id = (SELECT id FROM pc)
      AND st.name IN ('chembl', 'hmdb', 'chebi', 'drugbank')
),
bridge_chebi AS (
    SELECT st.name AS source_type, m.source_id, h.inchikey
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON st.id = m.source_type_id
    JOIN chebi_ik h ON h.hub = m.target_id
    WHERE m.target_type_id = (SELECT id FROM ce)
      AND st.name IN ('kegg', 'hmdb')
)
SELECT source_type, source_id, inchikey FROM direct
WHERE source_id IS NOT NULL AND inchikey IS NOT NULL
UNION ALL
SELECT source_type, source_id, inchikey FROM bridge_pubchem
WHERE source_id IS NOT NULL AND inchikey IS NOT NULL
UNION ALL
SELECT source_type, source_id, inchikey FROM bridge_chebi
WHERE source_id IS NOT NULL AND inchikey IS NOT NULL;

-- Keyed-lookup index: the build probes by (source_type, source_id).
CREATE INDEX IF NOT EXISTS resolver_chemical_key_idx
    ON omnipath_utils.resolver_chemical (source_type, source_id);
