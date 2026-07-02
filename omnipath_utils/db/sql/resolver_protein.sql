-- Canonical per-taxon protein resolution projection (spec 002, M-Proteins / FR-003/005).
--
-- Set-based replacement for the per-id Python ``uniprot_cleanup`` in DB-backed
-- mode. Produces (ncbi_tax_id, source_type, source_id) -> primary SwissProt
-- UniProt, reusable by:
--   * omnipath-build (DuckDB ATTACH reads it with a pushed-down taxon filter),
--   * the web app / API (same canonical mapping).
--
-- Coverage: the comprehensive full-UniProt idmapping is stored uniprot->X, so we
-- invert it; curated id_mapping is added in both directions. SwissProt-preference
-- and proteome filtering apply only where a reflist exists for the taxon
-- (currently human/mouse); other organisms still resolve to the available AC.

CREATE SCHEMA IF NOT EXISTS omnipath_utils;

CREATE OR REPLACE VIEW omnipath_utils.resolver_protein_source AS
-- full-UniProt: native uniprot -> X, inverted to X -> uniprot
SELECT m.ncbi_tax_id, tt.name AS source_type,
       m.target_id AS source_id, m.source_id AS ac
FROM omnipath_utils.id_mapping_ftp m
JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id
 AND tt.name IN ('genesymbol', 'ensg', 'ensp', 'entrez', 'refseqp')
UNION ALL
-- curated, native uniprot -> X (inverted)
SELECT m.ncbi_tax_id, tt.name, m.target_id, m.source_id
FROM omnipath_utils.id_mapping m
JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id
 AND tt.name IN ('genesymbol', 'ensg', 'ensp', 'entrez', 'refseqp')
UNION ALL
-- curated, forward X -> uniprot
SELECT m.ncbi_tax_id, st.name, m.source_id, m.target_id
FROM omnipath_utils.id_mapping m
JOIN omnipath_utils.id_type st ON m.source_type_id = st.id
 AND st.name IN ('genesymbol', 'ensg', 'ensp', 'entrez', 'refseqp')
JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'uniprot'
UNION ALL
-- uniprot identity (so a uniprot evidence id canonicalises to its primary)
SELECT m.ncbi_tax_id, 'uniprot', m.target_id, m.source_id
FROM omnipath_utils.id_mapping_ftp m
JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'uniprot_entry';

CREATE OR REPLACE VIEW omnipath_utils.resolver_protein AS
WITH norm AS (
    -- secondary -> primary AC normalisation
    SELECT s.ncbi_tax_id, s.source_type, s.source_id,
           COALESCE(sp.pri, s.ac) AS ac
    FROM omnipath_utils.resolver_protein_source s
    LEFT JOIN (
        SELECT m.source_id AS sec, m.target_id AS pri
        FROM omnipath_utils.id_mapping m
        JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot-sec'
        JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'uniprot-pri'
    ) sp ON sp.sec = s.ac
),
flagged AS (
    SELECT n.ncbi_tax_id, n.source_type, n.source_id, n.ac,
           (r.identifier IS NOT NULL) AS is_swissprot,
           bool_or(r.identifier IS NOT NULL) OVER (
               PARTITION BY n.ncbi_tax_id, n.source_type, n.source_id
           ) AS grp_has_swissprot
    FROM norm n
    -- SwissProt (reviewed) status is organism-agnostic: an AC is reviewed or
    -- not, independent of the query taxon. Match by AC alone so a single global
    -- reviewed set drives SwissProt-preference for every organism.
    LEFT JOIN omnipath_utils.reflist r
      ON r.list_name = 'swissprot'
     AND r.identifier = n.ac
)
SELECT DISTINCT ncbi_tax_id, source_type, source_id, ac AS uniprot
FROM flagged
WHERE ((grp_has_swissprot AND is_swissprot) OR NOT grp_has_swissprot)
  AND source_id IS NOT NULL
  AND ac ~ '^([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})$';


-- Gene-anchor projection (spec 002, M-Genes / US7 / FR-026): maps any in-scope
-- source identifier to its NCBI Gene (Entrez) anchor, per taxon. The gene is the
-- canonical collapsing identity; entrez is the anchor (R17 — Entrez covers 50,018
-- taxa vs Ensembl's 240). Gene ids anchor through GENE space via an ensg<->entrez
-- bridge (see the bridging note on the materialised view below), not by routing
-- through UniProt; uniprot -> entrez stays a direct protein->gene hop. entrez maps
-- to itself. (Organisms with no entrez fall back to ensg/genesymbol — a follow-up;
-- entrez-anchor first covers the working set.)
--
-- Columns: (ncbi_tax_id, source_type, source_id, entrez). omnipath-build reads this
-- via DuckDB ATTACH; it is a MATERIALIZED, indexed table (keyed lookups, not a scan).

-- MATERIALIZED (2026-07-02): resolver_gene is a materialized, indexed table, not a
-- view. The bridge derivation below is too expensive to run per query — as a view
-- it took ~127 s/taxon (the multiply-referenced bridge defeats taxon pushdown),
-- wrecking the per-taxon / keyed-lookup reads the build does. Materialising computes
-- it once after the FTP load and serves keyed lookups from the index, so the build
-- probes the *normalised, bridged* resolver at id_mapping speed (rather than
-- bypassing to raw id_mapping and losing the sec_ac + ENSG-bridge coverage).
-- Rebuilt on reload: a full FTP load DROPs id_mapping_ftp CASCADE (dropping this),
-- and create_resolver_views() recreates + repopulates it.
-- Drop whichever kind currently exists (plain view from the pre-materialised era,
-- or an earlier materialised view) — IF EXISTS does not cover a relkind mismatch.
DO $$
DECLARE k "char";
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'omnipath_utils' AND c.relname = 'resolver_gene';
  IF k = 'v' THEN EXECUTE 'DROP VIEW omnipath_utils.resolver_gene CASCADE';
  ELSIF k = 'm' THEN EXECUTE 'DROP MATERIALIZED VIEW omnipath_utils.resolver_gene CASCADE';
  END IF;
END $$;
CREATE MATERIALIZED VIEW omnipath_utils.resolver_gene AS
--
-- Bridging strategy (2026-07-02): gene identifiers anchor through **gene space**,
-- not protein space. Routing a gene id through UniProt (genesymbol/ensg/ensp ->
-- uniprot -> entrez) is gene->protein->gene and silently drops any gene copy
-- whose specific UniProt entry lacks the Entrez (GeneID) cross-reference — it
-- requires ONE UniProt row to carry both the source id and the Entrez. Instead we
-- build an ensg<->entrez bridge over *any* shared UniProt, then map ensp/genesymbol
-- to their ensg and bridge to entrez (protein->gene->gene, never gene->protein->gene).
-- Measured on utils2 (human): the ENSP path jumps 44,892 -> 179,772 (~4x, +134,880).
-- uniprot->entrez stays direct (UniProt *is* a protein, so protein->gene is fine).
-- The true gene-space source (NCBI gene2ensembl / gene_info, independent of UniProt)
-- is a follow-up that would also recover genes with no UniProt entry at all.
WITH up_entrez AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS uniprot, m.target_id AS entrez
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
),
up_ensg AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS uniprot, m.target_id AS ensg
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'ensg'
),
up_ensp AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS uniprot, m.target_id AS ensp
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'ensp'
),
up_symbol AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS uniprot, m.target_id AS genesymbol
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'genesymbol'
),
-- secondary -> primary UniProt AC (organism-agnostic, tax 0; ADR 0006). Lets a
-- resource that supplies a secondary accession still anchor to the gene.
sec_pri AS (
    SELECT m.source_id AS sec, m.target_id AS pri
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot-sec'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'uniprot-pri'
),
-- gene-space bridge: ensg -> entrez over ANY shared UniProt (the coverage win).
ensg_entrez AS (
    SELECT DISTINCT g.ncbi_tax_id, g.ensg, e.entrez
    FROM up_ensg g
    JOIN up_entrez e ON e.ncbi_tax_id = g.ncbi_tax_id AND e.uniprot = g.uniprot
)
-- entrez -> entrez (identity). Aliases here name the view columns for all UNION
-- branches (ncbi_tax_id, source_type, source_id, entrez).
SELECT DISTINCT ncbi_tax_id, 'entrez' AS source_type, entrez AS source_id, entrez
FROM up_entrez
UNION
-- ensg -> entrez (gene space)
SELECT DISTINCT ncbi_tax_id, 'ensg', ensg, entrez FROM ensg_entrez
UNION
-- ensp -> ensg -> entrez (protein -> gene -> gene; NOT via uniprot)
SELECT DISTINCT p.ncbi_tax_id, 'ensp', p.ensp, ge.entrez
FROM up_ensp p
JOIN up_ensg g  ON g.ncbi_tax_id = p.ncbi_tax_id AND g.uniprot = p.uniprot
JOIN ensg_entrez ge ON ge.ncbi_tax_id = p.ncbi_tax_id AND ge.ensg = g.ensg
UNION
-- genesymbol -> ensg -> entrez (prefer gene space over symbol->uniprot->entrez)
SELECT DISTINCT s.ncbi_tax_id, 'genesymbol', s.genesymbol, ge.entrez
FROM up_symbol s
JOIN up_ensg g  ON g.ncbi_tax_id = s.ncbi_tax_id AND g.uniprot = s.uniprot
JOIN ensg_entrez ge ON ge.ncbi_tax_id = s.ncbi_tax_id AND ge.ensg = g.ensg
UNION
-- uniprot -> entrez (the accession itself; uniprot IS a protein, direct is fine),
-- plus uniprot -> ensg -> entrez to recover entries with an ensg but no GeneID.
SELECT DISTINCT ncbi_tax_id, 'uniprot', uniprot, entrez FROM up_entrez
UNION
SELECT DISTINCT g.ncbi_tax_id, 'uniprot', g.uniprot, ge.entrez
FROM up_ensg g
JOIN ensg_entrez ge ON ge.ncbi_tax_id = g.ncbi_tax_id AND ge.ensg = g.ensg
UNION
-- secondary uniprot AC -> primary -> entrez (direct + via the ensg bridge).
SELECT DISTINCT e.ncbi_tax_id, 'uniprot', sp.sec, e.entrez
FROM up_entrez e
JOIN sec_pri sp ON sp.pri = e.uniprot
WHERE sp.sec IS NOT NULL
UNION
SELECT DISTINCT g.ncbi_tax_id, 'uniprot', sp.sec, ge.entrez
FROM up_ensg g
JOIN sec_pri sp ON sp.pri = g.uniprot
JOIN ensg_entrez ge ON ge.ncbi_tax_id = g.ncbi_tax_id AND ge.ensg = g.ensg
WHERE sp.sec IS NOT NULL
UNION
-- genesymbol -> entrez DIRECT via a single shared uniprot (supplement to the ensg
-- path): recovers symbols whose UniProt carries a GeneID but no ensg, which the
-- ensg-only route drops (~660 human). Preferred path is still gene-space (ensg);
-- this only ADDS, never removes. True symbol<->entrez needs NCBI gene_info (follow-up).
SELECT DISTINCT s.ncbi_tax_id, 'genesymbol', s.genesymbol, e.entrez
FROM up_symbol s
JOIN up_entrez e ON e.ncbi_tax_id = s.ncbi_tax_id AND e.uniprot = s.uniprot;

-- Keyed-lookup index: the build probes by (taxon, source_type, source_id); this
-- makes each shard's needed ids index scans on the materialised table.
CREATE INDEX IF NOT EXISTS resolver_gene_key_idx
    ON omnipath_utils.resolver_gene (ncbi_tax_id, source_type, source_id);
-- Reverse probe (by gene) for entrez-anchored lookups.
CREATE INDEX IF NOT EXISTS resolver_gene_entrez_idx
    ON omnipath_utils.resolver_gene (ncbi_tax_id, entrez);


-- Global (taxon-agnostic) UniProt/Entrez gene anchor (spec 002, T069 / R25 / US7).
--
-- A UniProt AC (and an Entrez id) uniquely determines its organism + gene, so this
-- slice carries NO taxon filter: it lets omnipath-build gene-anchor proteins that
-- arrive with no (or a mismatched) taxonomy — e.g. Rhea / Brenda / TCDB / ChEMBL
-- enzyme & target participants, which reference UniProt without an organism. The
-- resolved entity then inherits the organism *derived from the AC* (the row's
-- ncbi_tax_id) via the build's coalesce(rl.taxonomy_id, ee.taxonomy_id). Only
-- uniprot/entrez source types belong here; genesymbol/ensg/ensp stay per-taxon in
-- resolver_gene because a symbol repeats across organisms. The build's
-- taxonomy-optional-unambiguous-key gate keeps an AC that maps to >1 gene from
-- force-merging (it falls to the ambiguous lookup / multi-gene split instead).
--
-- Scale: ~24M rows over ~50k taxa (the full id_mapping_ftp uniprot->entrez map);
-- omnipath-build's needed_resolver_lookup filters this to the ACs actually
-- referenced, so resolution stays cheap. Read by omnipath-build via DuckDB ATTACH.
CREATE OR REPLACE VIEW omnipath_utils.resolver_gene_protein_global AS
WITH up_entrez AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS uniprot, m.target_id AS entrez
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
)
-- uniprot -> entrez (the accession itself as a source id)
SELECT DISTINCT ncbi_tax_id, 'uniprot' AS source_type, uniprot AS source_id, entrez
FROM up_entrez
WHERE uniprot IS NOT NULL AND entrez IS NOT NULL
UNION
-- entrez -> entrez (identity)
SELECT DISTINCT ncbi_tax_id, 'entrez' AS source_type, entrez AS source_id, entrez
FROM up_entrez
WHERE entrez IS NOT NULL;
