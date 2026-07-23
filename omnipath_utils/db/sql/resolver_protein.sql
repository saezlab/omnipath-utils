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
-- uniprot accession identity (so a primary uniprot evidence id canonicalises)
SELECT DISTINCT m.ncbi_tax_id, 'uniprot', m.source_id, m.source_id
FROM omnipath_utils.id_mapping_ftp m
JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
WHERE m.source_id IS NOT NULL
UNION ALL
-- uniprot entry name -> accession
SELECT m.ncbi_tax_id, 'uniprot_entry', m.target_id, m.source_id
FROM omnipath_utils.id_mapping_ftp m
JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot'
JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'uniprot_entry';

-- MATERIALIZED (2026-07-02): like resolver_gene/resolver_chemical, the build
-- full-COPYs this per-taxon source->UniProt projection; as a plain view it
-- re-expands the id_mapping_ftp inversion + SwissProt-preference window on every
-- read (a stall alongside the chemical one). Materialise + index on the keyed-
-- lookup columns. resolver_protein_source stays a view (only consumed here).
DO $$
DECLARE k "char";
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'omnipath_utils' AND c.relname = 'resolver_protein';
  IF k = 'v' THEN EXECUTE 'DROP VIEW omnipath_utils.resolver_protein CASCADE';
  ELSIF k = 'm' THEN EXECUTE 'DROP MATERIALIZED VIEW omnipath_utils.resolver_protein CASCADE';
  END IF;
END $$;
CREATE MATERIALIZED VIEW omnipath_utils.resolver_protein AS
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

-- Keyed-lookup index: the build probes by (ncbi_tax_id, source_type, source_id).
CREATE INDEX IF NOT EXISTS resolver_protein_key_idx
    ON omnipath_utils.resolver_protein (ncbi_tax_id, source_type, source_id);
-- The build's per-shard keyed join filters (source_type, source_id) but NOT
-- ncbi_tax_id (a source id may arrive with no/any taxon -> an OR on taxon), so the
-- ncbi_tax_id-leading index above is unusable for it and Postgres seq-scans the
-- 300M rows (~64 s/shard). This (source_type, source_id) covering index makes it an
-- index-only probe (taxon becomes a cheap post-filter). Covers uniprot for the payload.
CREATE INDEX IF NOT EXISTS resolver_protein_st_si_idx
    ON omnipath_utils.resolver_protein (source_type, source_id)
    INCLUDE (ncbi_tax_id, uniprot);


-- ===== resolver_gene: curated delta + FTP core, unioned =====
-- Gene-anchor projection (spec 002, M-Genes / US7 / FR-026): maps any in-scope
-- source identifier to its NCBI Gene (Entrez) anchor, per taxon. The gene is the
-- canonical collapsing identity; entrez is the anchor (R17 — Entrez covers 50,018
-- taxa vs Ensembl's 240). entrez maps to itself.
--
-- Columns: (ncbi_tax_id, source_type, source_id, entrez). omnipath-build reads
-- resolver_gene via DuckDB ATTACH; web/API read it too. The name and columns are
-- UNCHANGED — resolver_gene is now a VIEW = resolver_gene_ftp UNION ALL
-- resolver_gene_curated (spec 007 R10 / Phase 3P, T064):
--   * resolver_gene_ftp     — the id_mapping_ftp-derived branches (expensive;
--     rebuilt only on an FTP reload — see resolver_gene_ftp.sql).
--   * resolver_gene_curated — the curated-id_mapping branches incl. all the 007
--     US1 anchors (gene_info / gene2accession / KEGG / Ensembl Genomes / NCBI
--     gene2ensembl / Ensembl BioMart). Cheap; rebuilt on EVERY additive load so a
--     newly loaded curated mapping is picked up without re-deriving the FTP core.
--
-- UNION ALL (not UNION): so the resolver_gene VIEW flattens to an appendrel and the
-- build's keyed join push-probes each child's (source_type, source_id) index,
-- instead of materialising + deduping the ~83M-row union on every read (a UNION
-- set-op is a join-pushdown barrier). Rows that both children emit therefore appear
-- twice in the view; that is inert — every consumer collapses with SELECT DISTINCT
-- / count(DISTINCT) / row_number (omnipath-build resolver_lookup gates, and the
-- combined resolver below), so a duplicate never changes a resolution or an
-- ambiguity count.

CREATE TABLE omnipath_utils.resolver_gene_curated AS
-- secondary -> primary UniProt AC (organism-agnostic, tax 0; ADR 0006). Lets a
-- resource that supplies a secondary accession still anchor to the gene.
WITH sec_pri AS (
    SELECT m.source_id AS sec, m.target_id AS pri
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'uniprot-sec'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'uniprot-pri'
),
-- AUTHORITATIVE gene space (curated id_mapping): NCBI gene2ensembl gives ensp->entrez
-- and ensg->entrez DIRECT (all transcripts, 772 taxa); Ensembl BioMart gives
-- ensp->ensg and ensg->genesymbol. These are the primary paths.
g2e_ensp AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS ensp, m.target_id AS entrez
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensp'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
),
g2e_ensg AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS ensg, m.target_id AS entrez
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensg'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
),
bm_ensp_ensg AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS ensp, m.target_id AS ensg
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensp'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'ensg'
),
bm_symbol_ensg AS (
    SELECT DISTINCT m.ncbi_tax_id, m.target_id AS genesymbol, m.source_id AS ensg
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensg'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'genesymbol'
),
-- ===== 007 US1: authoritative NCBI gene-space + KEGG anchors =====
-- gene_info: genesymbol / synonym -> entrez DIRECT, all organisms, independent of
-- UniProt (covers genes with no protein product). Both primary symbols and synonyms
-- are emitted under source_type 'genesymbol' so a symbol lookup that hits a synonym
-- still resolves; ambiguous synonyms are dropped by the combined resolver's gate.
gi_symbol AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS genesymbol, m.target_id AS entrez
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id
     AND st.name IN ('genesymbol', 'genesymbol-syn')
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
),
-- gene2accession: RefSeq RNA/protein -> entrez DIRECT.
g2a_refseq AS (
    SELECT DISTINCT m.ncbi_tax_id, st.name AS source_type,
           m.source_id AS refseq, m.target_id AS entrez
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id
     AND st.name IN ('refseqn', 'refseqp')
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
),
-- KEGG gene id -> entrez.
kg_entrez AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS kegg_gene, m.target_id AS entrez
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'kegg_gene'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'entrez'
),
-- Ensembl Genomes divisions (BioMart genomes): gene -> genesymbol, and
-- protein/transcript -> gene. Anchored to entrez via the gi_symbol bridge below.
egg_symbol AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS ensgg, m.target_id AS genesymbol
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensgg'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'genesymbol'
),
egg_prot AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS ensgp, m.target_id AS ensgg
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensgp'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'ensgg'
),
egg_tx AS (
    SELECT DISTINCT m.ncbi_tax_id, m.source_id AS ensgt, m.target_id AS ensgg
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st ON m.source_type_id = st.id AND st.name = 'ensgt'
    JOIN omnipath_utils.id_type tt ON m.target_type_id = tt.id AND tt.name = 'ensgg'
)
-- Column names come from the first branch: (ncbi_tax_id, source_type, source_id, entrez).
-- ===== AUTHORITATIVE gene-space paths (NCBI gene2ensembl + Ensembl BioMart) =====
-- ensp -> entrez DIRECT (gene2ensembl, every transcript, 772 taxa).
SELECT DISTINCT ncbi_tax_id, 'ensp'::varchar(64) AS source_type,
       ensp AS source_id, entrez
FROM g2e_ensp
UNION
-- ensp -> ensg (BioMart) -> entrez (gene2ensembl): catches ENSPs not directly in g2e.
SELECT DISTINCT b.ncbi_tax_id, 'ensp', b.ensp, ge.entrez
FROM bm_ensp_ensg b
JOIN g2e_ensg ge ON ge.ncbi_tax_id = b.ncbi_tax_id AND ge.ensg = b.ensg
UNION
-- ensg -> entrez DIRECT (gene2ensembl).
SELECT DISTINCT ncbi_tax_id, 'ensg', ensg, entrez FROM g2e_ensg
UNION
-- genesymbol -> ensg (BioMart) -> entrez (gene2ensembl): authoritative symbol path.
SELECT DISTINCT s.ncbi_tax_id, 'genesymbol', s.genesymbol, ge.entrez
FROM bm_symbol_ensg s
JOIN g2e_ensg ge ON ge.ncbi_tax_id = s.ncbi_tax_id AND ge.ensg = s.ensg
UNION
-- ===== 007 US1: DIRECT NCBI gene-space + KEGG anchors =====
-- genesymbol (and synonyms) -> entrez DIRECT (gene_info).
SELECT DISTINCT ncbi_tax_id, 'genesymbol', genesymbol, entrez FROM gi_symbol
UNION
-- refseqn / refseqp -> entrez DIRECT (gene2accession).
SELECT DISTINCT ncbi_tax_id, source_type, refseq, entrez FROM g2a_refseq
UNION
-- kegg_gene -> entrez DIRECT (kegg_gene).
SELECT DISTINCT ncbi_tax_id, 'kegg_gene', kegg_gene, entrez FROM kg_entrez
UNION
-- Ensembl Genomes gene -> genesymbol -> entrez (division bridge via gene_info).
SELECT DISTINCT e.ncbi_tax_id, 'ensgg', e.ensgg, g.entrez
FROM egg_symbol e
JOIN gi_symbol g ON g.ncbi_tax_id = e.ncbi_tax_id AND g.genesymbol = e.genesymbol
UNION
-- Ensembl Genomes protein -> gene -> genesymbol -> entrez.
SELECT DISTINCT p.ncbi_tax_id, 'ensgp', p.ensgp, g.entrez
FROM egg_prot p
JOIN egg_symbol e ON e.ncbi_tax_id = p.ncbi_tax_id AND e.ensgg = p.ensgg
JOIN gi_symbol g ON g.ncbi_tax_id = p.ncbi_tax_id AND g.genesymbol = e.genesymbol
UNION
-- Ensembl Genomes transcript -> gene -> genesymbol -> entrez.
SELECT DISTINCT t.ncbi_tax_id, 'ensgt', t.ensgt, g.entrez
FROM egg_tx t
JOIN egg_symbol e ON e.ncbi_tax_id = t.ncbi_tax_id AND e.ensgg = t.ensgg
JOIN gi_symbol g ON g.ncbi_tax_id = t.ncbi_tax_id AND g.genesymbol = e.genesymbol
UNION
-- ===== secondary UniProt AC -> primary -> entrez (curated sec_pri x FTP core) =====
-- Reformulation of the two old up_entrez/up_ensg secondary-AC branches: the FTP
-- core already emits every ('uniprot', primary_ac -> entrez) row (direct AND via
-- the ensg bridge), so a secondary AC anchors by joining resolver_gene_ftp on its
-- primary, WITHOUT re-scanning id_mapping_ftp. Provably equal to the old
-- (up_entrez JOIN sec_pri) UNION (up_ensg JOIN sec_pri JOIN ensg_entrez).
SELECT DISTINCT rgf.ncbi_tax_id, 'uniprot', sp.sec, rgf.entrez
FROM sec_pri sp
JOIN omnipath_utils.resolver_gene_ftp rgf
  ON rgf.source_type = 'uniprot' AND rgf.source_id = sp.pri
WHERE sp.sec IS NOT NULL;

-- Keyed-lookup indexes (mirror resolver_gene_ftp so the UNION-ALL view probes both
-- children the same way).
CREATE INDEX resolver_gene_curated_st_si_idx
    ON omnipath_utils.resolver_gene_curated (source_type, source_id)
    INCLUDE (ncbi_tax_id, entrez);
CREATE INDEX resolver_gene_curated_key_idx
    ON omnipath_utils.resolver_gene_curated (ncbi_tax_id, source_type, source_id);
ANALYZE omnipath_utils.resolver_gene_curated;

-- resolver_gene: the unchanged name + columns consumers read, now a UNION-ALL view
-- over the FTP core + the curated delta. Drop whatever kind currently exists (an
-- earlier materialised view / table, or this view) before recreating.
DO $$
DECLARE k "char";
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'omnipath_utils' AND c.relname = 'resolver_gene';
  IF k = 'v' THEN EXECUTE 'DROP VIEW omnipath_utils.resolver_gene CASCADE';
  ELSIF k = 'm' THEN EXECUTE 'DROP MATERIALIZED VIEW omnipath_utils.resolver_gene CASCADE';
  ELSIF k = 'r' THEN EXECUTE 'DROP TABLE omnipath_utils.resolver_gene CASCADE';
  END IF;
END $$;
CREATE VIEW omnipath_utils.resolver_gene AS
SELECT ncbi_tax_id, source_type, source_id, entrez FROM omnipath_utils.resolver_gene_ftp
UNION ALL
SELECT ncbi_tax_id, source_type, source_id, entrez FROM omnipath_utils.resolver_gene_curated;


-- Combined gene/protein resolver. It emits one canonical target per
-- (taxon, source_type, source_id): Entrez if uniquely derivable, otherwise the
-- primary UniProt if uniquely derivable. Ambiguous keys are omitted and remain
-- unresolved in consumers.
DO $$
DECLARE k "char";
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'omnipath_utils'
     AND c.relname = 'resolver_gene_protein_combined';
  IF k = 'v' THEN
    EXECUTE 'DROP VIEW omnipath_utils.resolver_gene_protein_combined CASCADE';
  ELSIF k = 'm' THEN
    EXECUTE 'DROP MATERIALIZED VIEW omnipath_utils.resolver_gene_protein_combined CASCADE';
  END IF;
END $$;
CREATE MATERIALIZED VIEW omnipath_utils.resolver_gene_protein_combined AS
WITH sec_pri AS (
    SELECT m.source_id AS sec, m.target_id AS pri
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st
      ON m.source_type_id = st.id AND st.name = 'uniprot-sec'
    JOIN omnipath_utils.id_type tt
      ON m.target_type_id = tt.id AND tt.name = 'uniprot-pri'
),
protein_source_raw AS (
    -- full-UniProt: native uniprot -> X, inverted to X -> uniprot
    SELECT
      m.ncbi_tax_id,
      tt.name AS source_type,
      m.target_id AS source_id,
      m.source_id AS ac
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st
      ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt
      ON m.target_type_id = tt.id
     AND tt.name IN (
       'genesymbol', 'ensg', 'ensp', 'entrez', 'refseqp', 'uniprot_entry'
     )
    UNION ALL
    -- curated, native uniprot -> X (inverted)
    SELECT
      m.ncbi_tax_id,
      tt.name AS source_type,
      m.target_id AS source_id,
      m.source_id AS ac
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st
      ON m.source_type_id = st.id AND st.name = 'uniprot'
    JOIN omnipath_utils.id_type tt
      ON m.target_type_id = tt.id
     AND tt.name IN ('genesymbol', 'ensg', 'ensp', 'entrez', 'refseqp')
    UNION ALL
    -- curated, forward X -> uniprot
    SELECT
      m.ncbi_tax_id,
      st.name AS source_type,
      m.source_id AS source_id,
      m.target_id AS ac
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st
      ON m.source_type_id = st.id
     AND st.name IN ('genesymbol', 'ensg', 'ensp', 'entrez', 'refseqp')
    JOIN omnipath_utils.id_type tt
      ON m.target_type_id = tt.id AND tt.name = 'uniprot'
    UNION ALL
    -- primary UniProt accession identity
    SELECT DISTINCT
      m.ncbi_tax_id,
      'uniprot' AS source_type,
      m.source_id AS source_id,
      m.source_id AS ac
    FROM omnipath_utils.id_mapping_ftp m
    JOIN omnipath_utils.id_type st
      ON m.source_type_id = st.id AND st.name = 'uniprot'
    WHERE m.source_id IS NOT NULL
    UNION ALL
    -- secondary UniProt accession -> primary UniProt accession
    SELECT
      NULLIF(m.ncbi_tax_id, 0) AS ncbi_tax_id,
      'uniprot' AS source_type,
      m.source_id AS source_id,
      m.target_id AS ac
    FROM omnipath_utils.id_mapping m
    JOIN omnipath_utils.id_type st
      ON m.source_type_id = st.id AND st.name = 'uniprot-sec'
    JOIN omnipath_utils.id_type tt
      ON m.target_type_id = tt.id AND tt.name = 'uniprot-pri'
    WHERE m.source_id IS NOT NULL
      AND m.target_id IS NOT NULL
),
protein_key_normalized AS (
    SELECT
      r.ncbi_tax_id,
      r.source_type,
      r.source_id,
      COALESCE(sp.pri, r.ac) AS primary_uniprot
    FROM protein_source_raw r
    LEFT JOIN sec_pri sp ON sp.sec = r.ac
    WHERE r.source_id IS NOT NULL
      AND r.ac IS NOT NULL
),
protein_key_flagged AS (
    SELECT
      pk.ncbi_tax_id,
      pk.source_type,
      pk.source_id,
      pk.primary_uniprot,
      (r.identifier IS NOT NULL) AS is_swissprot,
      bool_or(r.identifier IS NOT NULL) OVER (
        PARTITION BY pk.ncbi_tax_id, pk.source_type, pk.source_id
      ) AS key_has_swissprot
    FROM protein_key_normalized pk
    LEFT JOIN omnipath_utils.reflist r
      ON r.list_name = 'swissprot'
     AND r.identifier = pk.primary_uniprot
),
protein_key AS (
    SELECT DISTINCT
      ncbi_tax_id,
      source_type,
      source_id,
      primary_uniprot
    FROM protein_key_flagged
    WHERE ((key_has_swissprot AND is_swissprot) OR NOT key_has_swissprot)
      AND primary_uniprot ~ '^([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9]([A-Z][A-Z0-9]{2}[0-9]){1,2})$'
),
entrez_candidate AS (
    SELECT
      rg.ncbi_tax_id,
      rg.source_type,
      rg.source_id,
      rg.entrez
    FROM omnipath_utils.resolver_gene rg
    WHERE rg.source_id IS NOT NULL
      AND rg.entrez IS NOT NULL
    UNION
    -- If a source key has both a gene anchor and a primary UniProt, the primary
    -- UniProt should resolve to the same gene (e.g. Cngb1 -> A0A8I5ZN27 and
    -- Cngb1 -> Entrez).
    SELECT
      pk.ncbi_tax_id,
      'uniprot' AS source_type,
      pk.primary_uniprot AS source_id,
      rg.entrez
    FROM protein_key pk
    JOIN omnipath_utils.resolver_gene rg
      ON rg.ncbi_tax_id = pk.ncbi_tax_id
     AND rg.source_type = pk.source_type
     AND rg.source_id = pk.source_id
    WHERE pk.primary_uniprot IS NOT NULL
      AND rg.entrez IS NOT NULL
    UNION
    -- Secondary accessions inherit the primary accession's gene anchor.
    SELECT
      rg.ncbi_tax_id,
      'uniprot' AS source_type,
      sp.sec AS source_id,
      rg.entrez
    FROM sec_pri sp
    JOIN omnipath_utils.resolver_gene rg
      ON rg.source_type = 'uniprot'
     AND rg.source_id = sp.pri
    WHERE sp.sec IS NOT NULL
),
entrez_unique AS (
    SELECT
      ncbi_tax_id,
      source_type,
      source_id,
      min(entrez) AS canonical_id
    FROM entrez_candidate
    WHERE source_id IS NOT NULL
      AND entrez IS NOT NULL
    GROUP BY ncbi_tax_id, source_type, source_id
    HAVING count(DISTINCT entrez) = 1
),
uniprot_unique AS (
    SELECT
      pk.ncbi_tax_id,
      pk.source_type,
      pk.source_id,
      min(pk.primary_uniprot) AS canonical_id
    FROM protein_key pk
    LEFT JOIN entrez_unique eu
      ON eu.ncbi_tax_id IS NOT DISTINCT FROM pk.ncbi_tax_id
     AND eu.source_type = pk.source_type
     AND eu.source_id = pk.source_id
    WHERE eu.source_id IS NULL
      AND pk.source_id IS NOT NULL
      AND pk.primary_uniprot IS NOT NULL
    GROUP BY pk.ncbi_tax_id, pk.source_type, pk.source_id
    HAVING count(DISTINCT pk.primary_uniprot) = 1
)
SELECT
  ncbi_tax_id,
  source_type,
  source_id,
  'entrez' AS canonical_type,
  canonical_id
FROM entrez_unique
UNION ALL
SELECT
  ncbi_tax_id,
  source_type,
  source_id,
  'uniprot' AS canonical_type,
  canonical_id
FROM uniprot_unique;

CREATE INDEX IF NOT EXISTS resolver_gene_protein_combined_key_idx
    ON omnipath_utils.resolver_gene_protein_combined
    (ncbi_tax_id, source_type, source_id);
CREATE INDEX IF NOT EXISTS resolver_gene_protein_combined_target_idx
    ON omnipath_utils.resolver_gene_protein_combined
    (canonical_type, canonical_id, ncbi_tax_id);


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
-- MATERIALIZED + indexed (2026-07-04): the build's keyed lookup probes this for
-- proteins that arrive with NO taxon (Rhea/Brenda/TCDB/ChEMBL reference UniProt
-- without an organism). As a plain view a keyed join seq-scans the 37M-row
-- derivation; materialise + a (source_type, source_id) index so it is an index
-- probe. The build emits its matches taxon-agnostically (taxonomy NULL) so the
-- no-taxon evidence matches — the taxon-bearing resolver_gene rows cannot.
DO $$
DECLARE k "char";
BEGIN
  SELECT c.relkind INTO k FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'omnipath_utils' AND c.relname = 'resolver_gene_protein_global';
  IF k = 'v' THEN EXECUTE 'DROP VIEW omnipath_utils.resolver_gene_protein_global CASCADE';
  ELSIF k = 'm' THEN EXECUTE 'DROP MATERIALIZED VIEW omnipath_utils.resolver_gene_protein_global CASCADE';
  END IF;
END $$;
CREATE MATERIALIZED VIEW omnipath_utils.resolver_gene_protein_global AS
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

CREATE INDEX IF NOT EXISTS resolver_gene_protein_global_st_si_idx
    ON omnipath_utils.resolver_gene_protein_global (source_type, source_id)
    INCLUDE (entrez);
