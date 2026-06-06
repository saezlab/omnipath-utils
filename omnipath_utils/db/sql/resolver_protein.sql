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
