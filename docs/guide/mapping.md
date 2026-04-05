# ID Translation

omnipath-utils translates between 97 biological identifier types using
data from multiple upstream databases.

## Core functions

### `map_name` -- translate a single identifier

```python
from omnipath_utils.mapping import map_name

map_name('TP53', 'genesymbol', 'uniprot')
# {'P04637'}

map_name('P04637', 'uniprot', 'genesymbol')
# {'TP53'}
```

Returns a `set[str]` of target identifiers. One-to-many mappings are common
(e.g. a gene symbol may map to multiple UniProt isoforms).

### `map_names` -- translate multiple, return union

```python
from omnipath_utils.mapping import map_names

map_names(['TP53', 'EGFR'], 'genesymbol', 'uniprot')
# {'P04637', 'P00533'}
```

### `translate` -- batch translate with per-input results

```python
from omnipath_utils.mapping import translate

translate(['TP53', 'EGFR'], 'genesymbol', 'uniprot')
# {'TP53': {'P04637'}, 'EGFR': {'P00533'}}
```

### `map_name0` -- translate to a single result

```python
from omnipath_utils.mapping import map_name0

map_name0('TP53', 'genesymbol', 'uniprot')
# 'P04637'
```

Returns `str | None`. If the mapping is ambiguous, an arbitrary result is
picked.

### `translation_table` -- full mapping table

```python
from omnipath_utils.mapping import translation_table

table = translation_table('genesymbol', 'uniprot')
# dict mapping every gene symbol to its UniProt ACs
```

### `id_types` -- list all supported types

```python
from omnipath_utils.mapping import id_types

id_types()
# ['uniprot', 'genesymbol', 'ensg', 'ensp', ...]
```

## Supported ID types

97 identifier types are defined in `id_types.yaml`, covering:

| Entity type | Examples |
|------------|----------|
| protein | uniprot, swissprot, trembl, uniprot-entry |
| gene | genesymbol, entrez, hgnc, mgi, rgd |
| transcript | enst, refseqmrna |
| gene/protein | ensg, ensp, ensembl_peptide_id |
| small molecule | hmdb, pubchem, chebi, drugbank |
| mirna | mirbase, mir-mat, mir-pre |
| probe | affy, illumina, agilent |

Use `id_types()` for the full list, or inspect the
`IdTypeRegistry` for detailed metadata.

## Backends

Translation data comes from multiple upstream sources:

| Backend | ID types covered |
|---------|-----------------|
| UniProt ID mapping | UniProt, gene symbols, Entrez, HGNC, RefSeq, PDB, etc. |
| Ensembl BioMart | Ensembl gene/transcript/protein, gene symbols, Entrez |
| HMDB | HMDB, PubChem, ChEBI, DrugBank, KEGG compound |
| RaMP | Cross-database metabolite mappings |
| UniChem | Chemical identifier cross-references |
| PRO | Protein Ontology identifiers |
| miRBase | miRNA identifiers |

Backends are selected automatically based on the requested ID type pair.

## Special-case handling

### Gene symbol fallbacks

When a gene symbol is not found directly, omnipath-utils tries:

1. Case-insensitive lookup
2. Synonym table (`genesymbol-syn`)
3. Strip trailing version numbers

### Chain translation

If no direct mapping exists between two ID types, omnipath-utils chains
through UniProt as an intermediate. For example, `entrez` to `ensg` goes
via `entrez -> uniprot -> ensg`.

### UniProt cleanup

When the target type is UniProt, secondary (deprecated) accessions are
automatically resolved to their current primary accession.

### Ensembl version stripping

Ensembl IDs with version suffixes (e.g. `ENSG00000141510.18`) are
automatically stripped to the base ID before lookup.

## Organism support

ID translation defaults to human (NCBI Taxonomy ID 9606). Pass
`ncbi_tax_id` to translate for other organisms:

```python
map_name('Trp53', 'genesymbol', 'uniprot', ncbi_tax_id=10090)
```
