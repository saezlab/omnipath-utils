"""Database build presets for common deployment scenarios."""

from __future__ import annotations

# Organism groups
HUMAN = [9606]
CORE = [9606, 10090]  # human + mouse
MODEL_ORGANISMS = [9606, 10090, 10116, 7955, 7227, 6239, 4932, 8364]
# human, mouse, rat, zebrafish, fruit fly, C. elegans, yeast, frog

# Mapping pair definitions
# Each tuple: (source_type, target_type, backend)

PROTEIN_CORE = [
    ("genesymbol", "uniprot", "uniprot"),
    ("entrez", "uniprot", "uniprot"),
    ("hgnc", "uniprot", "uniprot"),
    ("refseqp", "uniprot", "uniprot"),
    # SwissProt/TrEMBL for cleanup pipeline
    ("genesymbol", "swissprot", "uniprot"),
    ("trembl", "genesymbol", "uniprot"),
    # Synonyms
    ("genesymbol-syn", "uniprot", "uniprot"),
]

ENSEMBL = [
    ("ensg", "genesymbol", "biomart"),
    ("ensp", "ensg", "biomart"),
    ("enst", "ensg", "biomart"),
    ("ensp", "uniprot", "biomart"),
]

METABOLITE = [
    # These are not organism-specific -- built once with ncbi_tax_id=0
    # UniChem, RaMP, HMDB handled separately
]

MIRNA = [
    # miRBase -- organism-specific
    # Handled by mirbase backend, not standard mapping pairs
]

# Preset definitions
PRESETS = {
    "minimal": {
        "description": "Basic human protein ID mappings",
        "organisms": HUMAN,
        "mappings": PROTEIN_CORE + ENSEMBL,
        "metabolite": False,
        "mirna": False,
        "orthology": False,
        "reflists": True,
    },
    "standard": {
        "description": "Human + mouse protein mappings with metabolites",
        "organisms": CORE,
        "mappings": PROTEIN_CORE + ENSEMBL,
        "metabolite": True,
        "mirna": False,
        "orthology": True,
        "reflists": True,
    },
    "model": {
        "description": "All model organisms + metabolites + miRNA + orthology",
        "organisms": MODEL_ORGANISMS,
        "mappings": PROTEIN_CORE + ENSEMBL,
        "metabolite": True,
        "mirna": True,
        "orthology": True,
        "reflists": True,
    },
    "full": {
        "description": "All organisms from UniProt FTP + everything",
        "organisms": None,  # None means use FTP for all
        "mappings": PROTEIN_CORE + ENSEMBL,
        "metabolite": True,
        "mirna": True,
        "orthology": True,
        "reflists": True,
        "ftp": True,
    },
}

# Common Parquet export tables (source_type, target_type, ncbi_tax_id)
# These are the most frequently requested via the API
PARQUET_TABLES = {
    "minimal": [
        ("genesymbol", "uniprot", 9606),
        ("entrez", "uniprot", 9606),
        ("uniprot", "genesymbol", 9606),
    ],
    "standard": [
        ("genesymbol", "uniprot", 9606),
        ("entrez", "uniprot", 9606),
        ("uniprot", "genesymbol", 9606),
        ("genesymbol", "uniprot", 10090),
        ("entrez", "uniprot", 10090),
        ("ensg", "genesymbol", 9606),
        ("ensg", "genesymbol", 10090),
    ],
    "model": [
        ("genesymbol", "uniprot", org)
        for org in MODEL_ORGANISMS
    ] + [
        ("entrez", "uniprot", org)
        for org in MODEL_ORGANISMS
    ] + [
        ("ensg", "genesymbol", org)
        for org in MODEL_ORGANISMS
    ],
}
PARQUET_TABLES["full"] = PARQUET_TABLES["model"]
