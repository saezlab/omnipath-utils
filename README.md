[![Tests](https://github.com/saezlab/omnipath-utils/actions/workflows/ci-testing-unit.yml/badge.svg)](https://github.com/saezlab/omnipath-utils/actions)

# Utilities for molecular prior-knowledge processing

ID translation, taxonomy, reference lists, orthologous genes, and more. Also
available as a web service: https://utils.omnipathdb.org/

## Quick Start

```python
from omnipath_utils.mapping import map_name, map_names
from omnipath_utils.taxonomy import ensure_ncbi_tax_id
from omnipath_utils.reflists import is_swissprot

# Translate gene symbol to UniProt
map_name('TP53', 'genesymbol', 'uniprot')
# {'P04637', ...}

# Translate multiple
map_names(['TP53', 'EGFR'], 'genesymbol', 'uniprot')
# {'P04637', 'P00533', ...}

# Resolve organism
ensure_ncbi_tax_id('human')   # 9606
ensure_ncbi_tax_id('hsapiens') # 9606

# Check if reviewed
is_swissprot('P04637')  # True
```

## Installation

```bash
pip install omnipath-utils
```

With database and web service:

```bash
pip install "omnipath-utils[server]"
```

## Web Service

```bash
omnipath-utils build --organisms 9606
omnipath-utils serve --port 8082
```

```bash
curl "http://localhost:8082/mapping/translate?identifiers=TP53,EGFR&id_type=genesymbol&target_id_type=uniprot"
```

## Documentation

https://saezlab.github.io/omnipath-utils

## License

The package is under the GNU GPLv3 license. This doesn't affect the web service
and data, where each original resource carries its own license, and is
potentially available for commercial use.
