# Installation

## Basic (Python API only)

```bash
pip install omnipath-utils
```

## With database support

```bash
pip install "omnipath-utils[db]"
```

## With web service

```bash
pip install "omnipath-utils[server]"
```

## With pypath backends (recommended for building)

```bash
pip install "omnipath-utils[pypath]"
```

## Development

```bash
git clone https://github.com/saezlab/omnipath-utils
cd omnipath-utils
uv sync --all-extras
uv run pytest
```
