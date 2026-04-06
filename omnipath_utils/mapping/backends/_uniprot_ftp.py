"""UniProt FTP bulk ID mapping backend."""

from __future__ import annotations

import logging
from collections import defaultdict

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend

_log = logging.getLogger(__name__)

# Map FTP file ID type names to our canonical names
FTP_IDTYPE_MAP = {
    'UniProtKB-ID': 'uniprot_entry',
    'Gene_Name': 'genesymbol',
    'GeneID': 'entrez',
    'RefSeq': 'refseqp',
    'RefSeq_NT': 'refseqn',
    'GI': 'gi',
    'PDB': 'pdb',
    'EMBL-CDS': 'embl',
    'EMBL': 'embl_id',
    'Ensembl': 'ensg',
    'Ensembl_TRS': 'enst',
    'Ensembl_PRO': 'ensp',
    'HGNC': 'hgnc',
    'KEGG': 'kegg',
    'ChEMBL': 'chembl',
    'DrugBank': 'drugbank',
}

# Reverse: our name -> FTP file name
CANONICAL_TO_FTP = {v: k for k, v in FTP_IDTYPE_MAP.items()}


class UniProtFTPBackend(MappingBackend):
    """Bulk ID mapping from UniProt FTP idmapping files.

    Downloads per-organism idmapping.dat.gz files and parses them
    to extract mapping tables. One side is always 'uniprot' (the AC).
    """

    name = 'uniprot_ftp'
    yaml_key = 'uniprot'  # uses same columns as the REST backend

    def _read_via_pypath(self, id_type, target_id_type, ncbi_tax_id, *, src_col, tgt_col, **kwargs):
        from pypath.inputs.uniprot_ftp import idmapping_stream, IDTYPE_MAP
        from pkg_infra.utils import swap_dict

        # Determine which side is the UniProt AC and which is the other ID type
        if id_type == 'uniprot' or src_col == 'accession':
            # Source is UniProt AC, target is the field
            ftp_type = self._resolve_ftp_type(target_id_type)
            if not ftp_type:
                return {}
            data = self._load_mapping(ftp_type, ncbi_tax_id)
            return data
        elif target_id_type == 'uniprot' or tgt_col == 'accession':
            # Target is UniProt AC, source is the field
            ftp_type = self._resolve_ftp_type(id_type)
            if not ftp_type:
                return {}
            data = self._load_mapping(ftp_type, ncbi_tax_id)
            return swap_dict(data, force_sets=True)
        else:
            return {}

    def _read_direct(self, id_type, target_id_type, ncbi_tax_id, *, src_col, tgt_col, **kwargs):
        # The FTP backend always needs the pypath input module for download
        # Fall back: use requests to download directly
        import gzip
        import os
        import tempfile

        import requests
        from pkg_infra.utils import swap_dict

        if id_type == 'uniprot' or src_col == 'accession':
            ftp_type = self._resolve_ftp_type(target_id_type)
            swap = False
        elif target_id_type == 'uniprot' or tgt_col == 'accession':
            ftp_type = self._resolve_ftp_type(id_type)
            swap = True
        else:
            return {}

        if not ftp_type:
            return {}

        url = self._organism_url(ncbi_tax_id)
        if not url:
            return {}

        _log.info('Downloading FTP idmapping from %s', url)
        resp = requests.get(url, timeout=600, stream=True)
        resp.raise_for_status()

        # Write to temp file and parse
        with tempfile.NamedTemporaryFile(suffix='.dat.gz', delete=False) as tmp:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        data = defaultdict(set)
        try:
            with gzip.open(tmp_path, 'rt') as f:
                for line in f:
                    parts = line.rstrip('\n').split('\t')
                    if len(parts) == 3 and parts[1] == ftp_type:
                        data[parts[0]].add(parts[2])
        finally:
            os.unlink(tmp_path)

        return swap_dict(dict(data), force_sets=True) if swap else dict(data)

    @staticmethod
    def _resolve_ftp_type(canonical_name: str) -> str | None:
        """Map our canonical ID type name to the FTP file's ID type name."""
        return CANONICAL_TO_FTP.get(canonical_name)

    @staticmethod
    def _organism_url(ncbi_tax_id: int) -> str | None:
        _CODES = {
            9606: 'HUMAN', 10090: 'MOUSE', 10116: 'RAT',
            559292: 'YEAST', 83333: 'ECOLI', 7227: 'DROME',
            7955: 'DANRE', 6239: 'CAEEL', 9031: 'CHICK',
            3702: 'ARATH', 44689: 'DICDI', 284812: 'SCHPO',
        }
        code = _CODES.get(ncbi_tax_id)
        if not code:
            return None
        return (
            f'https://ftp.uniprot.org/pub/databases/uniprot/'
            f'current_release/knowledgebase/idmapping/'
            f'by_organism/{code}_{ncbi_tax_id}_idmapping.dat.gz'
        )

    @staticmethod
    def _load_mapping(ftp_type: str, ncbi_tax_id: int) -> dict[str, set[str]]:
        from pypath.inputs.uniprot_ftp import idmapping
        return idmapping(ftp_type, ncbi_tax_id)


register('uniprot_ftp', UniProtFTPBackend)
