"""Ensembl BioMart mapping backend."""

from __future__ import annotations

import logging
import textwrap
from collections import defaultdict

import requests

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend
from omnipath_utils.mapping._id_types import IdTypeRegistry
from omnipath_utils.taxonomy import ensure_ensembl_name

_log = logging.getLogger(__name__)

BIOMART_URL = "https://www.ensembl.org/biomart/martservice"

XML_TEMPLATE = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE Query>
    <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" datasetConfigVersion="0.6">
      <Dataset name="{dataset}" interface="default">
        {attributes}
      </Dataset>
    </Query>""")

ATTR_TEMPLATE = "<Attribute name=\"{name}\" />"


class BioMartBackend(MappingBackend):
    """Fetch ID mappings from Ensembl BioMart."""

    def read(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        reg = IdTypeRegistry.get()

        src_attr = reg.backend_column(id_type, "ensembl")
        tgt_attr = reg.backend_column(target_id_type, "ensembl")

        if not src_attr or not tgt_attr:
            _log.debug(
                "BioMart does not support %s -> %s",
                id_type,
                target_id_type,
            )
            return {}

        ensembl_name = ensure_ensembl_name(ncbi_tax_id)

        if not ensembl_name:
            _log.warning("No Ensembl name for organism %d", ncbi_tax_id)
            return {}

        dataset = f"{ensembl_name}_gene_ensembl"

        attrs = (
            [src_attr, tgt_attr]
            if src_attr != tgt_attr
            else [src_attr]
        )
        attr_xml = "\n        ".join(
            ATTR_TEMPLATE.format(name=a) for a in attrs
        )

        xml = XML_TEMPLATE.format(dataset=dataset, attributes=attr_xml)
        xml = xml.replace("\n", "").replace("  ", "")  # compact

        _log.info(
            "Querying BioMart: %s -> %s (dataset %s)",
            id_type,
            target_id_type,
            dataset,
        )

        resp = requests.get(
            BIOMART_URL,
            params={"query": xml},
            timeout=120,
        )
        resp.raise_for_status()

        data: dict[str, set[str]] = defaultdict(set)
        lines = resp.text.strip().split("\n")

        for line in lines[1:]:  # skip header
            parts = line.split("\t")

            if len(parts) >= 2:
                src = parts[0].strip()
                tgt = parts[1].strip()

                if src and tgt:
                    data[src].add(tgt)

        _log.info(
            "BioMart: loaded %d entries for %s -> %s",
            len(data),
            id_type,
            target_id_type,
        )

        return dict(data)


register("biomart", BioMartBackend)
