"""SwissLipids structure-bearing mapping backend (Milestone I) via the inputs_v2 adapter.

SwissLipids provides SMILES + a LIPID MAPS cross-reference (no InChIKey of its
own); it broadens SMILES coverage and bridges to lipidmaps' InChIKeys.
"""

from __future__ import annotations

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._inputs_v2_adapter import InputsV2Backend


class SwissLipidsBackend(InputsV2Backend):
    name = 'swisslipids'
    yaml_key = 'swisslipids'
    resource_module = 'swisslipids'
    dataset = 'lipids'


register('swisslipids', SwissLipidsBackend)
