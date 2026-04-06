"""UniChem mapping backend for small molecule identifiers."""

from __future__ import annotations

import logging

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend

_log = logging.getLogger(__name__)


class UniChemBackend(MappingBackend):
    name = 'unichem'
    yaml_key = 'unichem'

    def read(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        """UniChem mappings are not organism-specific."""
        try:
            return self._read_via_pypath(id_type, target_id_type, ncbi_tax_id)
        except ImportError:
            _log.debug('pypath not available for unichem backend')
            return {}

    def _read_via_pypath(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        from pypath.inputs.unichem import unichem_mapping

        _log.info('UniChem: %s -> %s', id_type, target_id_type)

        try:
            data = unichem_mapping(id_type, target_id_type)
        except (ValueError, KeyError) as e:
            _log.debug('UniChem: %s', e)
            return {}

        if not data:
            return {}

        # unichem_mapping returns {id: set(ids)} already
        return {k: (v if isinstance(v, set) else {v}) for k, v in data.items()}

    def _read_direct(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        raise ImportError('unichem requires pypath')


register('unichem', UniChemBackend)
