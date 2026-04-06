"""RaMP mapping backend for metabolite/compound identifiers."""

from __future__ import annotations

import logging

from omnipath_utils.mapping.backends import register
from omnipath_utils.mapping.backends._base import MappingBackend

_log = logging.getLogger(__name__)


class RampBackend(MappingBackend):
    name = 'ramp'
    yaml_key = 'ramp'

    def read(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        """RaMP mappings are not organism-specific (compound IDs)."""
        try:
            return self._read_via_pypath(id_type, target_id_type, ncbi_tax_id)
        except ImportError:
            _log.debug('pypath not available for ramp backend')
            return {}

    def _read_via_pypath(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        from pypath.inputs.ramp import ramp_mapping

        _log.info('RaMP: %s -> %s', id_type, target_id_type)

        try:
            data = ramp_mapping(id_type, target_id_type)
        except Exception as e:
            _log.debug('RaMP: %s', e)
            return {}

        if not data:
            return {}

        # ramp_mapping returns {id: set(ids)} already
        return {k: (v if isinstance(v, set) else {v}) for k, v in data.items()}

    def _read_direct(self, id_type, target_id_type, ncbi_tax_id, **kwargs):
        raise ImportError('ramp requires pypath')


register('ramp', RampBackend)
