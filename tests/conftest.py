"""Shared test fixtures."""

import pytest


@pytest.fixture
def sample_mapping_data():
    """Sample ID mapping data for testing."""
    return {
        "P04637": {"TP53"},
        "P00533": {"EGFR"},
        "P38398": {"BRCA1"},
        "Q13315": {"ATM"},
    }


@pytest.fixture
def sample_reverse_data():
    """Reverse mapping (gene symbol -> UniProt)."""
    return {
        "TP53": {"P04637"},
        "EGFR": {"P00533"},
        "BRCA1": {"P38398"},
        "ATM": {"Q13315"},
    }
