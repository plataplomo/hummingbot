"""Test markers for categorizing unit vs integration tests.

Defines pytest markers used throughout the test suite to categorize
different types of tests for selective execution and CI/CD organization.
"""

# Test markers for categorizing unit vs integration tests
import pytest


unit = pytest.mark.unit
integration = pytest.mark.integration
slow = pytest.mark.slow
network = pytest.mark.network
file_io = pytest.mark.file_io

# VCR cassette directory marker
vcr_cassette_dir = pytest.mark.vcr_cassette_dir

# Trading type markers
spot = pytest.mark.spot
perp = pytest.mark.perp
cross_exchange = pytest.mark.cross_exchange

# Balance requirement markers
requires_balance = pytest.mark.requires_balance
zero_balance = pytest.mark.zero_balance
positive_balance = pytest.mark.positive_balance
