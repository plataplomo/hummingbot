"""Test markers for categorizing integration tests.

Defines pytest markers used throughout the test suite to categorize
different types of tests for selective execution and CI/CD organization.
"""

# Test markers for categorizing tests
import pytest


# Core test categories
integration = pytest.mark.integration
timing = pytest.mark.timing
slow = pytest.mark.slow  # Alias for timing

# Trading type markers
spot = pytest.mark.spot
perp = pytest.mark.perp
cross_exchange = pytest.mark.cross_exchange

# Balance requirement markers (for safety)
requires_balance = pytest.mark.requires_balance
zero_balance = pytest.mark.zero_balance

# Shared functionality
shared = pytest.mark.shared

# VCR network isolation
vcr = pytest.mark.vcr
