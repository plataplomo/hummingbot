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
