# Test markers for categorizing unit vs integration tests
import pytest

unit = pytest.mark.unit
integration = pytest.mark.integration
slow = pytest.mark.slow
network = pytest.mark.network
file_io = pytest.mark.file_io
