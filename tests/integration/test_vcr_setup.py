"""Test VCR setup to verify pytest-recording is working correctly.

This is a simple test to verify that the VCR configuration in conftest.py
is working and can record/playback HTTP interactions.
"""

import aiohttp
import pytest


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.vcr
async def test_vcr_basic_functionality() -> None:
    """Basic test to verify VCR can record and playback HTTP interactions.

    This test makes a simple HTTP request to httpbin.org and verifies
    that VCR can record the interaction and play it back.
    Uses pytest-recording's automatic cassette management.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get("https://httpbin.org/get") as response:
            assert response.status == 200
            data = await response.json()
            assert "url" in data
            assert data["url"] == "https://httpbin.org/get"
