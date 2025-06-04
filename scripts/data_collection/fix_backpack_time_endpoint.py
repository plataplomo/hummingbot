#!/usr/bin/env python3
"""Fix the Backpack time endpoint by fetching the plain text response and converting it to JSON.

The /api/v1/time endpoint returns plain text instead of JSON, so we need to handle it separately.
This script uses the CyberDeltaEngine configuration system to get the Backpack API URL.
"""

import asyncio
import json
import logging
from pathlib import Path

import aiohttp

from cyberdelta.config import get_app_settings
from cyberdelta.config.logging_config import setup_logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def fetch_backpack_time() -> None:
    """Fetch the Backpack time endpoint and save as JSON."""
    output_dir = Path("tests/fixtures/raw_api_data/backpack")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get configuration
    try:
        app_settings = get_app_settings()
        setup_logging(app_settings)

        backpack_config = app_settings.exchanges.get("backpack")
        if not backpack_config:
            raise ValueError("Backpack exchange configuration not found")
        if not backpack_config.enabled:
            raise ValueError("Backpack exchange is disabled in configuration")

        api_base_url = str(backpack_config.api_base_url).rstrip("/")
        logger.info(f"Using Backpack API base URL from config: {api_base_url}")

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        # Fallback to hardcoded value
        api_base_url = "https://api.backpack.exchange"
        logger.warning(f"Using fallback API URL: {api_base_url}")

    url = f"{api_base_url}/api/v1/time"

    async with aiohttp.ClientSession() as session:
        try:
            logger.info(f"Fetching: {url}")
            async with session.get(url) as response:
                if response.status == 200:
                    # Get the plain text response
                    text_response = await response.text()
                    logger.info(f"Received text response: {text_response}")

                    # Convert to JSON format
                    try:
                        # Try to parse as integer (Unix timestamp)
                        timestamp = int(text_response.strip())
                        json_data: dict[str, int | str] = {"serverTime": timestamp}
                    except ValueError:
                        # If not an integer, store as string
                        json_data = {"serverTime": text_response.strip()}

                    # Save as JSON
                    filepath = output_dir / "bp_time.json"
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(json_data, f, indent=2)
                    logger.info(f"Saved fixture: {filepath}")
                else:
                    logger.error(f"HTTP {response.status} error for {url}: {await response.text()}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")


if __name__ == "__main__":
    asyncio.run(fetch_backpack_time())
