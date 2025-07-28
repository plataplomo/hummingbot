"""Fix the Backpack time endpoint by fetching the plain text response and converting it to JSON.

The /api/v1/time endpoint returns plain text instead of JSON, so we need to handle it separately.
This script uses the CyberDeltaEngine configuration system to get the Backpack API URL.
"""

import asyncio
import json
from http import HTTPStatus
from pathlib import Path

import aiohttp

from cyberdelta.config import get_app_settings
from cyberdelta.config.logging_config import setup_logging
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


async def fetch_backpack_time() -> None:
    """Fetch the Backpack time endpoint and save as JSON.

    Raises:
        ValueError: If Backpack exchange configuration is not found.
    """
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

        api_base_url = str(backpack_config.api_base_url_mainnet).rstrip("/")
        logger.info(
            "config_api_url: Using Backpack API base URL from config",
            api_base_url=api_base_url,
        )

    except (ValueError, ImportError, AttributeError, KeyError) as e:
        logger.exception("config_load_failed: Failed to load configuration", error=str(e))
        # Fallback to hardcoded value
        api_base_url = "https://api.backpack.exchange"
        logger.warning("fallback_api_url: Using fallback API URL", api_base_url=api_base_url)

    url = f"{api_base_url}/api/v1/time"

    async with aiohttp.ClientSession() as session:
        try:
            logger.info("fetch_request: Fetching endpoint", url=url)
            async with session.get(url) as response:
                if response.status == HTTPStatus.OK.value:
                    # Get the plain text response
                    text_response = await response.text()
                    logger.info(
                        "response_received: Received text response",
                        text_response=text_response,
                    )

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
                    # Use async file writing to avoid blocking in async function
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        lambda: filepath.write_text(
                            json.dumps(json_data, indent=2), encoding="utf-8"
                        ),
                    )
                    logger.info("fixture_saved: Saved fixture", filepath=str(filepath))
                else:
                    logger.error(
                        "http_error: HTTP error for URL",
                        status=response.status,
                        url=url,
                        response_text=await response.text(),
                    )
        except (OSError, ConnectionError, TimeoutError, ValueError) as e:
            logger.exception("fetch_error: Error fetching URL", url=url, error=str(e))


if __name__ == "__main__":
    asyncio.run(fetch_backpack_time())
