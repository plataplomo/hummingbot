#!/usr/bin/env python3
import asyncio
import logging
import os
from dotenv import load_dotenv
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("test_adapters")

# Load environment variables
load_dotenv()


# Load configuration
def load_config():
    try:
        with open("config.yaml", "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading config.yaml: {e}")
        return {}


async def test_adapters():
    """Test the exchange adapters."""
    from cyberdelta.apis.hyperliquid import HyperliquidAPI
    from cyberdelta.apis.backpack import BackpackAPI
    from cyberdelta.core.funding_rate import HyperliquidAdapter, BackpackAdapter

    # Load configuration
    config = load_config()

    # Setup API clients
    hyperliquid_config = config.get("exchanges", {}).get("hyperliquid", {})
    backpack_config = config.get("exchanges", {}).get("backpack", {})

    # Ensure the configurations are correct
    logger.info(f"Hyperliquid config: {hyperliquid_config}")
    logger.info(f"Backpack config: {backpack_config}")

    # Setup secrets from environment variables
    hyperliquid_secrets = {
        "HYPERLIQUID_WALLET_PRIVATE_KEY": os.environ.get(
            "HYPERLIQUID_WALLET_PRIVATE_KEY"
        ),
        "HYPERLIQUID_WALLET_ADDRESS": os.environ.get("HYPERLIQUID_WALLET_ADDRESS"),
    }

    backpack_secrets = {
        "BACKPACK_API_KEY": os.environ.get("BACKPACK_API_KEY"),
        "BACKPACK_API_SECRET": os.environ.get("BACKPACK_API_SECRET"),
    }

    logger.info("Initializing API clients...")

    # Initialize API clients
    hyperliquid_api = HyperliquidAPI(hyperliquid_config, hyperliquid_secrets)
    backpack_api = BackpackAPI(backpack_config, backpack_secrets)

    # Connect to exchanges
    logger.info("Connecting to exchanges...")
    await hyperliquid_api.connect()
    await backpack_api.connect()

    try:
        # Initialize adapters
        logger.info("Creating adapters...")
        hyperliquid_adapter = HyperliquidAdapter(hyperliquid_config, hyperliquid_api)
        backpack_adapter = BackpackAdapter(backpack_config, backpack_api)

        # Initialize adapters
        logger.info("Initializing Hyperliquid adapter...")
        await hyperliquid_adapter.initialize()

        logger.info("Initializing Backpack adapter...")
        await backpack_adapter.initialize()

        # Get funding rates
        logger.info("Getting Hyperliquid funding rates...")
        hyperliquid_rates = await hyperliquid_adapter.get_funding_rates()

        logger.info(f"Hyperliquid funding rates: {len(hyperliquid_rates)} assets")
        for symbol, rate_info in hyperliquid_rates.items():
            logger.info(
                f"  {symbol}: {rate_info.current_rate:.4f}% (volatility: {rate_info.historical_volatility:.4f}%)"
            )

        logger.info("Getting Backpack funding rates...")
        backpack_rates = await backpack_adapter.get_funding_rates()

        logger.info(f"Backpack funding rates: {len(backpack_rates)} assets")
        for symbol, rate_info in backpack_rates.items():
            logger.info(
                f"  {symbol}: {rate_info.current_rate:.4f}% (volatility: {rate_info.historical_volatility:.4f}%)"
            )

        # Example: Find arbitrage opportunities
        logger.info("Looking for funding rate arbitrage opportunities...")

        # Check matching symbols across exchanges
        hyperliquid_symbols = set(hyperliquid_rates.keys())
        backpack_symbols = set(backpack_rates.keys())

        # Find common symbols (simplified - in production would match BTC-PERP to BTC_USDC)
        common_base_assets = set()
        for h_symbol in hyperliquid_symbols:
            base = h_symbol.split("-")[0] if "-" in h_symbol else h_symbol
            for b_symbol in backpack_symbols:
                b_base = b_symbol.split("-")[0] if "-" in b_symbol else b_symbol
                if base == b_base:
                    common_base_assets.add(base)

        logger.info(
            f"Found {len(common_base_assets)} common base assets: {common_base_assets}"
        )

        # Find opportunities where funding rates differ significantly
        opportunities = []
        for base in common_base_assets:
            # Find corresponding symbols
            h_symbol = next(
                (s for s in hyperliquid_symbols if s.startswith(f"{base}-")), None
            )
            b_symbol = next(
                (s for s in backpack_symbols if s.startswith(f"{base}-")), None
            )

            if not h_symbol or not b_symbol:
                continue

            h_rate = hyperliquid_rates[h_symbol].current_rate
            b_rate = backpack_rates[b_symbol].current_rate

            # Calculate differential
            differential = h_rate - b_rate

            # If significant difference, log opportunity
            threshold = 5.0  # 5% annualized funding rate differential
            if abs(differential) > threshold:
                logger.info(f"Arbitrage opportunity for {base}:")
                logger.info(f"  Hyperliquid ({h_symbol}): {h_rate:.4f}%")
                logger.info(f"  Backpack ({b_symbol}): {b_rate:.4f}%")
                logger.info(f"  Differential: {differential:.4f}%")

                opportunities.append(
                    {
                        "base": base,
                        "hyperliquid_symbol": h_symbol,
                        "backpack_symbol": b_symbol,
                        "differential": differential,
                    }
                )

        logger.info(f"Found {len(opportunities)} potential arbitrage opportunities")

    except Exception as e:
        logger.error(f"Error during test: {e}", exc_info=True)
    finally:
        # Cleanup
        logger.info("Closing API connections...")
        await hyperliquid_api.close()
        await backpack_api.close()
        logger.info("Test completed")


if __name__ == "__main__":
    asyncio.run(test_adapters())
