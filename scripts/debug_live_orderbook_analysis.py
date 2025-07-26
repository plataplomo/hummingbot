#!/usr/bin/env python3
"""Live Backpack WebSocket Analysis - REAL EXCHANGE PROOF.

This script connects to Backpack's LIVE WebSocket and captures REAL messages
to validate the problem document claims with actual exchange data.

PROOF REQUIRED:
1. Backpack sends incremental updates with null bids/asks ✓ or ❌
2. System creates empty OrderBook objects from these updates ✓ or ❌
3. Majority of messages are incremental updates creating empty orderbooks ✓ or ❌
4. Claims in problem document match live exchange behavior ✓ or ❌
"""

import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Any

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.apis.exceptions.parsing import MsgpackSerializationError
from cyberdelta.apis.exceptions.websocket import InvalidWebSocketDataError
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.config_manager import ConfigManager
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.order_book import OrderBook


logger = get_logger(__name__)


class LiveExchangeProof:
    """Captures REAL exchange messages to prove or disprove problem document claims."""

    def __init__(self) -> None:
        """Initialize the live exchange proof analyzer."""
        self.raw_messages: list[dict[str, Any]] = []
        self.parsed_events: list[BackpackRawDepthUpdateEvent] = []
        self.created_orderbooks: list[OrderBook] = []
        self.message_types: list[str] = []
        self.start_time = datetime.now(UTC)
        self.symbol = "SOL_USDC"

        # Evidence counters
        self.snapshots = 0
        self.incremental_with_data = 0
        self.incremental_empty = 0
        self.empty_orderbooks_created = 0

    def analyze_message(self, raw_data: dict[str, Any]) -> str:
        """Analyze real message and classify type."""
        bids = raw_data.get("bids")
        asks = raw_data.get("asks")
        first_id = raw_data.get("U")
        last_id = raw_data.get("u")

        has_orderbook_data = bids is not None and asks is not None
        same_sequence = str(first_id) == str(last_id) if first_id and last_id else False

        if has_orderbook_data and same_sequence:
            self.snapshots += 1
            return "SNAPSHOT"
        if has_orderbook_data and not same_sequence:
            self.incremental_with_data += 1
            return "INCREMENTAL_WITH_DATA"
        if not has_orderbook_data:
            self.incremental_empty += 1
            return "INCREMENTAL_EMPTY"
        return "UNKNOWN"

    async def capture_live_message(self, context: WebSocketContextProtocol) -> None:
        """Capture and analyze each live message from Backpack."""
        try:
            if not (
                hasattr(context, "validated_envelope")
                and context.validated_envelope is not None
                and hasattr(context.validated_envelope, "data")
            ):
                return

            raw_data = context.validated_envelope.data
            if not isinstance(raw_data, dict):
                return

            # Add capture timestamp
            message_copy = dict(raw_data)
            message_copy["_live_capture_timestamp"] = datetime.now(UTC).isoformat()
            self.raw_messages.append(message_copy)

            # Analyze message type
            message_type = self.analyze_message(raw_data)
            self.message_types.append(message_type)

            # Parse with Pydantic model
            try:
                parsed_event = BackpackRawDepthUpdateEvent.model_validate(raw_data)
                self.parsed_events.append(parsed_event)

                # Transform to OrderBook using actual mapper
                orderbook = BackpackOrderBookMapper.transform_ws_depth_event_to_internal(
                    self.symbol, parsed_event
                )
                self.created_orderbooks.append(orderbook)

                # Check if empty orderbook was created
                is_empty = len(orderbook.bids) == 0 and len(orderbook.asks) == 0
                if is_empty:
                    self.empty_orderbooks_created += 1

                # Log each message with evidence
                logger.info(
                    "live_exchange_evidence",
                    message_type=message_type,
                    has_bids=raw_data.get("bids") is not None,
                    has_asks=raw_data.get("asks") is not None,
                    first_update_id=raw_data.get("U"),
                    last_update_id=raw_data.get("u"),
                    sequence_range=int(raw_data.get("u", 0)) - int(raw_data.get("U", 0)),
                    orderbook_empty=is_empty,
                    orderbook_bids_count=len(orderbook.bids),
                    orderbook_asks_count=len(orderbook.asks),
                    problem_evidence=is_empty and message_type == "INCREMENTAL_EMPTY",
                )

            except (MsgpackSerializationError, InvalidWebSocketDataError) as e:
                logger.exception("live_message_parsing_failed", error=str(e))

        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except (MsgpackSerializationError, InvalidWebSocketDataError) as e:
            logger.exception("live_capture_error", error=str(e))

    def print_live_evidence(self) -> None:
        """Print real-time evidence from live exchange."""
        total = len(self.raw_messages)
        if total == 0:
            logger.info("⏳ Waiting for live exchange messages...")
            return

        elapsed = (datetime.now(UTC) - self.start_time).total_seconds()
        empty_percentage = (
            (self.empty_orderbooks_created / len(self.created_orderbooks) * 100)
            if self.created_orderbooks
            else 0
        )

        separator = "=" * 80
        logger.info("Analysis separator")
        logger.info("🔴 LIVE BACKPACK EXCHANGE ANALYSIS", elapsed_seconds=f"{elapsed:.1f}")
        logger.info("📡 Analysis Summary", symbol=self.symbol, total_messages=total)
        logger.info(separator)
        logger.info("MESSAGE BREAKDOWN (REAL EXCHANGE DATA):")
        logger.info(
            "Snapshots", count=self.snapshots, percentage=round(self.snapshots / total * 100, 1)
        )
        logger.info(
            "Incremental with data",
            count=self.incremental_with_data,
            percentage=round(self.incremental_with_data / total * 100, 1),
        )
        logger.info(
            "Incremental empty",
            count=self.incremental_empty,
            percentage=round(self.incremental_empty / total * 100, 1),
        )
        logger.info("")
        logger.info("ORDERBOOK CREATION RESULTS:")
        logger.info("Total OrderBooks", count=len(self.created_orderbooks))
        logger.info(
            "Empty OrderBooks",
            count=self.empty_orderbooks_created,
            percentage=round(empty_percentage, 1),
        )
        logger.info(
            "Valid OrderBooks", count=len(self.created_orderbooks) - self.empty_orderbooks_created
        )
        logger.info("")
        logger.info("🎯 PROBLEM DOCUMENT VALIDATION:")
        has_empty_incrementals = self.incremental_empty > 0
        creates_empty_orderbooks = self.empty_orderbooks_created > 0
        majority_are_empty = empty_percentage > 50  # noqa: PLR2004

        logger.info(
            "Sends null bids/asks",
            result="CONFIRMED" if has_empty_incrementals else "REJECTED",
            count=self.incremental_empty,
        )
        logger.info(
            "Creates empty OrderBooks",
            result="CONFIRMED" if creates_empty_orderbooks else "REJECTED",
            count=self.empty_orderbooks_created,
        )
        logger.info(
            "Majority are empty",
            result="CONFIRMED" if majority_are_empty else "REJECTED",
            percentage=round(empty_percentage, 1),
        )
        logger.info("Problem severity", level="HIGH" if majority_are_empty else "LOW")
        logger.info(separator)

    def save_live_evidence(self) -> str:
        """Save live evidence to file with timestamp."""
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = f"live_backpack_evidence_{timestamp}.json"

        empty_percentage = (
            (self.empty_orderbooks_created / len(self.created_orderbooks) * 100)
            if self.created_orderbooks
            else 0
        )

        evidence = {
            "exchange": "backpack",
            "symbol": self.symbol,
            "analysis_period": {
                "start": self.start_time.isoformat(),
                "end": datetime.now(UTC).isoformat(),
                "duration_seconds": (datetime.now(UTC) - self.start_time).total_seconds(),
            },
            "live_message_counts": {
                "total_messages": len(self.raw_messages),
                "snapshots": self.snapshots,
                "incremental_with_data": self.incremental_with_data,
                "incremental_empty": self.incremental_empty,
            },
            "orderbook_analysis": {
                "total_orderbooks_created": len(self.created_orderbooks),
                "empty_orderbooks": self.empty_orderbooks_created,
                "empty_percentage": empty_percentage,
            },
            "problem_document_validation": {
                "sends_null_bids_asks": self.incremental_empty > 0,
                "creates_empty_orderbooks": self.empty_orderbooks_created > 0,
                "majority_are_empty": empty_percentage > 50,  # noqa: PLR2004
                "claims_validated": self.incremental_empty > 0
                and self.empty_orderbooks_created > 0,
            },
            "sample_messages": {
                "first_10_raw_messages": self.raw_messages[:10],
                "sample_empty_incremental": [
                    msg
                    for i, msg in enumerate(self.raw_messages[:20])
                    if i < len(self.message_types) and self.message_types[i] == "INCREMENTAL_EMPTY"
                ][:5],
            },
        }

        from pathlib import Path  # noqa: PLC0415

        with Path(filename).open("w", encoding="utf-8") as f:
            json.dump(evidence, f, indent=2, default=str)

        return filename


async def run_live_proof(duration: int = 60) -> int:
    """Run live proof against real Backpack exchange."""
    logger.info("🔍 LIVE BACKPACK WEBSOCKET PROOF")
    logger.info("🚨 CONNECTING TO REAL EXCHANGE...")
    logger.info("Analysis Duration", duration_seconds=duration)
    logger.info("🎯 Target: SOL_USDC depth stream")
    logger.info("")

    proof = LiveExchangeProof()
    api = None

    try:
        # Initialize configuration using test config
        config_manager = ConfigManager("tests/config/test_config.yaml")
        secrets_manager = SecretsManager("tests/config/test_secrets.yaml")

        # Get Backpack config and secrets from loaded settings
        app_settings = config_manager.settings
        if not app_settings:
            logger.error("❌ Failed to load app settings!")
            return 1

        # Access the exchange configs directly from the settings
        bp_config = app_settings.exchanges["backpack"]
        if secrets_manager.secrets_data is None:
            raise ValueError("Secrets configuration not loaded")
        bp_secrets = secrets_manager.secrets_data.exchanges["backpack"]

        # Create API instance
        api = BackpackAPI(
            exchange_config=bp_config,
            exchange_secrets=bp_secrets,
        )

        logger.info("✅ Connected to Backpack API")

        # Get available markets
        markets = await api.get_markets(GetMarketsArgs())
        if not markets:
            logger.error("❌ No markets available!")
            return 1

        # Find SOL_USDC or use first available
        test_symbol = next((m.symbol for m in markets if "SOL" in m.symbol), markets[0].symbol)
        proof.symbol = test_symbol

        logger.info("Using symbol", symbol=test_symbol)

        # Connect WebSocket first
        logger.info("🔌 Connecting to WebSocket...")
        await api.connect_websocket()

        # Subscribe to live depth stream
        logger.info("Subscribing to depth stream", channel=f"depth.{test_symbol}")
        await api.subscribe(f"depth.{test_symbol}", proof.capture_live_message)

        # Capture live data
        logger.info("📡 CAPTURING LIVE EXCHANGE MESSAGES...")
        start_time = asyncio.get_event_loop().time()

        while (asyncio.get_event_loop().time() - start_time) < duration:
            await asyncio.sleep(3)  # Update every 3 seconds
            proof.print_live_evidence()

        logger.info("\n✅ LIVE CAPTURE COMPLETE")

        # Final evidence
        proof.print_live_evidence()
        evidence_file = proof.save_live_evidence()

        # VERDICT
        total_messages = len(proof.raw_messages)
        empty_percentage = (
            (proof.empty_orderbooks_created / len(proof.created_orderbooks) * 100)
            if proof.created_orderbooks
            else 0
        )

        logger.info("\n🎯 FINAL LIVE EXCHANGE VERDICT:")
        logger.info("Evidence saved", filename=evidence_file)

        if (
            proof.incremental_empty > 0
            and proof.empty_orderbooks_created > 0
            and empty_percentage > 30  # noqa: PLR2004
        ):
            logger.info("✅ PROBLEM DOCUMENT CLAIMS VALIDATED BY LIVE EXCHANGE")
            logger.info("Messages analyzed", count=total_messages)
            logger.info("Empty incremental updates", count=proof.incremental_empty)
            logger.info(
                "Empty OrderBooks created",
                count=proof.empty_orderbooks_created,
                percentage=round(empty_percentage, 1),
            )
            logger.info("   🚨 Real architectural problem confirmed")
        else:
            logger.info("❌ PROBLEM DOCUMENT CLAIMS NOT SUPPORTED BY LIVE DATA")
            logger.info("Messages analyzed", count=total_messages)
            logger.info("Empty incremental updates", count=proof.incremental_empty)
            logger.info(
                "Empty OrderBooks created",
                count=proof.empty_orderbooks_created,
                percentage=round(empty_percentage, 1),
            )
            logger.info("   i  Problem may not exist or be less severe")

    except Exception:
        logger.exception("live_proof_failed")
        return 1
    else:
        return 0
    finally:
        if api is not None:
            import contextlib  # noqa: PLC0415

            with contextlib.suppress(Exception):
                await api.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Live Backpack Exchange Proof")
    parser.add_argument("--duration", type=int, default=60, help="Analysis duration in seconds")
    args = parser.parse_args()

    try:
        result = asyncio.run(run_live_proof(args.duration))
        sys.exit(result)
    except KeyboardInterrupt:
        logger.info("\n🛑 Analysis interrupted by user")
        sys.exit(0)
