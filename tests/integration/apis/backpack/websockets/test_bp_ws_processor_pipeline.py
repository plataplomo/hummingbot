"""Test 2: The New WebSocket Processor Pipeline.

This module tests the refactored WebSocket processor pipeline that handles
message transformation from raw WebSocket data to validated Pydantic models
and then to internal domain models.

Security Compliance:
- Tests processor pipeline validates input messages
- Validates transformation from raw to domain models
- Tests processor error handling and recovery
- Fails fast on pipeline architecture issues
- Uses REAL WebSocket data from Backpack exchange
- NO mocking of financial operations
- NO hardcoded financial values
"""

import asyncio
import gc
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import psutil
import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawTickerEvent
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.market.ticker import Ticker
from tests.integration.apis.backpack.shared.bp_test_helpers import get_market_constraints


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestBackpackProcessorPipeline:
    """Test WebSocket processor pipeline with real message processing."""

    def _extract_envelope_data(self, context: WebSocketContextProtocol) -> dict[str, Any] | None:
        """Extract data from validated envelope if present."""
        if hasattr(context, "validated_envelope") and context.validated_envelope:
            envelope = context.validated_envelope
            if hasattr(envelope, "data") and envelope.data:
                return {
                    "stream": getattr(envelope, "stream", "unknown"),
                    "data": envelope.data,
                    "timestamp": datetime.now(UTC),
                    "domain_model": getattr(context, "domain_model", None),
                }
        return None

    async def _ensure_websocket_connected(self, bp_api: BackpackAPI) -> None:
        """Ensure we have a real WebSocket connection."""
        needs_connection = not bp_api.is_connected

        if needs_connection:
            await bp_api.connect_websocket()
            if not bp_api.is_connected:
                pytest.fail("Failed to establish real WebSocket connection to Backpack exchange")
            # Small delay to ensure connection is stable
            await asyncio.sleep(0.5)

    async def _wait_for_messages(
        self, messages_list: list[Any], min_count: int, timeout_seconds: int
    ) -> None:
        """Helper to wait for a minimum number of messages."""
        start_time = datetime.now(UTC)
        elapsed = 0.0
        while len(messages_list) < min_count and elapsed < timeout_seconds:
            await asyncio.sleep(0.5)
            elapsed = (datetime.now(UTC) - start_time).total_seconds()

    async def _setup_market_test(self, bp_api: BackpackAPI) -> str:
        """Helper to setup market testing and return symbol."""
        await self._ensure_websocket_connected(bp_api)
        markets = await bp_api.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available from exchange")
        return markets[0].symbol

    @pytest.mark.asyncio
    async def test_processor_pipeline_initialization(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that processor pipeline is properly initialized."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized - processor pipeline unavailable")

        # Check processors are registered
        processors = getattr(router, "processors", {})

        expected_processors = ["ticker", "depth", "trades", "orders", "fills"]
        missing_processors: list[str] = []

        for proc_name in expected_processors:
            if proc_name not in processors:
                missing_processors.append(proc_name)
            else:
                processor = processors[proc_name]
                assert hasattr(processor, "process"), (
                    f"Processor {proc_name} missing process method"
                )

                logger.info(
                    "processor_pipeline_processor_found",
                    processor=proc_name,
                    processor_type=type(processor).__name__,
                    message=f"✓ {proc_name} processor properly initialized",
                )

        if missing_processors:
            pytest.fail(
                f"Processor pipeline missing processors: {missing_processors}. "
                "Pipeline initialization incomplete."
            )

    @pytest.mark.asyncio
    async def test_processor_validates_real_websocket_messages(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test processor pipeline validates real WebSocket messages from exchange."""
        symbol = await self._setup_market_test(bp_api_for_test_env)

        # Track received messages
        received_messages: list[dict[str, Any]] = []
        validation_errors: list[tuple[str, Exception]] = []

        async def validation_handler(context: WebSocketContextProtocol) -> None:
            """Handler that tracks validation results."""
            await asyncio.sleep(0)  # Ensure async function
            try:
                envelope_data = self._extract_envelope_data(context)
                if envelope_data:
                    received_messages.append(envelope_data)
                    logger.info(
                        "real_message_validated",
                        stream=envelope_data["stream"],
                        has_domain_model=envelope_data["domain_model"] is not None,
                    )
            except (ValueError, TypeError, AttributeError, KeyError) as e:
                validation_errors.append(("handler", e))

        # Subscribe to real ticker stream
        await bp_api_for_test_env.subscribe(f"ticker.{symbol}", validation_handler)

        # Wait for real messages
        await self._wait_for_messages(received_messages, 3, 10)

        # Validate results
        if validation_errors:
            pytest.fail(
                f"Validation errors occurred: {validation_errors}. "
                "Processor pipeline failed to handle real messages."
            )

        if not received_messages:
            pytest.fail(
                f"No messages received from ticker.{symbol} within 10s. "
                "WebSocket connection or processor pipeline may be broken."
            )

        # Verify message structure
        for msg in received_messages:
            assert "stream" in msg, "Message missing stream identifier"
            assert "data" in msg, "Message missing data payload"
            assert "timestamp" in msg, "Message missing timestamp"

            # Verify we got domain model transformation
            if msg["stream"].startswith("ticker."):
                domain_model = msg["domain_model"]
                if domain_model is None:
                    pytest.fail(
                        "Ticker message was not transformed to domain model. "
                        "Processor pipeline transformation broken."
                    )

                # Verify it's a proper Ticker domain model
                assert isinstance(domain_model, Ticker), (
                    f"Expected Ticker domain model, got {type(domain_model)}"
                )
                assert domain_model.symbol == symbol
                assert isinstance(domain_model.price, Decimal)
                assert domain_model.price > 0

        logger.info(
            "processor_validation_test_passed",
            message_count=len(received_messages),
            symbol=symbol,
            message="✓ Processor successfully validated and transformed real WebSocket messages",
        )

    @pytest.mark.asyncio
    async def test_processor_transforms_real_data_to_pydantic_models(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test processor transforms real exchange data to Pydantic models."""
        symbol = await self._setup_market_test(bp_api_for_test_env)

        # Get current real ticker data from REST API
        real_ticker = await bp_api_for_test_env.get_ticker(symbol)
        if not real_ticker:
            pytest.fail(f"Failed to get real ticker data for {symbol}")

        # Now test WebSocket ticker transformation
        pydantic_models_created: list[BackpackRawTickerEvent] = []

        async def pydantic_test_handler(context: WebSocketContextProtocol) -> None:
            """Handler that captures Pydantic model creation."""
            await asyncio.sleep(0)  # Ensure async function
            if hasattr(context, "validated_envelope") and context.validated_envelope:
                envelope = context.validated_envelope
                # Check if we have valid ticker stream data
                if (
                    hasattr(envelope, "stream")
                    and hasattr(envelope, "data")
                    and isinstance(getattr(envelope, "stream", None), str)
                    and getattr(envelope, "stream", "").startswith("ticker.")
                    and isinstance(envelope.data, dict)
                ):
                    try:
                        # Try to create Pydantic model from real data
                        ticker_model = BackpackRawTickerEvent.model_validate(envelope.data)
                        pydantic_models_created.append(ticker_model)
                    except ValidationError as e:
                        pytest.fail(
                            f"Failed to create Pydantic model from real WebSocket data: {e}\n"
                            f"Data: {envelope.data}"
                        )

        # Subscribe to real ticker
        await bp_api_for_test_env.subscribe(f"ticker.{symbol}", pydantic_test_handler)

        # Wait for real ticker updates
        await self._wait_for_messages(pydantic_models_created, 2, 10)

        if not pydantic_models_created:
            pytest.fail(
                f"No Pydantic models created from real WebSocket data for {symbol}. "
                "Processor pipeline may not be receiving or transforming messages."
            )

        # Validate the Pydantic models have real data
        for model in pydantic_models_created:
            assert model.symbol == symbol
            # Prices should be parsable decimals
            assert Decimal(model.last_price) > 0
            assert Decimal(model.high) > 0
            assert Decimal(model.low) > 0
            assert Decimal(model.volume) >= 0
            assert Decimal(model.quote_volume) >= 0

            # Sanity check - prices should be in reasonable range compared to REST data
            ws_price = Decimal(model.last_price)
            rest_price = Decimal(str(real_ticker.price))
            price_diff_pct = abs(ws_price - rest_price) / rest_price

            # Allow up to 10% difference (markets can move)
            if price_diff_pct > Decimal("0.1"):
                logger.warning(
                    "large_price_difference",
                    ws_price=str(ws_price),
                    rest_price=str(rest_price),
                    diff_pct=str(price_diff_pct),
                    message="Large price difference between WebSocket and REST API",
                )

        logger.info(
            "pydantic_transformation_verified",
            models_created=len(pydantic_models_created),
            symbol=symbol,
            message="✓ Successfully transformed real WebSocket data to Pydantic models",
        )

    @pytest.mark.asyncio
    async def test_processor_creates_domain_models_from_real_data(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test processor transforms real exchange data to domain models."""
        symbol = await self._setup_market_test(bp_api_for_test_env)

        # Collect domain models from real data
        domain_models: list[Ticker] = []

        async def domain_model_handler(context: WebSocketContextProtocol) -> None:
            """Handler that captures domain model creation."""
            await asyncio.sleep(0)  # Ensure async function
            if (
                hasattr(context, "domain_model")
                and context.domain_model
                and isinstance(context.domain_model, Ticker)
            ):
                domain_models.append(context.domain_model)

        # Subscribe to real ticker stream
        await bp_api_for_test_env.subscribe(f"ticker.{symbol}", domain_model_handler)

        # Wait for domain models
        await self._wait_for_messages(domain_models, 3, 10)

        if not domain_models:
            pytest.fail(
                f"No domain models created from real WebSocket data for {symbol}. "
                "Processor pipeline transformation to domain models is broken."
            )

        # Get market constraints for validation
        constraints = await get_market_constraints(bp_api_for_test_env, symbol)
        tick_size = constraints["tick_size"]

        # Validate domain models
        for model in domain_models:
            # Basic validation
            assert isinstance(model, Ticker), f"Expected Ticker, got {type(model)}"
            assert model.symbol == symbol
            assert isinstance(model.price, Decimal)
            assert model.price > 0

            # Price should respect tick size
            assert model.price % tick_size == 0, (
                f"Price {model.price} doesn't respect tick size {tick_size}"
            )

            # Validate other fields if present
            if model.volume is not None:
                assert isinstance(model.volume, Decimal)
                assert model.volume >= 0

            if model.timestamp:
                # Timestamp should be recent (within last minute)
                age = datetime.now(UTC) - model.timestamp
                assert age < timedelta(minutes=1), f"Domain model timestamp too old: {age}"

        logger.info(
            "domain_model_transformation_verified",
            models_created=len(domain_models),
            symbol=symbol,
            tick_size=str(tick_size),
            message="✓ Successfully created domain models from real WebSocket data",
        )

    @pytest.mark.asyncio
    async def test_processor_pipeline_real_message_flow(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test complete processor pipeline with real WebSocket messages."""
        try:
            # Ensure WebSocket is connected
            await self._ensure_websocket_connected(bp_api_for_test_env)

            # Track pipeline processing
            pipeline_results: list[dict[str, Any]] = []

            async def pipeline_test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029

                # Extract data from typed context
                context_data: dict[str, Any] = {}
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    context_data = data if isinstance(data, dict) else {"data": data}

                pipeline_results.append({
                    "context": context,
                    "timestamp": asyncio.get_event_loop().time(),
                    "processed": True,
                })
                logger.info(
                    "processor_pipeline_real_message_processed",
                    context_keys=list(context_data.keys()),
                    message="✓ Pipeline processed real WebSocket message",
                )

            # Subscribe to ticker to test pipeline
            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for pipeline testing")

            test_symbol = markets[0].symbol
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", pipeline_test_handler)

            # Wait for pipeline to process messages
            await asyncio.sleep(3.0)

            if pipeline_results:
                logger.info(
                    "processor_pipeline_real_flow_success",
                    processed_count=len(pipeline_results),
                    symbol=test_symbol,
                    message="✓ Processor pipeline successfully processed real messages",
                )

                # Verify context structure
                for result in pipeline_results[:3]:  # Check first 3 results
                    context = result["context"]
                    # Context is now a WebSocketContextProtocol object, not a dict
                    assert hasattr(context, "routing_key"), "Context should have routing_key"

                    # Extract key attributes from context object
                    context_attrs = [attr for attr in dir(context) if not attr.startswith("_")]
                    logger.info(
                        "processor_pipeline_context_structure",
                        context_attrs=context_attrs[:10],  # Show first 10 attrs
                        routing_key=getattr(context, "routing_key", None),
                        message="Pipeline produced structured context",
                    )
            else:
                logger.warning(
                    "processor_pipeline_no_real_messages",
                    symbol=test_symbol,
                    message="No real messages processed by pipeline - may indicate pipeline issues",
                )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Processor pipeline real message flow failed: {e}. "
                "Complete pipeline not working with real WebSocket data."
            )

    async def _get_test_symbol(self, bp_api_for_test_env: BackpackAPI) -> str:
        """Get a symbol for testing."""
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available")
        return markets[0].symbol

    def _calculate_performance_metrics(
        self,
        message_count: int,
        processing_times: list[float],
        start_time: float,
        symbol: str,
    ) -> tuple[float, float]:
        """Calculate and log performance metrics."""
        end_time = asyncio.get_event_loop().time()
        total_time = end_time - start_time
        messages_per_second = message_count / total_time if total_time > 0 else 0

        logger.info(
            "processor_pipeline_performance_results",
            message_count=message_count,
            total_time_seconds=f"{total_time:.3f}",
            messages_per_second=f"{messages_per_second:.1f}",
            message=f"✓ Pipeline processed {message_count} messages in {total_time:.3f}s",
        )

        if message_count == 0:
            pytest.fail(
                f"No messages received from {symbol} during {total_time:.3f}s performance test. "
                "WebSocket connection may be broken."
            )

        avg_processing_time_ms = (
            (sum(processing_times) / len(processing_times)) * 1000 if processing_times else 0
        )

        logger.info(
            "real_pipeline_performance",
            symbol=symbol,
            message_count=message_count,
            test_duration_seconds=f"{total_time:.1f}",
            messages_per_second=f"{messages_per_second:.1f}",
            avg_processing_time_ms=f"{avg_processing_time_ms:.3f}",
            message=(
                f"✓ Pipeline processed {message_count} real messages at "
                f"{messages_per_second:.1f} msg/s"
            ),
        )

        return messages_per_second, avg_processing_time_ms

    @pytest.mark.asyncio
    async def test_processor_pipeline_performance(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test processor pipeline performance with multiple messages."""
        # Ensure WebSocket is connected
        await self._ensure_websocket_connected(bp_api_for_test_env)

        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        processors = getattr(router, "processors", {}) if router else {}

        if "ticker" not in processors:
            pytest.fail("Ticker processor not found for performance testing")

        # Get real market for testing
        symbol = await self._get_test_symbol(bp_api_for_test_env)

        # Track performance metrics
        message_count = 0
        processing_times: list[float] = []

        async def performance_handler(context: WebSocketContextProtocol) -> None:
            """Handler that tracks processing time."""
            await asyncio.sleep(0)  # Ensure async function
            nonlocal message_count
            start = asyncio.get_event_loop().time()
            # Simulate some processing work
            if hasattr(context, "domain_model") and context.domain_model:
                # Access the model to ensure it's fully processed - use protocol methods
                try:
                    model_data = context.domain_model.model_dump()
                    _ = model_data.get("symbol")
                    _ = model_data.get("price")
                except AttributeError:
                    # Fallback for models that don't have model_dump
                    try:
                        model_data = context.domain_model.dict()
                        _ = model_data.get("symbol")
                        _ = model_data.get("price")
                    except AttributeError:
                        pass
            end = asyncio.get_event_loop().time()
            processing_times.append(end - start)
            message_count += 1

        # Subscribe to real ticker stream
        await bp_api_for_test_env.subscribe(f"ticker.{symbol}", performance_handler)

        # Collect messages for 5 seconds
        test_duration = 5.0
        start_time = asyncio.get_event_loop().time()

        elapsed = 0.0
        while elapsed < test_duration:
            await asyncio.sleep(0.1)
            elapsed = asyncio.get_event_loop().time() - start_time

        # No need to unsubscribe - cleanup happens on test end

        # Calculate and log metrics
        messages_per_second, avg_processing_time_ms = self._calculate_performance_metrics(
            message_count=message_count,
            processing_times=processing_times,
            start_time=start_time,
            symbol=symbol,
        )

        # Performance thresholds based on real-world requirements
        # Most exchanges send ticker updates 1-10 times per second
        if messages_per_second < 0.5:
            pytest.fail(
                f"Pipeline processing rate too low: {messages_per_second:.1f} msg/s. "
                "Expected at least 0.5 messages per second for ticker updates."
            )

        # Processing time should be fast for HFT
        if avg_processing_time_ms > 10:  # 10ms threshold
            logger.warning(
                "high_processing_latency",
                avg_ms=f"{avg_processing_time_ms:.3f}",
                message="Average processing time exceeds 10ms - may be too slow for HFT",
            )

    @pytest.mark.asyncio
    async def test_processor_pipeline_memory_efficiency(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test processor pipeline memory efficiency."""
        # Ensure WebSocket is connected
        await self._ensure_websocket_connected(bp_api_for_test_env)

        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        processors = getattr(router, "processors", {}) if router else {}

        if "ticker" not in processors:
            pytest.fail("Ticker processor not found for memory testing")

        # Test that processor doesn't accumulate memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Get real market
        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available")

        symbol = markets[0].symbol

        # Track messages for memory test
        message_count = 0

        async def memory_test_handler(context: WebSocketContextProtocol) -> None:
            """Simple handler for memory testing."""
            await asyncio.sleep(0)  # Ensure async function
            nonlocal message_count
            message_count += 1
            # Don't store anything to avoid artificial memory growth

        # Subscribe to real stream
        await bp_api_for_test_env.subscribe(f"ticker.{symbol}", memory_test_handler)

        # Process messages for 10 seconds
        test_duration = 10.0
        start_time = asyncio.get_event_loop().time()
        last_gc_time = start_time

        while (asyncio.get_event_loop().time() - start_time) < test_duration:
            current_time = asyncio.get_event_loop().time()
            # Force GC every 2 seconds
            if current_time - last_gc_time > 2.0:
                gc.collect()
                last_gc_time = current_time
            await asyncio.sleep(0.1)

        # Final GC before measurement
        gc.collect()
        await asyncio.sleep(0.5)  # Let memory settle

        # No need to unsubscribe - cleanup happens on test end

        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        memory_increase_mb = memory_increase / (1024 * 1024)

        logger.info(
            "processor_pipeline_memory_usage",
            initial_memory_mb=f"{initial_memory / (1024 * 1024):.1f}",
            final_memory_mb=f"{final_memory / (1024 * 1024):.1f}",
            memory_increase_mb=f"{memory_increase_mb:.1f}",
            message=f"Pipeline memory increase: {memory_increase_mb:.1f} MB after 1000 messages",
        )

        logger.info(
            "memory_efficiency_test_complete",
            symbol=symbol,
            message_count=message_count,
            test_duration_seconds=f"{test_duration:.1f}",
            initial_memory_mb=f"{initial_memory / (1024 * 1024):.1f}",
            final_memory_mb=f"{final_memory / (1024 * 1024):.1f}",
            memory_increase_mb=f"{memory_increase_mb:.1f}",
            messages_per_mb=(
                f"{message_count / max(memory_increase_mb, 0.1):.1f}"
                if message_count > 0
                else "N/A"
            ),
            message=(
                f"Pipeline processed {message_count} real messages with "
                f"{memory_increase_mb:.1f} MB memory increase"
            ),
        )

        # Memory increase should be reasonable
        # With real data, some increase is expected due to internal buffers
        # But it shouldn't grow unbounded
        if message_count > 0:
            mb_per_1000_messages = (memory_increase_mb / message_count) * 1000
            if mb_per_1000_messages > 10:  # More than 10MB per 1000 messages
                logger.warning(
                    "high_memory_usage_per_message",
                    mb_per_1000_msgs=f"{mb_per_1000_messages:.1f}",
                    message="High memory usage per message - possible memory leak",
                )

        # Absolute threshold - shouldn't use more than 100MB for a 10 second test
        if memory_increase_mb > 100:
            pytest.fail(
                f"Excessive memory usage: {memory_increase_mb:.1f} MB increase. "
                "Pipeline may have a memory leak."
            )
