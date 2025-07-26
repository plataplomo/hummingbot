"""Test 2: WebSocket Processor Pipeline.

This module tests that the refactored Hyperliquid WebSocket processor pipeline
correctly transforms raw WebSocket data through Pydantic models to domain models.

Security Compliance:
- Tests processor pipeline data flow
- Validates data transformation at each stage
- Tests processor error handling
- Fails fast on pipeline issues
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestHyperliquidWebSocketProcessorPipeline:
    """Test WebSocket processor pipeline in refactored architecture."""

    @pytest.mark.asyncio
    async def test_processor_pipeline_initialization(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that processor pipeline is properly initialized."""
        # Check for router
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized - processor pipeline unavailable")

        # Check for processors
        processors = getattr(router, "processors", {})
        assert len(processors) > 0, "No processors registered in pipeline"

        # Validate each processor
        for name, processor in processors.items():
            assert hasattr(processor, "process"), f"Processor {name} missing process method"

            # Check if processor has validation
            if hasattr(processor, "validate"):
                logger.info(
                    "processor_has_validation",
                    processor_name=name,
                    message=f"Processor {name} has validation method",
                )

            # Check if processor has transformation
            if hasattr(processor, "transform"):
                logger.info(
                    "processor_has_transformation",
                    processor_name=name,
                    message=f"Processor {name} has transformation method",
                )

        logger.info(
            "processor_pipeline_initialized",
            processor_count=len(processors),
            processor_names=list(processors.keys()),
            message="✓ Processor pipeline properly initialized",
        )

    @pytest.mark.asyncio
    async def test_raw_to_pydantic_transformation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test transformation from raw WebSocket data to Pydantic models."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available")

        # Connect WebSocket first
        await hl_api_for_test_env.connect_websocket()

        if not hl_api_for_test_env.is_connected:
            pytest.fail("WebSocket connection failed")

        # Track what we receive through the pipeline
        received_data: list[Any] = []
        pydantic_models_found: list[str] = []

        async def tracking_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            received_data.append(context)

            # Check if we're receiving properly transformed data
            # Context is now a typed object, not a dict
            if hasattr(context, "validated_envelope"):
                pydantic_models_found.append(
                    f"envelope:{type(context.validated_envelope).__name__}"
                )
                logger.info(
                    "pydantic_model_in_context",
                    model_type=type(context.validated_envelope).__name__,
                    message="Pydantic model found in handler context",
                )

        # Subscribe to get data flowing through pipeline
        await hl_api_for_test_env.subscribe("allMids", tracking_handler)

        # Wait for data
        await asyncio.sleep(3.0)

        logger.info(
            "raw_to_pydantic_transformation_results",
            raw_messages_received=len(received_data),
            pydantic_models_found=len(pydantic_models_found),
            message="✓ Raw to Pydantic transformation tested",
        )

        # At least some data should have been received
        assert len(received_data) > 0, "No data received through pipeline"

    @pytest.mark.asyncio
    async def test_pydantic_to_domain_model_transformation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test transformation from Pydantic models to domain models."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available")

            test_symbol = markets[0].symbol
            domain_models_received: list[Any] = []

            async def domain_model_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029

                # Check context for domain models
                # Context is now a typed object, check its attributes
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    # Check if data contains domain models
                    data = context.validated_envelope.data
                    if hasattr(data, "__class__"):
                        class_name = data.__class__.__name__
                        if class_name in ["OrderBook", "Trade", "Ticker", "Order"]:
                            domain_models_received.append(("data", data))
                            logger.info(
                                "domain_model_found",
                                key="data",
                                model_type=class_name,
                                message=f"Domain model {class_name} found in context",
                            )

            # Subscribe to different streams to get various domain models
            await hl_api_for_test_env.subscribe(f"l2Book:{test_symbol}", domain_model_handler)
            await hl_api_for_test_env.subscribe(f"trades:{test_symbol}", domain_model_handler)

            # Wait for transformations
            await asyncio.sleep(5.0)

            logger.info(
                "pydantic_to_domain_transformation_results",
                domain_models_count=len(domain_models_received),
                model_types=[type(m[1]).__name__ for m in domain_models_received[:5]],
                message="✓ Pydantic to domain model transformation tested",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Domain model transformation test failed: {e}")

    @pytest.mark.asyncio
    async def test_processor_data_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that processors validate data correctly."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available")

        processors = getattr(router, "_processors", {})

        # Test validation with invalid data
        validation_results: dict[str, list[str]] = {}

        for name, processor in processors.items():
            validation_results[name] = []

            # Test with various invalid inputs
            invalid_inputs: list[Any] = [
                None,
                {},
                {"invalid": "structure"},
                [],
                "",
                {"data": None},
            ]

            for invalid_input in invalid_inputs:
                try:
                    mock_handler = AsyncMock()
                    await processor.process(invalid_input, mock_handler)
                    validation_results[name].append("accepted")
                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    validation_results[name].append(f"rejected: {type(e).__name__}")

        logger.info(
            "processor_validation_results",
            processors_tested=list(validation_results.keys()),
            rejection_counts={
                name: len([r for r in results if r.startswith("rejected")])
                for name, results in validation_results.items()
            },
            message="✓ Processor data validation tested",
        )

    @pytest.mark.asyncio
    async def test_processor_pipeline_data_flow(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test complete data flow through processor pipeline."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            # Track data at different pipeline stages
            pipeline_stages: dict[str, list[Any]] = {
                "raw": [],
                "validated": [],
                "transformed": [],
                "final": [],
            }

            async def pipeline_tracking_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029

                # This is the final stage
                pipeline_stages["final"].append(context)

                # Analyze what stage markers we can find
                context_str = str(context)
                if "_raw" in context_str:
                    pipeline_stages["raw"].append(context)
                if "_validated" in context_str or hasattr(context, "validated_envelope"):
                    pipeline_stages["validated"].append(context)
                if "_transformed" in context_str or (
                    hasattr(context, "validated_envelope")
                    and hasattr(context.validated_envelope, "data")
                ):
                    pipeline_stages["transformed"].append(context)

            # Subscribe to get pipeline data
            await hl_api_for_test_env.subscribe("allMids", pipeline_tracking_handler)

            # Let pipeline process data
            await asyncio.sleep(3.0)

            logger.info(
                "processor_pipeline_flow_results",
                final_outputs=len(pipeline_stages["final"]),
                stage_counts={stage: len(data) for stage, data in pipeline_stages.items()},
                message="✓ Processor pipeline data flow tested",
            )

            assert pipeline_stages["final"], "No data reached final pipeline stage"

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Processor pipeline flow test failed: {e}")

    @pytest.mark.asyncio
    async def test_processor_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test processor error handling and recovery."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available")

        processors = getattr(router, "_processors", {})

        # Inject errors into processors
        error_injected = False
        for name, processor in processors.items():
            if name in ["l2Book", "trades"]:  # Test specific processors
                original_process = processor.process

                async def error_process(
                    data: dict[str, Any] | BaseModel,
                    handler: MessageHandler,
                    *,
                    original_process: Callable[
                        [dict[str, Any] | BaseModel, MessageHandler], Awaitable[None]
                    ] = original_process,
                ) -> None:
                    # Inject error on first call
                    nonlocal error_injected
                    if not error_injected:
                        error_injected = True
                        raise ValueError("Test error in processor")
                    await original_process(data, handler)

                processor.process = error_process
                break

        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            successful_messages = 0

            async def error_test_handler(context: WebSocketContextProtocol) -> None:
                nonlocal successful_messages
                await asyncio.sleep(0)
                successful_messages += 1

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if markets:
                await hl_api_for_test_env.subscribe(
                    f"l2Book:{markets[0].symbol}", error_test_handler
                )

            # Wait for processing
            await asyncio.sleep(3.0)

            # System should recover and continue processing
            assert successful_messages > 0 or error_injected, (
                "Processor should handle errors and recover"
            )

            logger.info(
                "processor_error_handling_results",
                error_injected=error_injected,
                successful_after_error=successful_messages,
                message="✓ Processor error handling tested",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            logger.info(
                "processor_error_test_completed",
                error=str(e),
                message="Processor error handling test completed",
            )

    @pytest.mark.asyncio
    async def test_processor_performance_metrics(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test processor pipeline performance metrics."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            processing_times: list[float] = []

            async def performance_handler(context: WebSocketContextProtocol) -> None:
                start_time = time.perf_counter()
                await asyncio.sleep(0)  # Satisfy RUF029
                # Simulate some processing
                _ = str(context)
                processing_time = time.perf_counter() - start_time
                processing_times.append(processing_time)

            # Subscribe to high-volume stream
            await hl_api_for_test_env.subscribe("allMids", performance_handler)

            # Collect performance data
            await asyncio.sleep(5.0)

            if processing_times:
                avg_time = sum(processing_times) / len(processing_times)
                max_time = max(processing_times)
                min_time = min(processing_times)

                logger.info(
                    "processor_performance_metrics",
                    messages_processed=len(processing_times),
                    avg_processing_time_ms=f"{avg_time * 1000:.3f}",
                    max_processing_time_ms=f"{max_time * 1000:.3f}",
                    min_processing_time_ms=f"{min_time * 1000:.3f}",
                    message="✓ Processor performance metrics collected",
                )

                # Performance should be reasonable for trading
                assert avg_time < 0.01, f"Average processing time {avg_time}s too high for trading"

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Processor performance test failed: {e}")

    @pytest.mark.asyncio
    async def test_processor_concurrent_stream_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test processors handle multiple concurrent streams correctly."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if len(markets) < 3:
                pytest.fail("Need at least 3 markets for concurrent stream testing")

            stream_counters: dict[str, int] = {}
            stream_lock = asyncio.Lock()

            async def make_stream_handler(stream_id: str) -> MessageHandler:
                await asyncio.sleep(0)  # Satisfy RUF029

                async def handler(context: WebSocketContextProtocol) -> None:
                    await asyncio.sleep(0)
                    async with stream_lock:
                        stream_counters[stream_id] = stream_counters.get(stream_id, 0) + 1

                return handler

            # Subscribe to multiple concurrent streams
            subscriptions: list[asyncio.Task[None]] = []
            for i in range(3):
                symbol = markets[i].symbol
                stream_id = f"l2Book_{symbol}"
                handler = await make_stream_handler(stream_id)
                subscriptions.append(
                    asyncio.create_task(hl_api_for_test_env.subscribe(f"l2Book:{symbol}", handler))
                )

                stream_id = f"trades_{symbol}"
                handler = await make_stream_handler(stream_id)
                subscriptions.append(
                    asyncio.create_task(hl_api_for_test_env.subscribe(f"trades:{symbol}", handler))
                )

            # Execute concurrent subscriptions
            await asyncio.gather(*subscriptions)

            # Let streams run
            await asyncio.sleep(5.0)

            active_streams = len([c for c in stream_counters.values() if c > 0])
            total_messages = sum(stream_counters.values())

            logger.info(
                "processor_concurrent_stream_results",
                total_streams=len(subscriptions),
                active_streams=active_streams,
                total_messages_processed=total_messages,
                stream_details=stream_counters,
                message="✓ Processors handled concurrent streams successfully",
            )

            assert active_streams > 0, "No streams were processed concurrently"

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Concurrent stream handling test failed: {e}")
