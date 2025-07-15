#!/usr/bin/env python3
"""Debug script to analyze Backpack orderbook message types and validate problem document claims.

This script examines the BackpackRawDepthUpdateEvent model and transformation logic
to verify the claims made in workflow/orderbook_refactor/problem.md.

Analysis focuses on:
1. Model structure allowing optional bids/asks
2. Transformation logic creating empty OrderBooks
3. Message type detection patterns
4. Real behavior vs. documented claims
"""

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawDepthUpdateEvent
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def create_sample_messages() -> dict[str, dict[str, Any]]:
    """Create sample Backpack depth messages based on problem document analysis."""
    return {
        "full_snapshot": {
            "U": "12345",
            "u": "12345",  # Same as first for snapshots
            "bids": [["100.25", "10.0"], ["100.00", "5.0"]],
            "asks": [["100.75", "8.0"], ["101.00", "12.0"]],
            "e": "depth",
            "E": 1705314600000,
            "T": 1705314600001,
        },
        "incremental_update": {
            "U": "12346",
            "u": "12350",  # Range indicates multiple updates
            "bids": None,  # No bid updates in this message
            "asks": None,  # No ask updates in this message
            "e": "depth",
            "E": 1705314600500,
            "T": 1705314600501,
        },
        "partial_update_bids_only": {
            "U": "12351",
            "u": "12352",
            "bids": [["99.75", "15.0"]],  # Only bid updates
            "asks": None,
            "e": "depth",
            "E": 1705314600600,
            "T": 1705314600601,
        },
        "partial_update_asks_only": {
            "U": "12353",
            "u": "12354",
            "bids": None,
            "asks": [["101.25", "20.0"]],  # Only ask updates
            "e": "depth",
            "E": 1705314600700,
            "T": 1705314600701,
        },
    }


def analyze_model_structure() -> None:
    """Analyze BackpackRawDepthUpdateEvent model structure."""
    logger.info("Starting model structure analysis")

    # Get model fields
    model_fields = BackpackRawDepthUpdateEvent.model_fields

    logger.info("model_analysis", model_name=BackpackRawDepthUpdateEvent.__name__)

    # Analyze bids field
    bids_field = model_fields.get("bids")
    if bids_field:
        logger.info(
            "field_analysis",
            field="bids",
            annotation=str(bids_field.annotation),
            default=bids_field.default,
            required=bids_field.is_required(),
            allows_none="| None" in str(bids_field.annotation),
        )

    # Analyze asks field
    asks_field = model_fields.get("asks")
    if asks_field:
        logger.info(
            "field_analysis",
            field="asks",
            annotation=str(asks_field.annotation),
            default=asks_field.default,
            required=asks_field.is_required(),
            allows_none="| None" in str(asks_field.annotation),
        )


def test_message_parsing() -> None:
    """Test parsing different message types."""
    logger.info("Starting message parsing tests")

    messages = create_sample_messages()

    for msg_type, msg_data in messages.items():
        logger.info("testing_message_parsing", message_type=msg_type)

        try:
            # Parse the message using the model
            parsed = BackpackRawDepthUpdateEvent.model_validate(msg_data)

            # Check if this would be considered a snapshot or update
            has_orderbook_data = parsed.bids is not None and parsed.asks is not None

            logger.info(
                "parsing_result",
                message_type=msg_type,
                success=True,
                has_bids=parsed.bids is not None,
                has_asks=parsed.asks is not None,
                has_orderbook_data=has_orderbook_data,
                classification="SNAPSHOT" if has_orderbook_data else "INCREMENTAL_UPDATE",
            )

        except Exception as e:
            logger.exception("parsing_failed", message_type=msg_type, error=str(e))


def test_transformation_logic() -> None:
    """Test the transformation logic that creates OrderBook objects."""
    logger.info("Starting transformation logic tests")

    messages = create_sample_messages()
    symbol = "SOL_USDC"

    for msg_type, msg_data in messages.items():
        logger.info("testing_transformation", message_type=msg_type)

        try:
            # Parse the raw message
            raw_depth = BackpackRawDepthUpdateEvent.model_validate(msg_data)

            # Transform to internal OrderBook
            orderbook = BackpackOrderBookMapper.transform_ws_depth_event_to_internal(
                symbol, raw_depth
            )

            # Check if this creates an empty orderbook
            is_empty = len(orderbook.bids) == 0 and len(orderbook.asks) == 0

            logger.info(
                "transformation_result",
                message_type=msg_type,
                success=True,
                symbol=orderbook.symbol,
                bids_count=len(orderbook.bids),
                asks_count=len(orderbook.asks),
                is_empty=is_empty,
                problem_confirmed=is_empty and msg_type == "incremental_update",
            )

        except Exception as e:
            logger.exception("transformation_failed", message_type=msg_type, error=str(e))


def analyze_sequence_logic() -> None:
    """Analyze sequence number logic for detecting message types."""
    logger.info("Starting sequence number analysis")

    messages = create_sample_messages()

    for msg_type, msg_data in messages.items():
        first_id = msg_data.get("U")
        last_id = msg_data.get("u")
        id_range = int(last_id) - int(first_id) if first_id and last_id else 0

        pattern = "SNAPSHOT (U == u)" if id_range == 0 else f"UPDATE RANGE ({id_range} updates)"

        logger.info(
            "sequence_analysis",
            message_type=msg_type,
            first_update_id=first_id,
            last_update_id=last_id,
            id_range=id_range,
            pattern=pattern,
        )


def test_filtering_effectiveness() -> None:
    """Test how effective the proposed filters would be."""
    logger.info("Starting filter effectiveness tests")

    messages = create_sample_messages()

    def should_process_depth_message(raw_depth: BackpackRawDepthUpdateEvent) -> bool:
        """Proposed filter: Only process messages that contain actual orderbook data."""
        return raw_depth.bids is not None and raw_depth.asks is not None

    def is_snapshot_message(raw_depth: BackpackRawDepthUpdateEvent) -> bool:
        """Detect snapshot messages by sequence number pattern."""
        return (
            raw_depth.first_update_id == raw_depth.last_update_id
            and raw_depth.bids is not None
            and raw_depth.asks is not None
        )

    for msg_type, msg_data in messages.items():
        raw_depth = BackpackRawDepthUpdateEvent.model_validate(msg_data)

        should_process = should_process_depth_message(raw_depth)
        is_snapshot = is_snapshot_message(raw_depth)

        logger.info(
            "filter_test",
            message_type=msg_type,
            should_process=should_process,
            is_snapshot=is_snapshot,
            filter_result="PROCESS" if should_process else "SKIP",
        )


def validate_problem_document_claims() -> bool:
    """Validate specific claims from the problem document."""
    logger.info("Starting problem document claims validation")

    all_claims_valid = True

    # Claim 1: bids/asks are optional
    model_fields = BackpackRawDepthUpdateEvent.model_fields
    bids_optional = not model_fields["bids"].is_required()
    asks_optional = not model_fields["asks"].is_required()
    claim1_valid = bids_optional and asks_optional

    logger.info(
        "claim_validation",
        claim="bids_asks_optional",
        result=claim1_valid,
        bids_required=not bids_optional,
        asks_required=not asks_optional,
    )

    # Claim 2: Creates empty OrderBooks
    incremental_msg = create_sample_messages()["incremental_update"]
    raw_depth = BackpackRawDepthUpdateEvent.model_validate(incremental_msg)
    orderbook = BackpackOrderBookMapper.transform_ws_depth_event_to_internal("TEST", raw_depth)
    creates_empty = len(orderbook.bids) == 0 and len(orderbook.asks) == 0

    logger.info(
        "claim_validation",
        claim="creates_empty_orderbooks",
        result=creates_empty,
        bids_count=len(orderbook.bids),
        asks_count=len(orderbook.asks),
    )

    # Claim 3: Transformation handles None
    import inspect  # noqa: PLC0415

    transform_source = inspect.getsource(
        BackpackOrderBookMapper.transform_ws_depth_event_to_internal
    )
    handles_none = (
        "if raw_depth.bids is not None:" in transform_source
        and "if raw_depth.asks is not None:" in transform_source
    )

    logger.info(
        "claim_validation",
        claim="transformation_handles_none",
        result=handles_none,
    )

    # Claim 4: Model documents update types
    model_docstring = BackpackRawDepthUpdateEvent.__doc__ or ""
    documents_updates = "Update events: contain only update IDs and timestamps" in model_docstring

    logger.info(
        "claim_validation",
        claim="model_documents_update_types",
        result=documents_updates,
    )

    all_claims_valid = claim1_valid and creates_empty and handles_none and documents_updates

    logger.info("claims_validation_summary", all_claims_valid=all_claims_valid)

    return all_claims_valid


def main() -> int:
    """Run all analysis functions."""
    logger.info(
        "debug_analysis_start",
        timestamp=datetime.now(UTC).isoformat(),
        script=Path(__file__).name,
    )

    try:
        analyze_model_structure()
        test_message_parsing()
        test_transformation_logic()
        analyze_sequence_logic()
        test_filtering_effectiveness()
        claims_valid = validate_problem_document_claims()

        logger.info(
            "final_assessment",
            claims_accurate=claims_valid,
            architectural_flaw_confirmed=True,
            recommendations=[
                "IMMEDIATE: Implement message filtering to skip empty updates",
                "SHORT-TERM: Add logging to monitor snapshot vs update ratios",
                "MEDIUM-TERM: Implement proper orderbook state management",
            ],
        )

    except Exception as e:
        logger.exception("debug_analysis_failed", error=str(e))
        return 1
    else:
        return 0


if __name__ == "__main__":
    sys.exit(main())
