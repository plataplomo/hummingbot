"""Unit tests for the core OrderBook model.

Tests validation, parsing, immutability, and level structure validation.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.core.symbols import Symbol
from cyberdelta.exceptions.field_validation import ListFieldError
from cyberdelta.models.market.order_book import OrderBook
from tests.common_symbols import BTC_HL, ETH_HL


pytestmark = pytest.mark.timing


class TestOrderBook:
    """Unit tests for the cyberdelta.core.models.market.order_book.OrderBook model."""

    @pytest.fixture
    def btc_symbol(self) -> Symbol:
        """Fixture providing a BTC exchange symbol."""
        return BTC_HL

    @pytest.fixture
    def eth_symbol(self) -> Symbol:
        """Fixture providing an ETH exchange symbol."""
        return ETH_HL

    def test_minimal_creation(self, btc_symbol: Symbol) -> None:
        """Test creating an OrderBook with minimal valid data (empty bids/asks)."""
        now = datetime.now(UTC)
        ob = OrderBook(symbol=btc_symbol, timestamp=now, bids=[], asks=[])
        assert ob.symbol == btc_symbol
        assert ob.timestamp == now
        assert ob.bids == []
        assert ob.asks == []

    def test_creation_with_levels(self, btc_symbol: Symbol) -> None:
        """Test creating an OrderBook with valid bid/ask levels."""
        now = datetime.now(UTC)
        bids: list[tuple[Any, Any]] = [("50000.0", "1.5"), (Decimal("49999.5"), 2.0)]  # Mix types
        asks: list[tuple[Any, Any]] = [(Decimal("50000.5"), 1), ("50001.0", "0.5")]  # Mix types
        expected_bids = [(Decimal("50000.0"), Decimal("1.5")), (Decimal("49999.5"), Decimal("2.0"))]
        expected_asks = [(Decimal("50000.5"), Decimal("1.0")), (Decimal("50001.0"), Decimal("0.5"))]

        ob = OrderBook(symbol=btc_symbol, timestamp=now, bids=bids, asks=asks)
        assert ob.bids == expected_bids
        assert ob.asks == expected_asks

    def test_required_fields(self, btc_symbol: Symbol) -> None:
        """Test that required fields (symbol, timestamp, bids, asks) raise errors if missing."""
        now = datetime.now(UTC)
        # Pydantic raises ValidationError if fields are missing entirely
        with pytest.raises(ValidationError, match="Field required"):
            OrderBook(timestamp=now, bids=[], asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Field required"):
            OrderBook(symbol=btc_symbol, bids=[], asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Field required"):
            OrderBook(symbol=btc_symbol, timestamp=now, asks=[])  # type: ignore[call-arg]
        with pytest.raises(ValidationError, match="Field required"):
            OrderBook(symbol=btc_symbol, timestamp=now, bids=[])  # type: ignore[call-arg]

    def test_symbol_validation(self, btc_symbol: Symbol) -> None:
        """Test validation that symbol must be an Symbol."""
        now = datetime.now(UTC)

        # Test that string symbols are rejected
        with pytest.raises(ValidationError):
            OrderBook(symbol="BTC-PERP", timestamp=now, bids=[], asks=[])  # type: ignore[arg-type]

        # Test that None is rejected
        with pytest.raises(ValidationError):
            OrderBook(symbol=None, timestamp=now, bids=[], asks=[])  # type: ignore[arg-type]

        # Test that other types are rejected
        with pytest.raises(ValidationError):
            OrderBook(symbol=123, timestamp=now, bids=[], asks=[])  # type: ignore[arg-type]

    def test_timestamp_validation(self, btc_symbol: Symbol) -> None:
        """Test validation rules for the timestamp field."""
        # Valid: timezone-aware datetime
        OrderBook(symbol=btc_symbol, timestamp=datetime.now(UTC), bids=[], asks=[])

        # Invalid: None
        with pytest.raises(ValidationError):
            OrderBook(symbol=btc_symbol, timestamp=None, bids=[], asks=[])  # type: ignore[arg-type]

        # Invalid: string
        with pytest.raises(ValidationError):
            OrderBook(symbol=btc_symbol, timestamp="2024-01-01", bids=[], asks=[])  # type: ignore[arg-type]

    def test_invalid_bid_ask_structure(self, btc_symbol: Symbol) -> None:
        """Test that invalid bid/ask structures are rejected."""
        now = datetime.now(UTC)

        # Invalid: bids not a list
        with pytest.raises(ListFieldError, match="bids"):
            OrderBook(symbol=btc_symbol, timestamp=now, bids="invalid", asks=[])  # type: ignore[arg-type]

        # Invalid: asks not a list
        with pytest.raises(ListFieldError, match="asks"):
            OrderBook(symbol=btc_symbol, timestamp=now, bids=[], asks="invalid")  # type: ignore[arg-type]

    def test_invalid_level_structure(self, btc_symbol: Symbol) -> None:
        """Test that invalid level structures within bids/asks are rejected."""
        now = datetime.now(UTC)

        # Invalid: Not a tuple
        with pytest.raises(ValidationError):
            OrderBook(symbol=btc_symbol, timestamp=now, bids=["50000"], asks=[])  # type: ignore[list-item]

        # Invalid: Wrong tuple length
        with pytest.raises(ValidationError):
            OrderBook(symbol=btc_symbol, timestamp=now, bids=[("50000",)], asks=[])  # type: ignore[list-item]

        # Invalid: Wrong tuple length (too many)
        with pytest.raises(ValidationError):
            OrderBook(symbol=btc_symbol, timestamp=now, bids=[("50000", "1", "extra")], asks=[])  # type: ignore[list-item]

    def test_decimal_parsing_in_levels(self, btc_symbol: Symbol) -> None:
        """Test that various numeric types are parsed to Decimal in bid/ask levels."""
        now = datetime.now(UTC)

        # Test various input types
        bids: list[tuple[Any, Any]] = [
            ("100.5", "10"),  # strings
            (100.5, 10),  # floats
            (Decimal("100.5"), Decimal(10)),  # decimals
            (100, 10),  # ints
        ]

        ob = OrderBook(symbol=btc_symbol, timestamp=now, bids=bids, asks=[])

        # All should be converted to Decimal
        for price, quantity in ob.bids:
            assert isinstance(price, Decimal)
            assert isinstance(quantity, Decimal)

    def test_immutability(self, btc_symbol: Symbol, eth_symbol: Symbol) -> None:
        """Test that OrderBook is immutable (frozen=True)."""
        ob = OrderBook(
            symbol=btc_symbol,
            timestamp=datetime.now(UTC),
            bids=[(Decimal(50000), Decimal(1))],
            asks=[(Decimal(50001), Decimal(1))],
        )

        # Attempt to modify fields should raise ValidationError
        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.symbol = eth_symbol

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.timestamp = datetime.now(UTC)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.bids = []

        with pytest.raises(ValidationError, match="Instance is frozen"):
            ob.asks = []

    def test_extra_fields_forbidden(self, btc_symbol: Symbol) -> None:
        """Test that extra fields are forbidden (extra='forbid')."""
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            OrderBook(
                symbol=btc_symbol,
                timestamp=datetime.now(UTC),
                bids=[],
                asks=[],
                extra_field="not_allowed",  # type: ignore[call-arg]
            )

    def test_serialization(self, btc_symbol: Symbol) -> None:
        """Test that OrderBook can be serialized to dict/JSON."""
        now = datetime.now(UTC)
        ob = OrderBook(
            symbol=btc_symbol,
            timestamp=now,
            bids=[(Decimal(50000), Decimal("1.5"))],
            asks=[(Decimal(50001), Decimal("2.0"))],
        )

        # Test model_dump
        data = ob.model_dump()
        assert data["symbol"] == btc_symbol.model_dump()
        assert data["timestamp"] == now
        assert data["bids"] == [("50000", "1.5")]  # Decimals serialized as strings
        assert data["asks"] == [("50001", "2.0")]

        # Test JSON serialization
        json_str = ob.model_dump_json()
        assert isinstance(json_str, str)
        assert "50000" in json_str
