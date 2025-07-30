"""Unit tests for the core FundingRate model and its Details sub-models.

Tests validation, parsing, immutability, and the Core+Details pattern.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.core.models.market.funding_rate import (
    BackpackFundingDetails,
    FundingRate,
    HyperliquidFundingDetails,
)
from cyberdelta.core.symbols.models import ExchangeSymbol
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from tests.factories.symbol_factories import ExchangeSymbolFactory


pytestmark = pytest.mark.timing


class TestFundingRate:
    """Test cases for the core FundingRate model."""

    @pytest.fixture
    def btc_symbol(self) -> ExchangeSymbol:
        """Fixture providing a BTC exchange symbol."""
        return ExchangeSymbolFactory.create_hyperliquid_btc_perp()

    @pytest.fixture
    def eth_symbol(self) -> ExchangeSymbol:
        """Fixture providing an ETH exchange symbol."""
        return ExchangeSymbolFactory.create_hyperliquid_eth_perp()

    def test_core_required_fields(self, btc_symbol: ExchangeSymbol) -> None:
        """Test that required fields are actually required."""
        # Symbol and timestamp are required
        with pytest.raises(ValidationError, match="1 validation error"):
            # We intentionally omit required 'symbol' to test validation
            kwargs1: dict[str, Any] = {"timestamp": datetime.now(UTC)}
            FundingRate(**kwargs1)

        with pytest.raises(ValidationError, match="1 validation error"):
            # We intentionally omit required 'timestamp' to test validation
            kwargs2: dict[str, Any] = {"symbol": btc_symbol}
            FundingRate(**kwargs2)

        # Both required fields present should succeed
        fr = FundingRate(symbol=btc_symbol, timestamp=datetime.now(UTC))
        assert fr.symbol == btc_symbol
        assert isinstance(fr.timestamp, datetime)

    def test_core_minimal_creation(self, btc_symbol: ExchangeSymbol) -> None:
        """Test creating a minimal FundingRate with only required fields."""
        now = datetime.now(UTC)
        fr = FundingRate(symbol=btc_symbol, timestamp=now)

        assert fr.symbol == btc_symbol
        assert fr.timestamp == now
        assert fr.funding_rate is None
        assert fr.predicted_rate is None
        assert fr.mark_price is None
        assert fr.index_price is None
        assert fr.next_funding_time is None
        assert fr.hl_details is None
        assert fr.bp_details is None

    def test_core_complete_creation(self, btc_symbol: ExchangeSymbol) -> None:
        """Test creating a FundingRate with all core fields."""
        now = datetime.now(UTC)
        next_time = now + timedelta(hours=8)

        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            predicted_rate=Decimal("0.00015"),
            mark_price=Decimal("50000.00"),
            index_price=Decimal("49950.00"),
            next_funding_time=next_time,
        )

        assert fr.symbol == btc_symbol
        assert fr.timestamp == now
        assert fr.funding_rate == Decimal("0.0001")
        assert fr.predicted_rate == Decimal("0.00015")
        assert fr.mark_price == Decimal("50000.00")
        assert fr.index_price == Decimal("49950.00")
        assert fr.next_funding_time == next_time

    def test_decimal_parsing(self, btc_symbol: ExchangeSymbol) -> None:
        """Test that numeric fields are parsed to Decimal correctly."""
        now = datetime.now(UTC)

        # Test various numeric input types
        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate="0.0001",  # string
            predicted_rate=0.00015,  # float
            mark_price=50000,  # int
            index_price="49950.00",  # string
        )

        # All should be converted to Decimal
        assert isinstance(fr.funding_rate, Decimal)
        assert isinstance(fr.predicted_rate, Decimal)
        assert isinstance(fr.mark_price, Decimal)
        assert isinstance(fr.index_price, Decimal)

        assert fr.funding_rate == Decimal("0.0001")
        assert fr.predicted_rate == Decimal("0.00015")
        assert fr.mark_price == Decimal("50000")
        assert fr.index_price == Decimal("49950.00")

    def test_symbol_validation(self, btc_symbol: ExchangeSymbol) -> None:
        """Test validation rules for the symbol field."""
        now = datetime.now(UTC)

        # Valid symbol should pass
        FundingRate(symbol=btc_symbol, timestamp=now)

        # Test that string symbols are rejected
        with pytest.raises(ValidationError):
            FundingRate(symbol="BTC-PERP", timestamp=now)  # type: ignore[arg-type]

    def test_timestamp_parsing(self, btc_symbol: ExchangeSymbol) -> None:
        """Test timestamp parsing from various formats."""
        ms_timestamp = 1678881600000  # 2023-03-15 12:00:00 UTC
        iso_timestamp = "2023-03-15T12:00:00Z"
        naive_dt = datetime(2023, 3, 15, 12, 0, 0)
        aware_dt = datetime(2023, 3, 15, 12, 0, 0, tzinfo=UTC)
        expected_dt = aware_dt

        # Test integer timestamp parsing
        kwargs_int: dict[str, Any] = {
            "symbol": btc_symbol,
            "timestamp": ms_timestamp,
        }
        fr_int = FundingRate(**kwargs_int)
        assert fr_int.timestamp == expected_dt

        # Test ISO string timestamp parsing
        kwargs_iso: dict[str, Any] = {
            "symbol": btc_symbol,
            "timestamp": iso_timestamp,
        }
        fr_iso = FundingRate(**kwargs_iso)
        assert fr_iso.timestamp == expected_dt

        # From naive datetime
        fr_naive = FundingRate(symbol=btc_symbol, timestamp=naive_dt)
        assert fr_naive.timestamp == expected_dt  # Should be made UTC aware

        # From aware datetime
        fr_aware = FundingRate(symbol=btc_symbol, timestamp=aware_dt)
        assert fr_aware.timestamp == expected_dt

    def test_hyperliquid_details_creation(self, btc_symbol: ExchangeSymbol) -> None:
        """Test creating FundingRate with Hyperliquid-specific details."""
        now = datetime.now(UTC)

        # Create with minimal HyperliquidFundingDetails
        hl_details = HyperliquidFundingDetails(
            vault_apr=Decimal("0.05"),
            premium=Decimal("0.0001"),
        )

        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            hl_details=hl_details,
        )

        assert fr.hl_details == hl_details
        assert fr.hl_details.vault_apr == Decimal("0.05")
        assert fr.hl_details.premium == Decimal("0.0001")
        assert fr.hl_details.open_interest is None
        assert fr.bp_details is None  # Should be exclusive

    def test_backpack_details_creation(self, btc_symbol: ExchangeSymbol) -> None:
        """Test creating FundingRate with Backpack-specific details."""
        now = datetime.now(UTC)

        # Create with minimal BackpackFundingDetails
        bp_details = BackpackFundingDetails(
            apr_24h=Decimal("0.0365"),
            apy_24h=Decimal("0.0372"),
        )

        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            bp_details=bp_details,
        )

        assert fr.bp_details == bp_details
        assert fr.bp_details.apr_24h == Decimal("0.0365")
        assert fr.bp_details.apy_24h == Decimal("0.0372")
        assert fr.bp_details.apr_7d is None
        assert fr.hl_details is None  # Should be exclusive

    def test_both_details_exclusive(self, btc_symbol: ExchangeSymbol) -> None:
        """Test that hl_details and bp_details are mutually exclusive."""
        now = datetime.now(UTC)

        hl_details = HyperliquidFundingDetails(
            vault_apr=Decimal("0.05"),
            premium=Decimal("0.0001"),
        )

        bp_details = BackpackFundingDetails(
            apr_24h=Decimal("0.0365"),
            apy_24h=Decimal("0.0372"),
        )

        with pytest.raises(ValidationError, match="Cannot have both"):
            FundingRate(
                symbol=btc_symbol,
                timestamp=now,
                hl_details=hl_details,
                bp_details=bp_details,
            )

    def test_immutability(self, btc_symbol: ExchangeSymbol, eth_symbol: ExchangeSymbol) -> None:
        """Test that FundingRate instances are immutable."""
        now = datetime.now(UTC)
        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate=Decimal("0.0001"),
        )

        # Attempt to modify fields should raise ValidationError
        with pytest.raises(ValidationError, match="Instance is frozen"):
            fr.symbol = eth_symbol

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fr.funding_rate = Decimal("0.0002")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            fr.timestamp = datetime.now(UTC)

    def test_extra_fields_forbidden(self, btc_symbol: ExchangeSymbol) -> None:
        """Test that extra fields are forbidden."""
        now = datetime.now(UTC)

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            FundingRate(
                symbol=btc_symbol,
                timestamp=now,
                funding_rate=Decimal("0.0001"),
                extra_field="not_allowed",  # type: ignore[call-arg]
            )

    def test_serialization(self, btc_symbol: ExchangeSymbol) -> None:
        """Test that FundingRate can be serialized properly."""
        now = datetime.now(UTC)
        next_time = now + timedelta(hours=8)

        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            next_funding_time=next_time,
        )

        # Test model_dump
        data = fr.model_dump()
        assert data["symbol"] == btc_symbol.model_dump()
        assert data["timestamp"] == now
        assert data["funding_rate"] == "0.0001"  # Decimal serializes to string
        assert data["next_funding_time"] == next_time

        # Test model_dump_json
        json_str = fr.model_dump_json()
        assert isinstance(json_str, str)
        assert "0.0001" in json_str

    def test_funding_rate_with_all_prices(self, btc_symbol: ExchangeSymbol) -> None:
        """Test FundingRate with complete price information."""
        now = datetime.now(UTC)

        fr = FundingRate(
            symbol=btc_symbol,
            timestamp=now,
            funding_rate=Decimal("0.0001"),
            predicted_rate=Decimal("0.00015"),
            mark_price=Decimal("50000.00"),
            index_price=Decimal("49950.00"),
        )

        # Calculate price difference
        price_diff = fr.mark_price - fr.index_price  # type: ignore[operator]
        assert price_diff == Decimal("50.00")

        # Verify all fields
        assert fr.funding_rate == Decimal("0.0001")
        assert fr.predicted_rate == Decimal("0.00015")
        assert fr.mark_price == Decimal("50000.00")
        assert fr.index_price == Decimal("49950.00")


class TestHyperliquidFundingDetails:
    """Test cases for Hyperliquid-specific funding details."""

    def test_minimal_creation(self) -> None:
        """Test creating minimal HyperliquidFundingDetails."""
        details = HyperliquidFundingDetails(
            vault_apr=Decimal("0.05"),
            premium=Decimal("0.0001"),
        )

        assert details.vault_apr == Decimal("0.05")
        assert details.premium == Decimal("0.0001")
        assert details.open_interest is None
        assert details.day_ntl_vlm is None

    def test_complete_creation(self) -> None:
        """Test creating complete HyperliquidFundingDetails."""
        details = HyperliquidFundingDetails(
            vault_apr=Decimal("0.05"),
            premium=Decimal("0.0001"),
            open_interest=Decimal("1000000"),
            day_ntl_vlm=Decimal("5000000"),
        )

        assert details.vault_apr == Decimal("0.05")
        assert details.premium == Decimal("0.0001")
        assert details.open_interest == Decimal("1000000")
        assert details.day_ntl_vlm == Decimal("5000000")

    def test_decimal_parsing(self) -> None:
        """Test decimal parsing for all numeric fields."""
        details = HyperliquidFundingDetails(
            vault_apr="0.05",  # string
            premium=0.0001,  # float
            open_interest=1000000,  # int
            day_ntl_vlm="5000000.00",  # string
        )

        assert isinstance(details.vault_apr, Decimal)
        assert isinstance(details.premium, Decimal)
        assert isinstance(details.open_interest, Decimal)
        assert isinstance(details.day_ntl_vlm, Decimal)

    def test_immutability(self) -> None:
        """Test that HyperliquidFundingDetails is immutable."""
        details = HyperliquidFundingDetails(
            vault_apr=Decimal("0.05"),
            premium=Decimal("0.0001"),
        )

        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.vault_apr = Decimal("0.06")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.premium = Decimal("0.0002")


class TestBackpackFundingDetails:
    """Test cases for Backpack-specific funding details."""

    def test_minimal_creation(self) -> None:
        """Test creating minimal BackpackFundingDetails."""
        details = BackpackFundingDetails(
            apr_24h=Decimal("0.0365"),
            apy_24h=Decimal("0.0372"),
        )

        assert details.apr_24h == Decimal("0.0365")
        assert details.apy_24h == Decimal("0.0372")
        assert details.apr_7d is None
        assert details.apy_7d is None

    def test_complete_creation(self) -> None:
        """Test creating complete BackpackFundingDetails."""
        details = BackpackFundingDetails(
            apr_24h=Decimal("0.0365"),
            apy_24h=Decimal("0.0372"),
            apr_7d=Decimal("0.0380"),
            apy_7d=Decimal("0.0387"),
        )

        assert details.apr_24h == Decimal("0.0365")
        assert details.apy_24h == Decimal("0.0372")
        assert details.apr_7d == Decimal("0.0380")
        assert details.apy_7d == Decimal("0.0387")

    def test_decimal_parsing(self) -> None:
        """Test decimal parsing for all numeric fields."""
        details = BackpackFundingDetails(
            apr_24h="0.0365",  # string
            apy_24h=0.0372,  # float
            apr_7d=Decimal("0.0380"),  # already Decimal
            apy_7d="0.0387",  # string
        )

        assert isinstance(details.apr_24h, Decimal)
        assert isinstance(details.apy_24h, Decimal)
        assert isinstance(details.apr_7d, Decimal)
        assert isinstance(details.apy_7d, Decimal)

    def test_immutability(self) -> None:
        """Test that BackpackFundingDetails is immutable."""
        details = BackpackFundingDetails(
            apr_24h=Decimal("0.0365"),
            apy_24h=Decimal("0.0372"),
        )

        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.apr_24h = Decimal("0.04")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            details.apy_24h = Decimal("0.041")
