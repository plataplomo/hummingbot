"""Unit tests for MarketOrderService."""

from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.execution.orders.errors import (
    InsufficientLiquidityError,
    PriceDeviationError,
)
from cyberdelta.core.execution.orders.market_order_config import MarketOrderConfig
from cyberdelta.core.execution.orders.market_order_service import MarketOrderService
from cyberdelta.core.models import OrderBook, OrderSide
from cyberdelta.core.models.market.mid_prices import MidPrices


class TestMarketOrderService:
    """Test cases for MarketOrderService."""

    @pytest.fixture
    def mock_exchange_api(self) -> AsyncMock:
        """Create a mock exchange API."""
        api = AsyncMock()
        api.exchange_name = "test_exchange"
        return api

    @pytest.fixture
    def mock_signal_generator(self) -> MagicMock:
        """Create a mock signal generator."""
        generator = MagicMock()
        generator.estimate_slippage.return_value = Decimal("0.002")  # 0.2% slippage
        return generator

    @pytest.fixture
    def default_config(self) -> MarketOrderConfig:
        """Create default market order config."""
        return MarketOrderConfig()

    @pytest.fixture
    def service(
        self,
        mock_exchange_api: AsyncMock,
        mock_signal_generator: MagicMock,
        default_config: MarketOrderConfig,
    ) -> MarketOrderService:
        """Create MarketOrderService instance."""
        return MarketOrderService(
            exchange_api=mock_exchange_api,
            signal_generator=mock_signal_generator,
            config=default_config,
        )

    @pytest.fixture
    def sample_order_book(self) -> OrderBook:
        """Create a sample order book with good liquidity."""
        from datetime import UTC, datetime

        return OrderBook(
            symbol="BTC",
            bids=[
                (Decimal("50000"), Decimal("10")),
                (Decimal("49995"), Decimal("20")),
                (Decimal("49990"), Decimal("30")),
            ],
            asks=[
                (Decimal("50010"), Decimal("10")),
                (Decimal("50015"), Decimal("20")),
                (Decimal("50020"), Decimal("30")),
            ],
            timestamp=datetime.now(UTC),
        )

    @pytest.fixture
    def thin_order_book(self) -> OrderBook:
        """Create a thin order book with limited liquidity."""
        from datetime import UTC, datetime

        return OrderBook(
            symbol="ILLIQUID",
            bids=[(Decimal("100"), Decimal("0.1"))],
            asks=[(Decimal("101"), Decimal("0.1"))],
            timestamp=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_calculate_aggressive_price_buy(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
        sample_order_book: OrderBook,
    ) -> None:
        """Test calculating aggressive price for buy orders."""
        mock_exchange_api.get_order_book.return_value = sample_order_book

        price = await service.calculate_aggressive_price(
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("5"),
        )

        # Best ask is 50010, with 0.2% slippage should be ~50110
        expected = Decimal("50010") * Decimal("1.002")
        assert abs(price - expected) < Decimal("1")  # Allow small rounding difference

    @pytest.mark.asyncio
    async def test_calculate_aggressive_price_sell(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
        sample_order_book: OrderBook,
    ) -> None:
        """Test calculating aggressive price for sell orders."""
        mock_exchange_api.get_order_book.return_value = sample_order_book

        price = await service.calculate_aggressive_price(
            symbol="BTC",
            side=OrderSide.SELL,
            quantity=Decimal("5"),
        )

        # Best bid is 50000, with 0.2% slippage should be ~49900
        expected = Decimal("50000") * Decimal("0.998")
        assert abs(price - expected) < Decimal("1")

    @pytest.mark.asyncio
    async def test_insufficient_liquidity_error(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
        thin_order_book: OrderBook,
    ) -> None:
        """Test error when insufficient liquidity."""
        mock_exchange_api.get_order_book.return_value = thin_order_book

        with pytest.raises(InsufficientLiquidityError) as exc_info:
            await service.calculate_aggressive_price(
                symbol="ILLIQUID",
                side=OrderSide.BUY,
                quantity=Decimal("10"),  # Requesting 10, only 0.1 available
            )

        error = exc_info.value
        assert error.symbol == "ILLIQUID"
        assert error.requested_quantity == Decimal("10")
        assert error.available_quantity == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_no_order_book_error(
        self, service: MarketOrderService, mock_exchange_api: AsyncMock
    ) -> None:
        """Test error when order book is unavailable."""
        mock_exchange_api.get_order_book.return_value = None

        with pytest.raises(APIError) as exc_info:
            await service.calculate_aggressive_price(
                symbol="BTC",
                side=OrderSide.BUY,
                quantity=Decimal("1"),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value

    @pytest.mark.asyncio
    async def test_max_slippage_override(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
        sample_order_book: OrderBook,
        mock_signal_generator: MagicMock,
    ) -> None:
        """Test max slippage override parameter."""
        mock_exchange_api.get_order_book.return_value = sample_order_book
        mock_signal_generator.estimate_slippage.return_value = Decimal("0.01")  # 1%

        # Override max slippage to 0.5%
        price = await service.calculate_aggressive_price(
            symbol="BTC",
            side=OrderSide.BUY,
            quantity=Decimal("5"),
            max_slippage=Decimal("0.005"),
        )

        # Should use 0.5% instead of 1%
        expected = Decimal("50010") * Decimal("1.005")
        assert abs(price - expected) < Decimal("1")

    @pytest.mark.asyncio
    async def test_price_deviation_error(
        self, service: MarketOrderService, mock_exchange_api: AsyncMock
    ) -> None:
        """Test error when price deviation exceeds limits."""
        # Create order book with extreme spread
        from datetime import UTC, datetime

        extreme_book = OrderBook(
            symbol="EXTREME",
            bids=[(Decimal("100"), Decimal("10"))],
            asks=[(Decimal("200"), Decimal("10"))],  # 100% spread
            timestamp=datetime.now(UTC),
        )
        mock_exchange_api.get_order_book.return_value = extreme_book

        # Configure to allow high slippage but low price deviation
        service._config = MarketOrderConfig(
            max_slippage_pct=Decimal("0.2"),  # Allow 20% slippage
            max_price_deviation_pct=Decimal("0.05"),  # But only 5% price deviation
            slippage_by_symbol={
                "EXTREME": Decimal("0.2"),  # Allow 20% for EXTREME symbol
                "default": Decimal("0.02"),
            },
        )

        # Mock signal generator to return high slippage
        if service._signal_generator:
            mock_estimate = cast(MagicMock, service._signal_generator.estimate_slippage)
            mock_estimate.return_value = Decimal("0.2")  # 20%

        with pytest.raises(PriceDeviationError) as exc_info:
            await service.calculate_aggressive_price(
                symbol="EXTREME",
                side=OrderSide.BUY,
                quantity=Decimal("1"),
            )

        error = exc_info.value
        assert error.symbol == "EXTREME"
        assert error.deviation_pct > error.max_deviation_pct

    def test_calculate_liquidity_ratio(self, service: MarketOrderService) -> None:
        """Test liquidity ratio calculation."""
        from datetime import UTC, datetime

        order_book = OrderBook(
            symbol="TEST",
            bids=[(Decimal("100"), Decimal("5"))],
            asks=[(Decimal("101"), Decimal("10"))],
            timestamp=datetime.now(UTC),
        )

        # Buy side - 10 available, requesting 5
        ratio = service.calculate_liquidity_ratio(order_book, OrderSide.BUY, Decimal("5"))
        assert ratio == Decimal("2")  # 10/5 = 2

        # Sell side - 5 available, requesting 10
        ratio = service.calculate_liquidity_ratio(order_book, OrderSide.SELL, Decimal("10"))
        assert ratio == Decimal("0.5")  # 5/10 = 0.5

    def test_estimate_slippage_fallback(self, service: MarketOrderService) -> None:
        """Test slippage estimation fallback when signal generator fails."""
        # Make signal generator raise exception
        if service._signal_generator:
            mock_estimate = cast(MagicMock, service._signal_generator.estimate_slippage)
            mock_estimate.side_effect = Exception("Test error")

        # Should fall back to config default for BTC
        slippage = service._estimate_slippage("BTC", Decimal("10"))
        assert slippage == Decimal("0.005")  # BTC default from config

    def test_estimate_slippage_no_generator(
        self, mock_exchange_api: AsyncMock, default_config: MarketOrderConfig
    ) -> None:
        """Test slippage estimation without signal generator."""
        service = MarketOrderService(
            exchange_api=mock_exchange_api,
            signal_generator=None,
            config=default_config,
        )

        # Should use config default
        slippage = service._estimate_slippage("SOL", Decimal("100"))
        assert slippage == Decimal("0.01")  # SOL default from config

    @pytest.mark.asyncio
    async def test_get_reference_price_all_mids(
        self, service: MarketOrderService, mock_exchange_api: AsyncMock
    ) -> None:
        """Test getting reference price from AllMids."""
        # Enable AllMids in config
        service._config = MarketOrderConfig(use_all_mids_for_reference=True)

        # Mock get_all_mids method to return MidPrices instance
        mock_exchange_api.get_all_mids = AsyncMock(
            return_value=MidPrices(
                prices={"BTC": Decimal("50000"), "ETH": Decimal("3000")}, exchange="test_exchange"
            )
        )

        price = await service.get_reference_price_all_mids("BTC")
        assert price == Decimal("50000")

        price = await service.get_reference_price_all_mids("UNKNOWN")
        assert price is None

    @pytest.mark.asyncio
    async def test_get_reference_price_all_mids_disabled(self, service: MarketOrderService) -> None:
        """Test AllMids reference when disabled in config."""
        price = await service.get_reference_price_all_mids("BTC")
        assert price is None  # Should return None when disabled

    @pytest.mark.asyncio
    async def test_get_reference_price_all_mids_error(
        self, service: MarketOrderService, mock_exchange_api: AsyncMock
    ) -> None:
        """Test AllMids reference with error handling."""
        service._config = MarketOrderConfig(use_all_mids_for_reference=True)
        mock_exchange_api.get_all_mids = AsyncMock(side_effect=Exception("API Error"))

        price = await service.get_reference_price_all_mids("BTC")
        assert price is None  # Should return None on error

    def test_validate_config(self, service: MarketOrderService) -> None:
        """Test config validation."""
        # Valid config should not raise
        service.validate_config()

        # Disabled market orders should raise
        service._config = MarketOrderConfig(enabled=False)
        with pytest.raises(ValueError, match="Market orders are disabled"):
            service.validate_config()

        # Invalid slippage should raise
        service._config = MagicMock()
        service._config.enabled = True
        service._config.max_slippage_pct = Decimal("0")
        with pytest.raises(ValueError, match="Maximum slippage must be positive"):
            service.validate_config()

    def test_round_to_tick_size(self, service: MarketOrderService) -> None:
        """Test price rounding to tick size."""
        # Currently just returns the price as-is
        price = service._round_to_tick_size(Decimal("50123.456789"), "BTC")
        assert price == Decimal("50123.456789")

    @pytest.mark.asyncio
    async def test_empty_order_book_levels(
        self, service: MarketOrderService, mock_exchange_api: AsyncMock
    ) -> None:
        """Test handling of order book with empty bid/ask levels."""
        from datetime import UTC, datetime

        empty_book = OrderBook(
            symbol="EMPTY",
            bids=[],  # Empty bids
            asks=[],  # Empty asks
            timestamp=datetime.now(UTC),
        )
        mock_exchange_api.get_order_book.return_value = empty_book

        with pytest.raises(APIError) as exc_info:
            await service.calculate_aggressive_price(
                symbol="EMPTY",
                side=OrderSide.BUY,
                quantity=Decimal("1"),
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
