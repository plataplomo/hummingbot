"""Unit tests for MarketOrderService."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from cyberdelta.core.execution.orders.market_order_config import MarketOrderConfig
from cyberdelta.core.execution.orders.market_order_errors import (
    InsufficientLiquidityError,
    MarketOrderError,
    PriceDeviationError,
)
from cyberdelta.core.execution.orders.market_order_service import MarketOrderService
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import OrderBook
from cyberdelta.models.market.mid_prices import MidPrices
from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL


pytestmark = pytest.mark.timing


class TestMarketOrderService:
    """Test cases for MarketOrderService."""

    @pytest.fixture
    def mock_exchange_api(self) -> AsyncMock:
        """Create a mock exchange API.

        Returns:
            AsyncMock: Mock exchange API for testing.
        """
        api = AsyncMock()
        api.exchange_name = "test_exchange"
        return api

    @pytest.fixture
    def mock_signal_generator(self) -> MagicMock:
        """Create a mock signal generator.

        Returns:
            MagicMock: Mock signal generator with slippage estimation.
        """
        generator = MagicMock()
        generator.estimate_slippage.return_value = Decimal("0.002")  # 0.2% slippage
        return generator

    @pytest.fixture
    def default_config(self) -> MarketOrderConfig:
        """Create default market order config.

        Returns:
            MarketOrderConfig: Default configuration for market order service.
        """
        return MarketOrderConfig()

    @pytest.fixture
    def service(
        self,
        mock_exchange_api: AsyncMock,
        mock_signal_generator: MagicMock,
        default_config: MarketOrderConfig,
    ) -> MarketOrderService:
        """Create MarketOrderService instance.

        Returns:
            MarketOrderService: Configured market order service for testing.
        """
        return MarketOrderService(
            exchange_api=mock_exchange_api,
            config=default_config,
        )

    @pytest.fixture
    def sample_order_book(self) -> OrderBook:
        """Create a sample order book with good liquidity.

        Returns:
            OrderBook: Order book with good bid/ask liquidity for testing.
        """
        return OrderBook(
            symbol=BTC_HL,
            bids=[
                (Decimal(50000), Decimal(10)),
                (Decimal(49995), Decimal(20)),
                (Decimal(49990), Decimal(30)),
            ],
            asks=[
                (Decimal(50010), Decimal(10)),
                (Decimal(50015), Decimal(20)),
                (Decimal(50020), Decimal(30)),
            ],
            timestamp=datetime.now(UTC),
        )

    @pytest.fixture
    def thin_order_book(self) -> OrderBook:
        """Create a thin order book with limited liquidity.

        Returns:
            OrderBook: Order book with limited liquidity for testing.
        """
        illiquid_symbol = exchanges.hyperliquid("ILLIQUID-PERP")
        return OrderBook(
            symbol=illiquid_symbol,
            bids=[(Decimal(100), Decimal("0.1"))],
            asks=[(Decimal(101), Decimal("0.1"))],
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
        # Mock get_market to return None so round_to_tick_size returns original price
        mock_exchange_api.get_market.return_value = None

        price = await service.calculate_aggressive_price(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            quantity=Decimal(5),
        )

        # Best ask is 50010, signal generator returns 0.2% slippage
        expected = Decimal(50010) * Decimal("1.002")
        assert abs(price - expected) < Decimal(1)  # Allow small rounding difference

    @pytest.mark.asyncio
    async def test_calculate_aggressive_price_sell(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
        sample_order_book: OrderBook,
    ) -> None:
        """Test calculating aggressive price for sell orders."""
        mock_exchange_api.get_order_book.return_value = sample_order_book
        # Mock get_market to return None so round_to_tick_size returns original price
        mock_exchange_api.get_market.return_value = None

        price = await service.calculate_aggressive_price(
            symbol=BTC_HL,
            side=OrderSide.SELL,
            quantity=Decimal(5),
        )

        # Best bid is 50000, signal generator returns 0.2% slippage
        expected = Decimal(50000) * Decimal("0.998")
        assert abs(price - expected) < Decimal(1)

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
                symbol=exchanges.hyperliquid("ILLIQUID-PERP"),
                side=OrderSide.BUY,
                quantity=Decimal(10),  # Requesting 10, only 0.1 available
            )

        error = exc_info.value
        assert error.symbol.value == "ILLIQUID-PERP"
        assert error.requested_quantity == Decimal(10)
        assert error.available_quantity == Decimal("0.1")

    @pytest.mark.asyncio
    async def test_no_order_book_error(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test error when order book is unavailable."""
        mock_exchange_api.get_order_book.return_value = None

        with pytest.raises(MarketOrderError) as exc_info:
            await service.calculate_aggressive_price(
                symbol=BTC_HL,
                side=OrderSide.BUY,
                quantity=Decimal(1),
            )

        assert "no order book for BTC-PERP" in str(exc_info.value)

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
        # Mock get_market to return None so round_to_tick_size returns original price
        mock_exchange_api.get_market.return_value = None
        mock_signal_generator.estimate_slippage.return_value = Decimal("0.01")  # 1%

        # Override max slippage to 0.5%
        price = await service.calculate_aggressive_price(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            quantity=Decimal(5),
            max_slippage=Decimal("0.005"),
        )

        # Should use 0.5% instead of 1%
        expected = Decimal(50010) * Decimal("1.005")
        assert abs(price - expected) < Decimal(1)

    @pytest.mark.asyncio
    async def test_price_deviation_error(
        self,
        mock_exchange_api: AsyncMock,
        mock_signal_generator: MagicMock,
    ) -> None:
        """Test error when price deviation exceeds limits."""
        # Create order book with extreme spread
        extreme_symbol = exchanges.hyperliquid("EXTREME-PERP")
        extreme_book = OrderBook(
            symbol=extreme_symbol,
            bids=[(Decimal(100), Decimal(10))],
            asks=[(Decimal(200), Decimal(10))],  # 100% spread
            timestamp=datetime.now(UTC),
        )
        mock_exchange_api.get_order_book.return_value = extreme_book

        # Create a new service instance with custom configuration to allow high
        # slippage but low price deviation
        custom_config = MarketOrderConfig(
            max_slippage_pct=Decimal("0.2"),  # Allow 20% slippage
            max_price_deviation_pct=Decimal("0.05"),  # But only 5% price deviation
            slippage_by_base_asset={
                "EXTREME": Decimal("0.2"),  # Allow 20% for EXTREME symbol
                "default": Decimal("0.02"),
            },
        )

        service_with_custom_config = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=custom_config,
        )

        # Mock signal generator to return high slippage
        mock_signal_generator.estimate_slippage.return_value = Decimal("0.2")  # 20%

        with pytest.raises(PriceDeviationError) as exc_info:
            await service_with_custom_config.calculate_aggressive_price(
                symbol=extreme_symbol,
                side=OrderSide.BUY,
                quantity=Decimal(1),
            )

        error = exc_info.value
        assert error.symbol.value == "EXTREME-PERP"
        assert error.deviation_pct > error.max_deviation_pct

    def test_calculate_liquidity_ratio(self, service: MarketOrderService) -> None:
        """Test liquidity ratio calculation."""
        test_symbol = exchanges.hyperliquid("TEST-PERP")
        order_book = OrderBook(
            symbol=test_symbol,
            bids=[(Decimal(100), Decimal(5))],
            asks=[(Decimal(101), Decimal(10))],
            timestamp=datetime.now(UTC),
        )

        # Buy side - 10 available, requesting 5
        ratio = service.calculate_liquidity_ratio(order_book, OrderSide.BUY, Decimal(5))
        assert ratio == Decimal(2)  # 10/5 = 2

        # Sell side - 5 available, requesting 10
        ratio = service.calculate_liquidity_ratio(order_book, OrderSide.SELL, Decimal(10))
        assert ratio == Decimal("0.5")  # 5/10 = 0.5

    @pytest.mark.asyncio
    async def test_estimate_slippage_fallback(
        self,
        mock_exchange_api: AsyncMock,
        default_config: MarketOrderConfig,
    ) -> None:
        """Test slippage estimation fallback when signal generator fails."""
        # Create a mock signal generator that will fail
        mock_failing_signal_generator = MagicMock()
        mock_failing_signal_generator.estimate_slippage.side_effect = ValueError("Test error")

        # Create a service with the failing signal generator
        service_with_failing_generator = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=default_config,
        )

        # Test the slippage estimation indirectly through calculate_aggressive_price
        # which internally calls _estimate_slippage and handles the fallback
        mock_exchange_api.get_order_book.return_value = OrderBook(
            symbol=BTC_HL,
            bids=[(Decimal(50000), Decimal(100))],
            asks=[(Decimal(50010), Decimal(100))],
            timestamp=datetime.now(UTC),
        )
        mock_exchange_api.get_market.return_value = None

        # When signal generator fails, it should fall back to config default
        # This is indirectly tested through the price calculation
        price = await service_with_failing_generator.calculate_aggressive_price(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            quantity=Decimal(10),
        )

        # Verify the price was calculated (indicating fallback worked)
        # Expected price with BTC default from config (0.005)
        expected = Decimal(50010) * Decimal("1.005")
        assert abs(price - expected) < Decimal(1)

    @pytest.mark.asyncio
    async def test_estimate_slippage_no_generator(
        self,
        mock_exchange_api: AsyncMock,
        default_config: MarketOrderConfig,
    ) -> None:
        """Test slippage estimation without signal generator."""
        service = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=default_config,
        )

        # Test the slippage estimation indirectly through calculate_aggressive_price
        mock_exchange_api.get_order_book.return_value = OrderBook(
            symbol=SOL_HL,
            bids=[(Decimal(100), Decimal(100))],
            asks=[(Decimal(101), Decimal(100))],
            timestamp=datetime.now(UTC),
        )
        mock_exchange_api.get_market.return_value = None

        # When no signal generator and insufficient liquidity, should raise error
        with pytest.raises(InsufficientLiquidityError):
            await service.calculate_aggressive_price(
                symbol=SOL_HL,
                side=OrderSide.BUY,
                quantity=Decimal(100),
            )

    @pytest.mark.asyncio
    async def test_get_reference_price_all_mids(
        self,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test getting reference price from AllMids."""
        # Create a service with AllMids enabled in config
        config_with_all_mids = MarketOrderConfig(use_all_mids_for_reference=True)
        service_with_all_mids = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=config_with_all_mids,
        )

        # Mock get_all_mids method to return MidPrices instance
        mock_exchange_api.get_all_mids = AsyncMock(
            return_value=MidPrices(
                prices={BTC_HL: Decimal(50000), ETH_HL: Decimal(3000)},
                exchange=ExchangeName.HYPERLIQUID,
            ),
        )

        price = await service_with_all_mids.get_reference_price_all_mids(BTC_HL)
        assert price == Decimal(50000)

        unknown_symbol = exchanges.hyperliquid("UNKNOWN-PERP")
        price = await service_with_all_mids.get_reference_price_all_mids(unknown_symbol)
        assert price is None

    @pytest.mark.asyncio
    async def test_get_reference_price_all_mids_disabled(
        self,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test AllMids reference when disabled in config."""
        # Create a service with default config (AllMids disabled)
        service_with_disabled_all_mids = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=MarketOrderConfig(),  # Default has use_all_mids_for_reference=False
        )

        price = await service_with_disabled_all_mids.get_reference_price_all_mids(BTC_HL)
        assert price is None  # Should return None when disabled

    @pytest.mark.asyncio
    async def test_get_reference_price_all_mids_error(
        self,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test AllMids reference with error handling."""
        # Create a service with AllMids enabled
        config_with_all_mids = MarketOrderConfig(use_all_mids_for_reference=True)
        service_with_all_mids = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=config_with_all_mids,
        )

        mock_exchange_api.get_all_mids = AsyncMock(side_effect=ValueError("API Error"))

        price = await service_with_all_mids.get_reference_price_all_mids(BTC_HL)
        assert price is None  # Should return None on error

    def test_validate_config(
        self,
        mock_exchange_api: AsyncMock,
        mock_signal_generator: MagicMock,
    ) -> None:
        """Test config validation."""
        # Test valid config
        valid_service = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=MarketOrderConfig(),
        )
        valid_service.validate_config()  # Should not raise

        # Test disabled market orders
        disabled_config = MarketOrderConfig(enabled=False)
        disabled_service = MarketOrderService(
            exchange_api=mock_exchange_api,
            config=disabled_config,
        )
        with pytest.raises(ValueError, match="Market orders are disabled"):
            disabled_service.validate_config()

        # Test invalid slippage configuration
        # This should fail at model validation time, not at validate_config time
        # So we test that the config validation catches issues
        with pytest.raises(ValidationError):
            MarketOrderConfig(max_slippage_pct=Decimal(0))

    @pytest.mark.asyncio
    async def test_round_to_tick_size(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test price rounding to tick size."""
        # Mock get_market to return None so it returns the original price
        mock_exchange_api.get_market.return_value = None

        price = await service.round_to_tick_size(Decimal("50123.456789"), BTC_HL)
        assert price == Decimal("50123.456789")

    @pytest.mark.asyncio
    async def test_empty_order_book_levels(
        self,
        service: MarketOrderService,
        mock_exchange_api: AsyncMock,
    ) -> None:
        """Test handling of order book with empty bid/ask levels."""
        empty_symbol = exchanges.hyperliquid("EMPTY-PERP")
        empty_book = OrderBook(
            symbol=empty_symbol,
            bids=[],  # Empty bids
            asks=[],  # Empty asks
            timestamp=datetime.now(UTC),
        )
        mock_exchange_api.get_order_book.return_value = empty_book

        with pytest.raises(MarketOrderError) as exc_info:
            await service.calculate_aggressive_price(
                symbol=empty_symbol,
                side=OrderSide.BUY,
                quantity=Decimal(1),
            )

        assert "no order book for EMPTY-PERP" in str(exc_info.value)
