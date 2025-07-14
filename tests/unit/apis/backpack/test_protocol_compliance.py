"""Test suite for Backpack protocol compliance.

Tests that all Backpack components properly implement their corresponding protocols,
ensuring type safety and interface adherence throughout the codebase.
"""

import pytest

from cyberdelta.apis.backpack.mappers import (
    BackpackAccountSummaryMapper,
    BackpackBalanceMapper,
    BackpackCandleMapper,
    BackpackCommonMappers,
    BackpackFundingRateMapper,
    BackpackMarketMapper,
    BackpackOrderBookMapper,
    BackpackOrderMapper,
    BackpackPositionMapper,
    BackpackTickerMapper,
    BackpackTradeMapper,
    BackpackTransactionMapper,
    BackpackTransferMapper,
)
from cyberdelta.apis.backpack.protocols.builder_protocols import (
    AccountRequestBuilderProtocol,
    MarketDataRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
)
from cyberdelta.apis.backpack.protocols.handler_protocols import (
    AccountResponseHandlerProtocol,
    MarketDataResponseHandlerProtocol,
    TradingResponseHandlerProtocol,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
    BalanceMapperProtocol,
    CandleMapperProtocol,
    FundingRateMapperProtocol,
    MarketMapperProtocol,
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    PositionMapperProtocol,
    TickerMapperProtocol,
    TradeMapperProtocol,
    TransactionMapperProtocol,
    TransferMapperProtocol,
)
from cyberdelta.apis.backpack.request_builders import (
    BackpackAccountRequestBuilder,
    BackpackMarketDataRequestBuilder,
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers import (
    BackpackAccountResponseHandler,
    BackpackMarketDataResponseHandler,
    BackpackTradingResponseHandler,
)


class TestMapperProtocolCompliance:
    """Test that all mappers implement their protocols correctly."""

    def test_balance_mapper_implements_protocol(self) -> None:
        """Test that BackpackBalanceMapper implements BalanceMapperProtocol."""
        mapper = BackpackBalanceMapper()
        assert isinstance(mapper, BalanceMapperProtocol)

    def test_position_mapper_implements_protocol(self) -> None:
        """Test that BackpackPositionMapper implements PositionMapperProtocol."""
        mapper = BackpackPositionMapper()
        assert isinstance(mapper, PositionMapperProtocol)

    def test_account_summary_mapper_implements_protocol(self) -> None:
        """Test that BackpackAccountSummaryMapper implements AccountSummaryMapperProtocol."""
        mapper = BackpackAccountSummaryMapper()
        assert isinstance(mapper, AccountSummaryMapperProtocol)

    def test_transaction_mapper_implements_protocol(self) -> None:
        """Test that BackpackTransactionMapper implements TransactionMapperProtocol."""
        mapper = BackpackTransactionMapper()
        assert isinstance(mapper, TransactionMapperProtocol)

    def test_transfer_mapper_implements_protocol(self) -> None:
        """Test that BackpackTransferMapper implements TransferMapperProtocol."""
        mapper = BackpackTransferMapper()
        assert isinstance(mapper, TransferMapperProtocol)

    def test_order_mapper_implements_protocol(self) -> None:
        """Test that BackpackOrderMapper implements OrderMapperProtocol."""
        mapper = BackpackOrderMapper()
        assert isinstance(mapper, OrderMapperProtocol)

    def test_ticker_mapper_implements_protocol(self) -> None:
        """Test that BackpackTickerMapper implements TickerMapperProtocol."""
        mapper = BackpackTickerMapper()
        assert isinstance(mapper, TickerMapperProtocol)

    def test_order_book_mapper_implements_protocol(self) -> None:
        """Test that BackpackOrderBookMapper implements OrderBookMapperProtocol."""
        mapper = BackpackOrderBookMapper()
        assert isinstance(mapper, OrderBookMapperProtocol)

    def test_candle_mapper_implements_protocol(self) -> None:
        """Test that BackpackCandleMapper implements CandleMapperProtocol."""
        mapper = BackpackCandleMapper()
        assert isinstance(mapper, CandleMapperProtocol)

    def test_trade_mapper_implements_protocol(self) -> None:
        """Test that BackpackTradeMapper implements TradeMapperProtocol."""
        mapper = BackpackTradeMapper()
        assert isinstance(mapper, TradeMapperProtocol)

    def test_market_mapper_implements_protocol(self) -> None:
        """Test that BackpackMarketMapper implements MarketMapperProtocol."""
        mapper = BackpackMarketMapper()
        assert isinstance(mapper, MarketMapperProtocol)

    def test_funding_rate_mapper_implements_protocol(self) -> None:
        """Test that BackpackFundingRateMapper implements FundingRateMapperProtocol."""
        mapper = BackpackFundingRateMapper()
        assert isinstance(mapper, FundingRateMapperProtocol)

    def test_common_mappers_utility_class(self) -> None:
        """Test that BackpackCommonMappers is a utility class with static methods."""
        mapper = BackpackCommonMappers()
        assert hasattr(mapper, "parse_decimal_safely")
        assert hasattr(mapper, "normalize_symbol")


class TestBuilderProtocolCompliance:
    """Test that all request builders implement their protocols correctly."""

    def test_account_request_builder_implements_protocol(self) -> None:
        """Test that BackpackAccountRequestBuilder implements AccountRequestBuilderProtocol."""
        builder = BackpackAccountRequestBuilder()
        assert isinstance(builder, AccountRequestBuilderProtocol)

    def test_trading_request_builder_implements_protocol(self) -> None:
        """Test that BackpackTradingRequestBuilder implements TradingRequestBuilderProtocol."""
        builder = BackpackTradingRequestBuilder()
        assert isinstance(builder, TradingRequestBuilderProtocol)

    def test_market_data_request_builder_implements_protocol(self) -> None:
        """Test that BackpackMarketDataRequestBuilder implements MarketDataRequestBuilderProtocol.

        Verifies protocol compliance.
        """
        builder = BackpackMarketDataRequestBuilder()
        assert isinstance(builder, MarketDataRequestBuilderProtocol)


class TestHandlerProtocolCompliance:
    """Test that all response handlers implement their protocols correctly."""

    def test_account_response_handler_implements_protocol(self) -> None:
        """Test that BackpackAccountResponseHandler implements AccountResponseHandlerProtocol."""
        handler = BackpackAccountResponseHandler()
        assert isinstance(handler, AccountResponseHandlerProtocol)

    def test_trading_response_handler_implements_protocol(self) -> None:
        """Test that BackpackTradingResponseHandler implements TradingResponseHandlerProtocol."""
        handler = BackpackTradingResponseHandler()
        assert isinstance(handler, TradingResponseHandlerProtocol)

    def test_market_data_response_handler_implements_protocol(self) -> None:
        """Test that BackpackMarketDataResponseHandler implements MarketDataResponseHandlerProtocol.

        Verifies protocol compliance.
        """
        handler = BackpackMarketDataResponseHandler()
        assert isinstance(handler, MarketDataResponseHandlerProtocol)


class TestProtocolIntegration:
    """Test protocol integration with services and registry."""

    def test_all_mappers_have_common_methods(self) -> None:
        """Test that all mappers implement base mapper protocol methods."""
        mappers = [
            BackpackBalanceMapper(),
            BackpackPositionMapper(),
            BackpackAccountSummaryMapper(),
            BackpackTransactionMapper(),
            BackpackTransferMapper(),
            BackpackOrderMapper(),
            BackpackTickerMapper(),
            BackpackOrderBookMapper(),
            BackpackCandleMapper(),
            BackpackTradeMapper(),
            BackpackMarketMapper(),
            BackpackFundingRateMapper(),
            BackpackCommonMappers(),
        ]

        for mapper in mappers:
            # Check that all mappers have the common protocol methods
            assert hasattr(mapper, "parse_decimal_safely")
            assert hasattr(mapper, "normalize_symbol")
            assert hasattr(mapper, "denormalize_symbol")
            assert hasattr(mapper, "timestamp_ms_to_datetime")

    def test_all_builders_have_common_methods(self) -> None:
        """Test that all builders implement base builder protocol methods."""
        builders = [
            BackpackAccountRequestBuilder(),
            BackpackTradingRequestBuilder(),
            BackpackMarketDataRequestBuilder(),
        ]

        for builder in builders:
            # Check that all builders have the common protocol methods
            assert hasattr(builder, "build_request")

    def test_all_handlers_have_common_methods(self) -> None:
        """Test that all handlers implement base handler protocol methods."""
        handlers = [
            BackpackAccountResponseHandler(),
            BackpackTradingResponseHandler(),
            BackpackMarketDataResponseHandler(),
        ]

        for handler in handlers:
            # Check that all handlers have the common protocol methods
            assert hasattr(handler, "handle_response")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
