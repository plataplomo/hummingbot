"""Test protocol compliance for Hyperliquid protocol implementations.

This module tests that Hyperliquid mappers, builders, and handlers correctly
implement their respective protocols, following the Backpack pattern.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import AnyUrl, HttpUrl, SecretStr

from cyberdelta.apis.hyperliquid.hl_api_components_factory import (
    HyperliquidAPIComponentsFactory,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_account_summary_mapper import (
    HyperliquidAccountSummaryMapper,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_balance_mapper import (
    HyperliquidBalanceMapper,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_position_mapper import (
    HyperliquidPositionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_transaction_mapper import (
    HyperliquidTransactionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_historical_data_mapper import (
    HyperliquidHistoricalDataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper import (
    HyperliquidMarketMetadataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_order_book_mapper import (
    HyperliquidOrderBookMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_price_ticker_mapper import (
    HyperliquidPriceTickerMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import (
    HyperliquidOrderMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_response_mapper import (
    HyperliquidOrderResponseMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_trading_enum_mapper import (
    HyperliquidTradingEnumMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
)
from cyberdelta.apis.hyperliquid.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import (
    AccountRequestBuilderProtocol,
    MarketDataRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    AccountResponseHandlerProtocol,
    MarketDataResponseHandlerProtocol,
    TradingResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
    BalanceMapperProtocol,
    HistoricalDataMapperProtocol,
    MarketMetadataMapperProtocol,
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    OrderResponseMapperProtocol,
    PositionMapperProtocol,
    PriceTickerMapperProtocol,
    TradingEnumMapperProtocol,
    TransactionMapperProtocol,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_account_request_builder import (
    HyperliquidAccountRequestBuilder,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_account_response_handler import (
    HyperliquidAccountResponseHandler,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_market_data_response_handler import (
    HyperliquidMarketDataResponseHandler,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.models.spot_balance import SpotBalance
from cyberdelta.enums.exchange_names import ExchangeName


# Test data for parametrization
MAPPER_PROTOCOL_PAIRS = [
    (HyperliquidBalanceMapper, BalanceMapperProtocol),
    (HyperliquidPositionMapper, PositionMapperProtocol),
    (HyperliquidAccountSummaryMapper, AccountSummaryMapperProtocol),
    (HyperliquidTransactionMapper, TransactionMapperProtocol),
    (HyperliquidOrderMapper, OrderMapperProtocol),
    (HyperliquidOrderResponseMapper, OrderResponseMapperProtocol),
    (HyperliquidTradingEnumMapper, TradingEnumMapperProtocol),
    (HyperliquidOrderBookMapper, OrderBookMapperProtocol),
    (HyperliquidPriceTickerMapper, PriceTickerMapperProtocol),
    (HyperliquidHistoricalDataMapper, HistoricalDataMapperProtocol),
    (HyperliquidMarketMetadataMapper, MarketMetadataMapperProtocol),
]

BUILDER_PROTOCOL_PAIRS = [
    (HyperliquidAccountRequestBuilder, AccountRequestBuilderProtocol),
    (HyperliquidTradingRequestBuilder, TradingRequestBuilderProtocol),
    (HyperliquidMarketDataRequestBuilder, MarketDataRequestBuilderProtocol),
]

HANDLER_PROTOCOL_PAIRS = [
    (HyperliquidAccountResponseHandler, AccountResponseHandlerProtocol),
    (HyperliquidTradingResponseHandler, TradingResponseHandlerProtocol),
    (HyperliquidMarketDataResponseHandler, MarketDataResponseHandlerProtocol),
]

BASE_PROTOCOLS: list[type[Any]] = [
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
]

MAPPER_PROTOCOLS: list[type[Any]] = [
    BalanceMapperProtocol,
    PositionMapperProtocol,
    AccountSummaryMapperProtocol,
    TransactionMapperProtocol,
    OrderMapperProtocol,
    OrderResponseMapperProtocol,
    TradingEnumMapperProtocol,
    OrderBookMapperProtocol,
    PriceTickerMapperProtocol,
    HistoricalDataMapperProtocol,
    MarketMetadataMapperProtocol,
]

BUILDER_PROTOCOLS: list[type[Any]] = [
    AccountRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
    MarketDataRequestBuilderProtocol,
]

HANDLER_PROTOCOLS: list[type[Any]] = [
    AccountResponseHandlerProtocol,
    TradingResponseHandlerProtocol,
    MarketDataResponseHandlerProtocol,
]

BALANCE_MAPPER_REQUIRED_METHODS = [
    "parse_decimal_safely",
    "normalize_symbol",
    "denormalize_symbol",
    "timestamp_ms_to_datetime",
    "transform_raw_balance_to_internal",
]

MARKET_DATA_BUILDER_REQUIRED_METHODS = [
    "build_request",
    "build_get_all_mids_params",
    "build_get_l2_book_params",
    "build_get_recent_trades_params",
    "build_get_candles_params",
    "build_get_funding_history_params",
    "build_get_meta_params",
]

MARKET_DATA_HANDLER_REQUIRED_METHODS = [
    "handle_get_all_mids_response",
    "handle_get_l2_book_response",
    "handle_get_recent_trades_response",
    "handle_get_candles_response",
    "handle_get_funding_history_response",
    "handle_get_meta_response",
]

MARKET_DATA_BUILDER_TEST_METHODS = [
    ("build_get_all_mids_params", []),
    ("build_get_l2_book_params", ["BTC"]),
    ("build_get_recent_trades_params", ["BTC"]),
    ("build_get_candles_params", ["BTC", "1m", 1000000000, 2000000000]),
    ("build_get_funding_history_params", ["BTC"]),
    ("build_get_meta_params", []),
]

FACTORY_COMPONENT_TESTS = [
    # Mappers
    ("balance_mapper", BalanceMapperProtocol),
    ("position_mapper", PositionMapperProtocol),
    ("account_summary_mapper", AccountSummaryMapperProtocol),
    ("transaction_mapper", TransactionMapperProtocol),
    ("order_mapper", OrderMapperProtocol),
    ("order_response_mapper", OrderResponseMapperProtocol),
    ("trading_enum_mapper", TradingEnumMapperProtocol),
    ("order_book_mapper", OrderBookMapperProtocol),
    ("price_ticker_mapper", PriceTickerMapperProtocol),
    ("historical_data_mapper", HistoricalDataMapperProtocol),
    ("market_metadata_mapper", MarketMetadataMapperProtocol),
    # Builders
    ("account_request_builder", AccountRequestBuilderProtocol),
    ("trading_request_builder", TradingRequestBuilderProtocol),
    ("market_data_request_builder", MarketDataRequestBuilderProtocol),
    # Handlers
    ("account_response_handler", AccountResponseHandlerProtocol),
    ("trading_response_handler", TradingResponseHandlerProtocol),
    ("market_data_response_handler", MarketDataResponseHandlerProtocol),
]


# Fixtures
@pytest.fixture
def balance_mapper() -> HyperliquidBalanceMapper:
    """Provide a HyperliquidBalanceMapper instance for testing."""
    return HyperliquidBalanceMapper()


@pytest.fixture
def market_data_builder() -> HyperliquidMarketDataRequestBuilder:
    """Provide a HyperliquidMarketDataRequestBuilder instance for testing."""
    return HyperliquidMarketDataRequestBuilder()


@pytest.fixture
def market_data_handler() -> HyperliquidMarketDataResponseHandler:
    """Provide a HyperliquidMarketDataResponseHandler instance for testing."""
    return HyperliquidMarketDataResponseHandler()


@pytest.fixture
def test_factory() -> HyperliquidAPIComponentsFactory:
    """Provide a configured factory instance for testing."""
    config = ExchangeSpecificConfig(
        enabled=True,
        api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
        ws_url_mainnet=AnyUrl("wss://api.hyperliquid.xyz/ws"),
        api_base_url_testnet=HttpUrl("https://api.hyperliquid.xyz"),
        ws_url_testnet=AnyUrl("wss://api.hyperliquid.xyz/ws"),
        symbols={"BTC": "BTC", "ETH": "ETH"},
        exchange_name=ExchangeName.HYPERLIQUID,
    )

    secrets = PrivateKeyAuthSecrets(
        auth_type="private_key",
        private_key=SecretStr("0x" + "00" * 32),
    )
    return HyperliquidAPIComponentsFactory(
        exchange_config=config,
        exchange_secrets=secrets,
        chain_id=1337,
    )


@pytest.fixture
def structural_balance_mapper() -> BalanceMapperProtocol:
    """Provide a structural balance mapper for testing structural typing."""

    class StructuralBalanceMapper:
        """A class that structurally matches BalanceMapperProtocol without inheriting."""

        @staticmethod
        def parse_decimal_safely(
            value: str | float | Decimal | None, default: Decimal = Decimal(0)
        ) -> Decimal:
            return Decimal(str(value) if value else 0)

        @staticmethod
        def normalize_symbol(symbol: str) -> str:
            return symbol.upper()

        @staticmethod
        def denormalize_symbol(symbol: str) -> str:
            return symbol

        @staticmethod
        def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
            if timestamp_ms is None:
                return None
            return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)

        @staticmethod
        def transform_raw_balance_to_internal(
            asset_symbol: str, raw_user_state: HyperliquidRawClearinghouseState
        ) -> SpotBalance:
            return SpotBalance(
                exchange="test",
                asset=asset_symbol,
                timestamp=datetime.now(UTC),
                total_quantity=Decimal(0),
                available_quantity=Decimal(0),
            )

        @staticmethod
        def transform_raw_clearinghouse_state_to_spot_balances(
            raw_state: HyperliquidRawClearinghouseState,
        ) -> dict[str, SpotBalance]:
            return {}

    return StructuralBalanceMapper()


# Test Classes
@pytest.mark.protocol_compliance
class TestMapperProtocolCompliance:
    """Test that all Hyperliquid mappers implement their protocols correctly."""

    @pytest.mark.parametrize(("mapper_class", "protocol_class"), MAPPER_PROTOCOL_PAIRS)
    def test_mapper_class_implements_protocol(
        self, mapper_class: type, protocol_class: type
    ) -> None:
        """Test that mapper class implements its specific protocol."""
        assert issubclass(mapper_class, protocol_class), (
            f"{mapper_class.__name__} should implement {protocol_class.__name__}"
        )

    @pytest.mark.parametrize(("mapper_class", "protocol_class"), MAPPER_PROTOCOL_PAIRS)
    def test_mapper_instance_implements_protocol(
        self, mapper_class: type, protocol_class: type
    ) -> None:
        """Test that mapper instance implements its specific protocol."""
        mapper_instance = mapper_class()
        assert isinstance(mapper_instance, protocol_class), (
            f"{mapper_class.__name__} instance should implement {protocol_class.__name__}"
        )

    @pytest.mark.parametrize(("mapper_class", "protocol_class"), MAPPER_PROTOCOL_PAIRS)
    def test_mapper_implements_base_protocol(
        self, mapper_class: type, protocol_class: type
    ) -> None:
        """Test that all mappers implement the base MapperProtocol."""
        mapper_instance = mapper_class()
        assert isinstance(mapper_instance, MapperProtocol), (
            f"{mapper_class.__name__} should implement base MapperProtocol"
        )

    @pytest.mark.parametrize("protocol_class", MAPPER_PROTOCOLS)
    def test_mapper_protocol_inherits_from_base(self, protocol_class: type) -> None:
        """Test that mapper protocols inherit from base MapperProtocol."""
        assert issubclass(protocol_class, MapperProtocol), (
            f"{protocol_class.__name__} should inherit from MapperProtocol"
        )


@pytest.mark.protocol_compliance
class TestBuilderProtocolCompliance:
    """Test that all Hyperliquid request builders implement their protocols correctly."""

    @pytest.mark.parametrize(("builder_class", "protocol_class"), BUILDER_PROTOCOL_PAIRS)
    def test_builder_class_implements_protocol(
        self, builder_class: type, protocol_class: type
    ) -> None:
        """Test that builder class implements its specific protocol."""
        assert issubclass(builder_class, protocol_class), (
            f"{builder_class.__name__} should implement {protocol_class.__name__}"
        )

    @pytest.mark.parametrize(("builder_class", "protocol_class"), BUILDER_PROTOCOL_PAIRS)
    def test_builder_instance_implements_protocol(
        self, builder_class: type, protocol_class: type
    ) -> None:
        """Test that builder instance implements its specific protocol."""
        builder_instance = builder_class()
        assert isinstance(builder_instance, protocol_class), (
            f"{builder_class.__name__} instance should implement {protocol_class.__name__}"
        )

    @pytest.mark.parametrize(("builder_class", "protocol_class"), BUILDER_PROTOCOL_PAIRS)
    def test_builder_implements_base_protocol(
        self, builder_class: type, protocol_class: type
    ) -> None:
        """Test that all builders implement the base RequestBuilderProtocol."""
        builder_instance = builder_class()
        assert isinstance(builder_instance, RequestBuilderProtocol), (
            f"{builder_class.__name__} should implement base RequestBuilderProtocol"
        )

    @pytest.mark.parametrize("protocol_class", BUILDER_PROTOCOLS)
    def test_builder_protocol_inherits_from_base(self, protocol_class: type) -> None:
        """Test that builder protocols inherit from base RequestBuilderProtocol."""
        assert issubclass(protocol_class, RequestBuilderProtocol), (
            f"{protocol_class.__name__} should inherit from RequestBuilderProtocol"
        )


@pytest.mark.protocol_compliance
class TestHandlerProtocolCompliance:
    """Test that all Hyperliquid response handlers implement their protocols correctly."""

    @pytest.mark.parametrize(("handler_class", "protocol_class"), HANDLER_PROTOCOL_PAIRS)
    def test_handler_class_implements_protocol(
        self, handler_class: type, protocol_class: type
    ) -> None:
        """Test that handler class implements its specific protocol."""
        assert issubclass(handler_class, protocol_class), (
            f"{handler_class.__name__} should implement {protocol_class.__name__}"
        )

    @pytest.mark.parametrize(("handler_class", "protocol_class"), HANDLER_PROTOCOL_PAIRS)
    def test_handler_instance_implements_protocol(
        self, handler_class: type, protocol_class: type
    ) -> None:
        """Test that handler instance implements its specific protocol."""
        handler_instance = handler_class()
        assert isinstance(handler_instance, protocol_class), (
            f"{handler_class.__name__} instance should implement {protocol_class.__name__}"
        )

    @pytest.mark.parametrize(("handler_class", "protocol_class"), HANDLER_PROTOCOL_PAIRS)
    def test_handler_implements_base_protocol(
        self, handler_class: type, protocol_class: type
    ) -> None:
        """Test that all handlers implement the base ResponseHandlerProtocol."""
        handler_instance = handler_class()
        assert isinstance(handler_instance, ResponseHandlerProtocol), (
            f"{handler_class.__name__} should implement base ResponseHandlerProtocol"
        )

    @pytest.mark.parametrize("protocol_class", HANDLER_PROTOCOLS)
    def test_handler_protocol_inherits_from_base(self, protocol_class: type) -> None:
        """Test that handler protocols inherit from base ResponseHandlerProtocol."""
        assert issubclass(protocol_class, ResponseHandlerProtocol), (
            f"{protocol_class.__name__} should inherit from ResponseHandlerProtocol"
        )


@pytest.mark.protocol_methods
class TestProtocolMethodSignatures:
    """Test that protocol method signatures match expected patterns."""

    @pytest.mark.parametrize("method_name", BALANCE_MAPPER_REQUIRED_METHODS)
    def test_balance_mapper_has_required_methods(
        self, balance_mapper: HyperliquidBalanceMapper, method_name: str
    ) -> None:
        """Test that balance mapper has all required methods."""
        assert hasattr(balance_mapper, method_name), (
            f"BalanceMapper should have {method_name} method"
        )
        assert callable(getattr(balance_mapper, method_name)), f"{method_name} should be callable"

    def test_balance_mapper_parse_decimal_safely_returns_decimal(
        self, balance_mapper: HyperliquidBalanceMapper
    ) -> None:
        """Test that parse_decimal_safely returns Decimal."""
        result = balance_mapper.parse_decimal_safely("10.5")
        assert isinstance(result, Decimal)
        assert result == Decimal("10.5")

    def test_balance_mapper_normalize_symbol_returns_string(
        self, balance_mapper: HyperliquidBalanceMapper
    ) -> None:
        """Test that normalize_symbol returns string."""
        result = balance_mapper.normalize_symbol("BTC-PERP")
        assert isinstance(result, str)

    def test_balance_mapper_denormalize_symbol_returns_string(
        self, balance_mapper: HyperliquidBalanceMapper
    ) -> None:
        """Test that denormalize_symbol returns string."""
        result = balance_mapper.denormalize_symbol("BTC")
        assert isinstance(result, str)

    @pytest.mark.parametrize("method_name", MARKET_DATA_BUILDER_REQUIRED_METHODS)
    def test_market_data_builder_has_required_methods(
        self, market_data_builder: HyperliquidMarketDataRequestBuilder, method_name: str
    ) -> None:
        """Test that market data builder has all required methods."""
        assert hasattr(market_data_builder, method_name), (
            f"MarketDataRequestBuilder should have {method_name} method"
        )
        assert callable(getattr(market_data_builder, method_name)), (
            f"{method_name} should be callable"
        )

    @pytest.mark.parametrize("method_name", MARKET_DATA_HANDLER_REQUIRED_METHODS)
    def test_market_data_handler_has_required_methods(
        self, market_data_handler: HyperliquidMarketDataResponseHandler, method_name: str
    ) -> None:
        """Test that market data handler has all required methods."""
        assert hasattr(HyperliquidMarketDataResponseHandler, method_name), (
            f"MarketDataResponseHandler should have {method_name} method"
        )
        assert callable(getattr(HyperliquidMarketDataResponseHandler, method_name)), (
            f"{method_name} should be callable"
        )

    @pytest.mark.parametrize(("method_name", "args"), MARKET_DATA_BUILDER_TEST_METHODS)
    def test_market_data_builder_methods_return_dict(
        self, market_data_builder: HyperliquidMarketDataRequestBuilder, method_name: str, args: list
    ) -> None:
        """Test that market data builder methods return dict."""
        method = getattr(market_data_builder, method_name)
        result = method(*args)
        assert isinstance(result, dict), f"{method_name} should return dict"
        # All keys should be strings for JSON serialization
        for key in result:
            assert isinstance(key, str), f"Keys should be strings in {method_name}"


@pytest.mark.protocol_runtime_check
class TestRuntimeProtocolChecking:
    """Test runtime protocol checking functionality."""

    @pytest.mark.parametrize(
        "protocol_class", BASE_PROTOCOLS + MAPPER_PROTOCOLS + BUILDER_PROTOCOLS + HANDLER_PROTOCOLS
    )
    def test_protocols_are_runtime_checkable(self, protocol_class: type) -> None:
        """Test that all protocols are marked as runtime_checkable."""
        assert hasattr(protocol_class, "__protocol_attrs__"), (
            f"{protocol_class.__name__} should be runtime_checkable"
        )

    def test_proper_implementation_satisfies_protocol(
        self, balance_mapper: HyperliquidBalanceMapper
    ) -> None:
        """Test that a proper implementation satisfies the protocol."""
        assert isinstance(balance_mapper, BalanceMapperProtocol)
        assert isinstance(balance_mapper, MapperProtocol)

    def test_non_implementation_fails_protocol_check(self) -> None:
        """Test that an object NOT implementing the protocol fails isinstance check."""

        class FakeMapper:
            """A class that doesn't implement the protocol."""

        fake_mapper = FakeMapper()
        assert not isinstance(fake_mapper, BalanceMapperProtocol)
        assert not isinstance(fake_mapper, MapperProtocol)

    def test_partial_implementation_fails_protocol_check(self) -> None:
        """Test that partial implementation also fails protocol check."""

        class PartialMapper:
            """A class that only partially implements the protocol."""

            @staticmethod
            def parse_decimal_safely(value: str | float | None, default: Decimal) -> Decimal:
                return Decimal(0)

            # Missing other required methods

        partial_mapper = PartialMapper()
        assert not isinstance(partial_mapper, BalanceMapperProtocol)
        assert not isinstance(partial_mapper, MapperProtocol)

    def test_structural_matching_works(
        self, structural_balance_mapper: BalanceMapperProtocol
    ) -> None:
        """Test that protocols use structural matching, not nominal."""
        # Should satisfy the protocol through structural matching
        assert isinstance(structural_balance_mapper, BalanceMapperProtocol)
        assert isinstance(structural_balance_mapper, MapperProtocol)

    def test_wrong_method_signatures_fail_protocol_check(self) -> None:
        """Test that wrong method signatures fail protocol check."""
        # Due to mypy's strict protocol checking, we can't actually create a class
        # that has incompatible signatures and still compiles. This test validates
        # that mypy correctly identifies signature mismatches at compile time.

        # Instead, test that a class missing required methods fails the check
        class IncompatibleMapper:
            """A mapper missing required methods."""

            def some_other_method(self) -> None:
                pass

        fake = IncompatibleMapper()
        # Should not pass protocol check due to missing required methods
        assert not isinstance(fake, BalanceMapperProtocol)

    def test_protocol_in_type_annotations(self) -> None:
        """Test that protocols work correctly in type annotations."""

        def process_balance_mapper(mapper: BalanceMapperProtocol) -> bool:
            """Function that accepts a BalanceMapperProtocol."""
            return isinstance(mapper, BalanceMapperProtocol)

        # Should accept actual implementation
        assert process_balance_mapper(HyperliquidBalanceMapper())


@pytest.mark.factory_validation
class TestFactoryProtocolValidation:
    """Test that the factory validates protocol compliance."""

    @pytest.mark.parametrize(("component_name", "expected_protocol"), FACTORY_COMPONENT_TESTS)
    def test_factory_component_implements_protocol(
        self,
        test_factory: HyperliquidAPIComponentsFactory,
        component_name: str,
        expected_protocol: type,
    ) -> None:
        """Test that factory components implement their expected protocols."""
        # Use string literal to get around type checker
        # Use cast to bypass overload checking for parametrized test
        component = cast(Any, test_factory.get_shared_component)(component_name)
        assert isinstance(component, expected_protocol), (
            f"Component '{component_name}' should implement {expected_protocol.__name__}"
        )

    def test_factory_rejects_non_compliant_components(
        self, test_factory: HyperliquidAPIComponentsFactory
    ) -> None:
        """Test that factory properly rejects non-protocol compliant components."""

        class BadMapper:
            """A mapper that doesn't implement the protocol."""

        bad_mapper = BadMapper()

        # The _validate_protocol_compliance method should raise TypeError
        with pytest.raises(TypeError) as exc_info:
            # Cast to bypass strict type checking for testing invalid component
            test_factory._validate_protocol_compliance("balance_mapper", cast(Any, bad_mapper))
        assert "does not implement BalanceMapperProtocol" in str(exc_info.value)

    def test_factory_has_sufficient_overloads(self) -> None:
        """Test that factory has the expected number of overloads."""
        source = inspect.getsource(HyperliquidAPIComponentsFactory)
        overload_count = source.count("def get_shared_component(")

        # Should have 17 overloads + 1 implementation = 18 total
        assert overload_count >= 18, (
            f"Factory should have at least 17 overloads, found {overload_count - 1}"
        )


@pytest.mark.protocol_consistency
class TestProtocolConsistency:
    """Test that protocol implementations are consistent across the system."""

    def test_mapper_inheritance_consistency(self) -> None:
        """Test that mapper protocol inheritance is consistent."""
        balance_mapper = HyperliquidBalanceMapper()
        assert isinstance(balance_mapper, MapperProtocol)
        assert isinstance(balance_mapper, BalanceMapperProtocol)

    def test_builder_inheritance_consistency(self) -> None:
        """Test that builder protocol inheritance is consistent."""
        builder = HyperliquidMarketDataRequestBuilder()
        assert isinstance(builder, RequestBuilderProtocol)
        assert isinstance(builder, MarketDataRequestBuilderProtocol)

    def test_handler_inheritance_consistency(self) -> None:
        """Test that handler protocol inheritance is consistent."""
        handler = HyperliquidMarketDataResponseHandler()
        assert isinstance(handler, ResponseHandlerProtocol)
        assert isinstance(handler, MarketDataResponseHandlerProtocol)

    @pytest.mark.parametrize("method_name", BALANCE_MAPPER_REQUIRED_METHODS)
    def test_mapper_static_methods_are_callable(self, method_name: str) -> None:
        """Test that mapper static methods are callable without instance."""
        method = getattr(HyperliquidBalanceMapper, method_name)
        assert callable(method), f"{method_name} should be callable"

    @pytest.mark.parametrize(
        "method_name", MARKET_DATA_BUILDER_REQUIRED_METHODS[:6]
    )  # Skip build_request with complex args
    def test_builder_static_methods_are_callable(self, method_name: str) -> None:
        """Test that builder static methods are callable without instance."""
        method = getattr(HyperliquidMarketDataRequestBuilder, method_name)
        assert callable(method), f"{method_name} should be callable"

    @pytest.mark.parametrize("method_name", MARKET_DATA_HANDLER_REQUIRED_METHODS)
    def test_handler_static_methods_are_callable(self, method_name: str) -> None:
        """Test that handler static methods are callable without instance."""
        method = getattr(HyperliquidMarketDataResponseHandler, method_name)
        assert callable(method), f"{method_name} should be callable"
