"""Tests for Hyperliquid WebSocket subscription payload models."""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsAllMidsSubscriptionPayload,
    HyperliquidRawWsCandleSubscriptionPayload,
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
    HyperliquidRawWsTradesSubscriptionPayload,
    HyperliquidRawWsUserEventsSubscriptionPayload,
)


class TestHyperliquidWsPayloads:
    """Test Hyperliquid WebSocket subscription payload models."""

    def test_l2book_subscription_payload(self) -> None:
        """Test L2Book subscription payload construction."""
        payload = HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin="ETH")
        assert payload.type == "l2Book"
        assert payload.coin == "ETH"

        # Test serialization
        data = payload.model_dump(by_alias=True)
        assert data == {"type": "l2Book", "coin": "ETH"}

    def test_trades_subscription_payload(self) -> None:
        """Test trades subscription payload construction."""
        payload = HyperliquidRawWsTradesSubscriptionPayload(type="trades", coin="BTC")
        assert payload.type == "trades"
        assert payload.coin == "BTC"

    def test_user_events_subscription_payload(self) -> None:
        """Test userEvents subscription payload construction."""
        wallet = "0x1234567890abcdef1234567890abcdef12345678"
        payload = HyperliquidRawWsUserEventsSubscriptionPayload(type="userEvents", user=wallet)
        assert payload.type == "userEvents"
        assert payload.user == wallet

    def test_candle_subscription_payload(self) -> None:
        """Test candle subscription payload construction."""
        payload = HyperliquidRawWsCandleSubscriptionPayload(
            type="candle", coin="ETH", interval="1m",
        )
        assert payload.type == "candle"
        assert payload.coin == "ETH"
        assert payload.interval == "1m"

    def test_all_mids_subscription_payload(self) -> None:
        """Test allMids subscription payload construction."""
        payload = HyperliquidRawWsAllMidsSubscriptionPayload(type="allMids")
        assert payload.type == "allMids"

    def test_subscribe_request_with_l2book(self) -> None:
        """Test complete subscription request with L2Book payload."""
        inner_payload = HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin="ETH")
        request = HyperliquidRawWsSubscribeRequest(method="subscribe", subscription=inner_payload)

        assert request.method == "subscribe"
        assert isinstance(request.subscription, HyperliquidRawWsL2BookSubscriptionPayload)

        # Test full serialization
        data = request.model_dump(by_alias=True, exclude_none=True)
        assert data == {"method": "subscribe", "subscription": {"type": "l2Book", "coin": "ETH"}}

    def test_subscribe_request_with_user_events(self) -> None:
        """Test complete subscription request with userEvents payload."""
        wallet = "0x1234567890abcdef1234567890abcdef12345678"
        inner_payload = HyperliquidRawWsUserEventsSubscriptionPayload(
            type="userEvents", user=wallet,
        )
        request = HyperliquidRawWsSubscribeRequest(method="subscribe", subscription=inner_payload)

        data = request.model_dump(by_alias=True, exclude_none=True)
        assert data == {
            "method": "subscribe",
            "subscription": {"type": "userEvents", "user": wallet},
        }

    def test_unsubscribe_request(self) -> None:
        """Test unsubscribe request."""
        inner_payload = HyperliquidRawWsAllMidsSubscriptionPayload(type="allMids")
        request = HyperliquidRawWsSubscribeRequest(method="unsubscribe", subscription=inner_payload)

        assert request.method == "unsubscribe"

    def test_invalid_method(self) -> None:
        """Test invalid method raises validation error."""
        invalid_json = '{"method": "SUBSCRIBE", "subscription": {"type": "allMids"}}'
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawWsSubscribeRequest.model_validate_json(invalid_json)
        assert "method" in str(exc_info.value)

    def test_invalid_subscription_type(self) -> None:
        """Test invalid subscription type raises validation error."""
        invalid_json = '{"type": "orderbook", "coin": "ETH"}'
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawWsL2BookSubscriptionPayload.model_validate_json(invalid_json)
        assert "type" in str(exc_info.value)

    def test_missing_required_fields(self) -> None:
        """Test missing required fields raise validation errors."""
        # Missing coin for l2Book
        invalid_json = '{"type": "l2Book"}'
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawWsL2BookSubscriptionPayload.model_validate_json(invalid_json)
        assert "coin" in str(exc_info.value)

        # Missing user for userEvents
        invalid_json = '{"type": "userEvents"}'
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawWsUserEventsSubscriptionPayload.model_validate_json(invalid_json)
        assert "user" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        invalid_json = '{"type": "allMids", "extra_field": "not_allowed"}'
        with pytest.raises(ValidationError) as exc_info:
            HyperliquidRawWsAllMidsSubscriptionPayload.model_validate_json(invalid_json)
        assert "Extra inputs are not permitted" in str(exc_info.value)

    def test_model_immutability(self) -> None:
        """Test that models are frozen (immutable)."""
        payload = HyperliquidRawWsAllMidsSubscriptionPayload(type="allMids")
        with pytest.raises(ValidationError) as exc_info:
            payload.__setattr__("type", "trades")
        assert "Instance is frozen" in str(exc_info.value)
