"""Tests for Backpack WebSocket subscription payload models."""

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackRawWsSubscriptionRequest


class TestBackpackWsPayloads:
    """Test Backpack WebSocket subscription payload models."""

    def test_public_stream_subscription(self) -> None:
        """Test public stream subscription without signature."""
        request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=["depth.ETH_USDC"])

        assert request.method == "SUBSCRIBE"
        assert request.params == ["depth.ETH_USDC"]
        assert request.signature is None

        # Test serialization
        data = request.model_dump(by_alias=True, exclude_none=True)
        assert data == {"method": "SUBSCRIBE", "params": ["depth.ETH_USDC"]}

    def test_multiple_stream_subscription(self) -> None:
        """Test subscribing to multiple streams at once."""
        request = BackpackRawWsSubscriptionRequest(
            method="SUBSCRIBE", params=["ticker.BTC_USDC", "trades.ETH_USDC", "depth.SOL_USDC"],
        )

        assert len(request.params) == 3
        assert "ticker.BTC_USDC" in request.params
        assert "trades.ETH_USDC" in request.params
        assert "depth.SOL_USDC" in request.params

    def test_private_stream_subscription_with_signature(self) -> None:
        """Test private stream subscription with signature."""
        request = BackpackRawWsSubscriptionRequest(
            method="SUBSCRIBE",
            params=["account.orderUpdate"],
            signature=(
                "base64encodedverifyingkey==",
                "base64encodedsignature==",
                "1234567890",
                "5000",
            ),
        )

        assert request.method == "SUBSCRIBE"
        assert request.params == ["account.orderUpdate"]
        assert request.signature is not None
        assert len(request.signature) == 4
        assert request.signature[0] == "base64encodedverifyingkey=="
        assert request.signature[1] == "base64encodedsignature=="
        assert request.signature[2] == "1234567890"
        assert request.signature[3] == "5000"

        # Test serialization includes signature
        data = request.model_dump(by_alias=True, exclude_none=True)
        assert "signature" in data
        # Pydantic keeps tuples as tuples in model_dump, but they convert to lists in JSON
        expected_signature = (
            "base64encodedverifyingkey==",
            "base64encodedsignature==",
            "1234567890",
            "5000",
        )
        assert data["signature"] == expected_signature

    def test_unsubscribe_request(self) -> None:
        """Test unsubscribe request."""
        request = BackpackRawWsSubscriptionRequest(method="UNSUBSCRIBE", params=["ticker.BTC_USDC"])

        assert request.method == "UNSUBSCRIBE"
        assert request.params == ["ticker.BTC_USDC"]

    def test_empty_params_valid(self) -> None:
        """Test that empty params list is valid."""
        request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=[])

        assert request.params == []

    def test_invalid_method(self) -> None:
        """Test invalid method raises validation error."""
        invalid_json = '{"method": "subscribe", "params": ["ticker.BTC_USDC"]}'
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawWsSubscriptionRequest.model_validate_json(invalid_json)
        assert "method" in str(exc_info.value)

    def test_invalid_signature_tuple_length(self) -> None:
        """Test signature with wrong number of elements."""
        invalid_json = """
        {
            "method": "SUBSCRIBE",
            "params": ["account.orderUpdate"],
            "signature": ["key", "sig", "timestamp"]
        }
        """
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawWsSubscriptionRequest.model_validate_json(invalid_json)
        assert "signature" in str(exc_info.value)

    def test_model_immutability(self) -> None:
        """Test that models are frozen (immutable)."""
        request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=["ticker.BTC_USDC"])

        with pytest.raises(ValidationError) as exc_info:
            request.__setattr__("method", "UNSUBSCRIBE")
        assert "Instance is frozen" in str(exc_info.value)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields are forbidden."""
        invalid_json = """
        {
            "method": "SUBSCRIBE",
            "params": ["ticker.BTC_USDC"],
            "extra_field": "not_allowed"
        }
        """
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawWsSubscriptionRequest.model_validate_json(invalid_json)
        assert "Extra inputs are not permitted" in str(exc_info.value)

    def test_long_stream_names_valid(self) -> None:
        """Test that stream names up to 128 chars are valid."""
        long_stream = "a" * 128
        request = BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=[long_stream])

        assert request.params[0] == long_stream

    def test_too_long_stream_name_invalid(self) -> None:
        """Test that stream names over 128 chars are invalid."""
        too_long_stream = "a" * 129
        with pytest.raises(ValidationError) as exc_info:
            BackpackRawWsSubscriptionRequest(method="SUBSCRIBE", params=[too_long_stream])
        assert "String value too long (max 128 chars)" in str(exc_info.value)

    def test_signature_serialization_consistency(self) -> None:
        """Test that signature tuple is consistent in serialization."""
        request = BackpackRawWsSubscriptionRequest(
            method="SUBSCRIBE",
            params=["fills"],
            signature=("verifying_key", "signature", "1234567890", "5000"),
        )

        # model_dump preserves tuple structure
        data = request.model_dump(by_alias=True)
        assert isinstance(data["signature"], tuple)
        assert data["signature"] == ("verifying_key", "signature", "1234567890", "5000")

        # JSON serialization would convert to list
        import json

        json_str = request.model_dump_json(by_alias=True)
        parsed = json.loads(json_str)
        assert isinstance(parsed["signature"], list)
        assert parsed["signature"] == ["verifying_key", "signature", "1234567890", "5000"]
