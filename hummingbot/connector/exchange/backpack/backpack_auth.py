"""Authentication for Backpack Exchange using Ed25519 signatures.
Implements Backpack's instruction-based signing scheme for spot endpoints.
"""

import base64
import json
from typing import Any
from urllib.parse import parse_qs, urlparse

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class BackpackAuth(AuthBase):
    """Backpack Exchange authentication using Ed25519 signatures.

    Implements the authentication pattern required by Backpack:
    - X-API-Key: API key
    - X-Timestamp: Unix timestamp in milliseconds
    - X-Signature: Ed25519 signature of payload
    - X-Window: Request validity window (5000ms)
    """

    def __init__(self, api_key: str, api_secret: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.api_secret = api_secret
        self.time_provider = time_provider

        self._private_key = None
        if api_secret:
            try:
                private_key_bytes = base64.b64decode(api_secret)
                self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(private_key_bytes)
            except Exception as e:
                raise ValueError(
                    "Invalid API secret format. Expected base64 encoded Ed25519 private key: "
                    f"{e}"
                ) from e

    def _get_timestamp(self) -> int:
        """Get current timestamp in milliseconds."""
        return int(self.time_provider.time() * 1e3)

    def _get_instruction_for_endpoint(self, method: str, path: str) -> str:
        """Get the instruction string for a given method and path."""
        lookup_path = path.split("?", maxsplit=1)[0] if "?" in path else path
        if not lookup_path.startswith("/"):
            lookup_path = f"/{lookup_path}"
        instruction = CONSTANTS.INSTRUCTION_MAP.get((method.upper(), lookup_path))
        if not instruction:
            instruction = f"{method.lower()}Query"
        return instruction

    def _generate_signature(self, payload: str) -> str:
        """Generate Ed25519 signature for the given payload."""
        if self._private_key is None:
            raise ValueError("API secret is required to generate signed requests.")
        try:
            signature_bytes = self._private_key.sign(payload.encode("utf-8"))
            return base64.b64encode(signature_bytes).decode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to generate signature: {e}") from e

    def _build_signature_payload(
        self,
        timestamp: str,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: str | None = None,
        window: str = str(CONSTANTS.AUTH_WINDOW_MS),
    ) -> str:
        """Build the payload string for signing using instruction-based format."""
        instruction = self._get_instruction_for_endpoint(method, path)
        payload_parts = [f"instruction={instruction}"]

        method = method.upper()
        if method == "GET" and params:
            sorted_params = sorted(params.items())
            for key, value in sorted_params:
                if value is not None:
                    formatted_value = "true" if value is True else "false" if value is False else value
                    payload_parts.append(f"{key}={formatted_value}")
        elif method in ["POST", "PUT", "DELETE", "PATCH"] and body:
            try:
                body_dict = json.loads(body) if isinstance(body, str) else body
                sorted_params = sorted(body_dict.items())
                for key, value in sorted_params:
                    if value is not None:
                        formatted_value = "true" if value is True else "false" if value is False else value
                        payload_parts.append(f"{key}={formatted_value}")
            except (json.JSONDecodeError, TypeError):
                pass

        payload_parts.extend((f"timestamp={timestamp}", f"window={window}"))
        return "&".join(payload_parts)

    def _generate_auth_headers(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: str | None = None,
    ) -> dict[str, str]:
        """Generate authentication headers for a request."""
        timestamp = str(self._get_timestamp())
        window = str(CONSTANTS.AUTH_WINDOW_MS)

        signature_payload = self._build_signature_payload(
            timestamp=timestamp,
            method=method,
            path=path,
            params=params,
            body=body,
            window=window,
        )

        signature = self._generate_signature(signature_payload)

        return {
            "X-API-Key": self.api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
            "X-Window": window,
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """Add authentication headers to REST request."""
        url = request.url or ""
        method = request.method.name

        if "?" in url:
            path = url.split("?")[0]
            query_string = url.split("?")[1]
            full_path = f"{path}?{query_string}"
        else:
            path = url
            full_path = path

        if full_path.startswith("http"):
            parsed = urlparse(full_path)
            full_path = parsed.path
            if parsed.query:
                full_path += f"?{parsed.query}"

        params = None
        clean_path = full_path
        if "?" in full_path and method == "GET":
            clean_path, query_string = full_path.split("?", 1)
            params = {k: v[0] for k, v in parse_qs(query_string).items()}
        elif request.params:
            params = dict(request.params) if request.params else None

        auth_headers = self._generate_auth_headers(
            method=method,
            path=clean_path,
            params=params,
            body=request.data,
        )

        if request.headers is None:
            request.headers = {}

        headers_dict = dict(request.headers) if request.headers else {}
        headers_dict.update(auth_headers)
        request.headers = headers_dict

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """Add authentication to WebSocket request."""
        return request

    def get_ws_auth_message(self) -> dict[str, Any]:
        """Generate WebSocket authentication message."""
        timestamp = str(self._get_timestamp())
        window = str(CONSTANTS.AUTH_WINDOW_MS)

        auth_payload = f"instruction={CONSTANTS.WS_AUTH_INSTRUCTION}&timestamp={timestamp}&window={window}"
        signature = self._generate_signature(auth_payload)

        return {
            "method": CONSTANTS.WS_AUTH_MESSAGE_METHOD,
            "params": {
                "apiKey": self.api_key,
                "timestamp": timestamp,
                "signature": signature,
                "window": window,
            },
        }
