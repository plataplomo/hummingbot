"""WebSocket Schema Export Utilities.

This module provides utilities for exporting JSON schemas from Pydantic models
for API documentation, client SDK generation, and contract testing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.base.schema_export import SchemaExportMode
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import HyperliquidUserEventEnvelope


class SchemaExporter:
    """Export JSON schemas for WebSocket models."""

    def __init__(self, output_dir: Path | str = "schemas/websocket") -> None:
        """Initialize schema exporter.

        Args:
            output_dir: Directory to export schemas to
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_model_schema(
        self,
        model: type[BaseModel],
        filename: str | None = None,
        export_mode: SchemaExportMode = SchemaExportMode.WITH_EXAMPLES,
    ) -> dict[str, Any]:
        """Export a single model's JSON schema.

        Args:
            model: Pydantic model class to export
            filename: Optional filename (defaults to model name)
            export_mode: Schema export mode with example inclusion policy

        Returns:
            The exported schema dictionary
        """
        # Generate schema based on export mode
        schema = model.model_json_schema(
            by_alias=True,
            mode="validation",
        )

        # Adjust schema based on export mode
        if export_mode == SchemaExportMode.MINIMAL:
            # Remove examples from schema
            if "examples" in schema:
                del schema["examples"]
            # Remove examples from properties
            if "properties" in schema:
                for prop in schema["properties"].values():
                    if isinstance(prop, dict) and "examples" in prop:
                        del prop["examples"]
        elif export_mode == SchemaExportMode.API_DOCUMENTATION:
            # Add API documentation specific metadata
            schema["x-api-version"] = "2.0"
            schema["x-generated-by"] = "CyberDelta Schema Exporter"

        # Add metadata
        schema["$schema"] = "http://json-schema.org/draft-07/schema#"
        schema["$id"] = f"https://cyberdelta.com/schemas/{model.__name__}.json"

        # Save to file if filename provided
        if filename is None:
            filename = f"{model.__name__}.json"

        output_path = self.output_dir / filename
        output_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        return schema

    def export_all_websocket_schemas(self) -> dict[str, dict[str, Any]]:
        """Export all WebSocket model schemas.

        Returns:
            Dictionary mapping model names to their schemas
        """
        models: dict[str, type[BaseModel]] = {
            # Backpack models
            "BackpackRawWebSocketEnvelope": BackpackRawWebSocketEnvelope,
            # Hyperliquid models
            "HyperliquidUserEventEnvelope": HyperliquidUserEventEnvelope,
        }

        schemas: dict[str, dict[str, Any]] = {}
        for name, model in models.items():
            try:
                schema = self.export_model_schema(model)
                schemas[name] = schema
            except ValidationError:
                # Skip models that fail schema generation
                continue

        # Create index file
        index = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "CyberDelta WebSocket Schema Index",
            "description": "Index of all WebSocket message schemas",
            "schemas": list(schemas.keys()),
            "generated": str(Path.cwd()),
        }

        index_path = self.output_dir / "index.json"
        index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

        return schemas

    def generate_openapi_spec(self) -> dict[str, Any]:
        """Generate OpenAPI specification for WebSocket APIs.

        Returns:
            OpenAPI 3.1 specification dictionary
        """
        return {
            "openapi": "3.1.0",
            "info": {
                "title": "CyberDelta WebSocket API",
                "version": "2.0.0",
                "description": "WebSocket API for real-time cryptocurrency trading data",
            },
            "servers": [
                {
                    "url": "wss://api.backpack.exchange",
                    "description": "Backpack WebSocket API",
                    "protocol": "wss",
                },
                {
                    "url": "wss://api.hyperliquid.xyz",
                    "description": "Hyperliquid WebSocket API",
                    "protocol": "wss",
                },
            ],
            "channels": {
                "/depth.{symbol}": {
                    "parameters": {
                        "symbol": {
                            "description": "Trading pair symbol (e.g., BTC_USDC)",
                            "schema": {"type": "string"},
                        }
                    },
                    "subscribe": {
                        "summary": "Subscribe to order book depth updates",
                        "message": {"$ref": "#/components/messages/BackpackDepthUpdate"},
                    },
                },
                "/l2Book": {
                    "subscribe": {
                        "summary": "Subscribe to Hyperliquid L2 book updates",
                        "message": {"$ref": "#/components/messages/HyperliquidL2BookUpdate"},
                    },
                },
            },
            "components": {
                "messages": {
                    "BackpackDepthUpdate": {
                        "contentType": "application/json",
                        "payload": {"$ref": "#/components/schemas/BackpackRawWebSocketEnvelope"},
                    },
                    "HyperliquidUserEvent": {
                        "contentType": "application/json",
                        "payload": {"$ref": "#/components/schemas/HyperliquidUserEventEnvelope"},
                    },
                },
                "schemas": {
                    # Schemas will be populated from model exports
                },
            },
        }


# Example usage
if __name__ == "__main__":
    exporter = SchemaExporter()

    # Export all schemas
    schemas = exporter.export_all_websocket_schemas()

    # Generate OpenAPI spec
    openapi_spec = exporter.generate_openapi_spec()

    # Add schemas to OpenAPI spec
    openapi_spec["components"]["schemas"] = schemas

    # Save OpenAPI spec
    openapi_path = exporter.output_dir / "openapi.json"
    openapi_path.write_text(json.dumps(openapi_spec, indent=2), encoding="utf-8")

    # Schema export completed successfully
    # Generated OpenAPI specification
