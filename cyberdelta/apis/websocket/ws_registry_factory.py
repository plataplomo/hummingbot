"""WebSocket Registry Factory - Eliminates circular imports.

This factory creates WebSocket context registries. Each exchange is responsible
for registering itself when needed, avoiding circular dependencies.

Architecture:
- Factory creates empty registries
- Each exchange registers itself when initialized
- No global registry instance - created on demand
- Complete separation of concerns
"""

from __future__ import annotations

from cyberdelta.apis.websocket.ws_context_registry import WebSocketContextRegistry


class WebSocketRegistryFactory:
    """Factory for creating fully configured WebSocket context registries.

    This factory eliminates circular import issues by:
    1. Importing initialization functions at module level (safe imports)
    2. Creating registry instances on demand
    3. Pre-registering all supported exchanges
    4. Providing dependency injection pattern
    """

    @staticmethod
    def create_registry() -> WebSocketContextRegistry:
        """Create an empty registry for WebSocket contexts.

        This method creates a clean registry instance. Each exchange should
        register itself when initialized.

        Returns:
            Empty WebSocketContextRegistry instance

        Examples:
            >>> registry = WebSocketRegistryFactory.create_registry()
            >>> # Exchange registers itself
            >>> from cyberdelta.apis.backpack.bp_ws_init import initialize_backpack_ws
            >>> initialize_backpack_ws(registry)
        """
        return WebSocketContextRegistry()

    @staticmethod
    def create_configured_registry() -> WebSocketContextRegistry:
        """Create registry with exchanges self-registering when needed.

        Note: This method returns an empty registry. Each exchange is responsible
        for registering itself when initialized to avoid circular imports.

        Returns:
            Empty WebSocketContextRegistry instance
        """
        # Return empty registry - exchanges will register themselves when needed
        return WebSocketContextRegistry()

    @staticmethod
    def create_empty_registry() -> WebSocketContextRegistry:
        """Create an empty registry for testing purposes.

        Returns:
            Empty WebSocketContextRegistry instance
        """
        return WebSocketContextRegistry()
