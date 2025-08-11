"""Real unit tests for workflow registry that test actual business logic."""

import pytest

from cyberdelta.exceptions import RequiredFieldError
from cyberdelta.models.events.workflow import BaseWorkflowEvent
from cyberdelta.models.events.workflow_context import WorkflowContextModel
from cyberdelta.orchestration.registry import WorkflowRegistry


class RealWorkflowHandler:
    """Real workflow handler that implements actual logic."""

    def __init__(self, name: str, should_fail: bool = False) -> None:
        """Initialize real handler.

        Args:
            name: Handler name for identification
            should_fail: Whether handler should fail during execution
        """
        self.name = name
        self.should_fail = should_fail
        self.execution_count = 0

    async def execute(self, event: BaseWorkflowEvent) -> WorkflowContextModel:
        """Execute with real logic and counting.

        Args:
            event: Event to execute

        Returns:
            WorkflowContextModel with execution results

        Raises:
            RuntimeError: If should_fail is True
        """
        self.execution_count += 1

        if self.should_fail:
            raise RuntimeError(f"Handler {self.name} intentionally failed")

        return WorkflowContextModel(
            workflow_id=f"workflow-{self.name}-{self.execution_count}",
            workflow_type=f"Real{self.name}",
            timeout=event.timeout,
        )


class TestWorkflowRegistryValidation:
    """Test WorkflowRegistry with real validation and business logic."""

    def test_empty_event_type_validation(self) -> None:
        """Test that empty event types are properly rejected."""
        registry = WorkflowRegistry()
        handler = RealWorkflowHandler("TestHandler")

        # Test various empty/invalid event type scenarios
        invalid_event_types = ["", "   ", "\t", "\n"]

        for invalid_type in invalid_event_types:
            with pytest.raises(RequiredFieldError) as exc_info:
                registry.register_handler(invalid_type, handler)

            # Verify error details
            assert exc_info.value.field_name == "event_type"
            assert "workflow handler registration" in str(exc_info.value)

            # Registry should remain empty
            assert registry.list_event_types() == []

    def test_handler_identity_preservation(self) -> None:
        """Test that handlers maintain their identity through registry operations."""
        registry = WorkflowRegistry()

        # Create handlers with different characteristics
        handler1 = RealWorkflowHandler("Handler1", should_fail=False)
        handler2 = RealWorkflowHandler("Handler2", should_fail=True)

        registry.register_handler("event1", handler1)
        registry.register_handler("event2", handler2)

        # Retrieved handlers should be exactly the same objects
        retrieved1 = registry.get_handler("event1")
        retrieved2 = registry.get_handler("event2")

        assert retrieved1 is handler1  # Same object reference
        assert retrieved2 is handler2  # Same object reference

        # Type-safe access to handler properties
        assert isinstance(retrieved1, RealWorkflowHandler)
        assert isinstance(retrieved2, RealWorkflowHandler)

        # All handler properties should be preserved
        assert retrieved1.name == "Handler1"
        assert retrieved1.should_fail is False
        assert retrieved2.name == "Handler2"
        assert retrieved2.should_fail is True

    def test_handler_override_behavior(self) -> None:
        """Test real handler override behavior with execution tracking."""
        registry = WorkflowRegistry()

        # Register original handler
        original_handler = RealWorkflowHandler("OriginalHandler")
        registry.register_handler("test_event", original_handler)

        # Verify original is registered
        assert registry.get_handler("test_event") is original_handler
        assert len(registry.list_event_types()) == 1

        # Override with new handler
        new_handler = RealWorkflowHandler("NewHandler")
        registry.register_handler("test_event", new_handler)

        # Verify new handler replaced original
        retrieved = registry.get_handler("test_event")
        assert retrieved is new_handler
        assert retrieved is not original_handler

        assert isinstance(retrieved, RealWorkflowHandler)
        assert retrieved.name == "NewHandler"

        # Should still have only one event type
        assert len(registry.list_event_types()) == 1

    @pytest.mark.asyncio
    async def test_handler_execution_through_registry(self) -> None:
        """Test that handlers retrieved from registry can actually execute."""
        registry = WorkflowRegistry()

        # Register working handler
        working_handler = RealWorkflowHandler("WorkingHandler")
        registry.register_handler("working_event", working_handler)

        # Register failing handler
        failing_handler = RealWorkflowHandler("FailingHandler", should_fail=True)
        registry.register_handler("failing_event", failing_handler)

        # Test working handler execution
        working_retrieved = registry.get_handler("working_event")
        assert working_retrieved is not None
        event = BaseWorkflowEvent(event_type="working_event", timeout=30.0)

        result = await working_retrieved.execute(event)

        assert result.workflow_id == "workflow-WorkingHandler-1"
        assert result.workflow_type == "RealWorkingHandler"
        assert working_handler.execution_count == 1

        # Test failing handler execution
        failing_retrieved = registry.get_handler("failing_event")
        assert failing_retrieved is not None
        failing_event = BaseWorkflowEvent(event_type="failing_event", timeout=30.0)

        with pytest.raises(RuntimeError, match="Handler FailingHandler intentionally failed"):
            await failing_retrieved.execute(failing_event)

        assert failing_handler.execution_count == 1

    def test_concurrent_handler_registration(self) -> None:
        """Test registry behavior with multiple concurrent registrations."""
        registry = WorkflowRegistry()

        # Register multiple handlers for different event types
        handlers: list[RealWorkflowHandler] = []
        event_types: list[str] = []

        for i in range(10):
            handler = RealWorkflowHandler(f"Handler{i}")
            event_type = f"event_type_{i}"

            handlers.append(handler)
            event_types.append(event_type)
            registry.register_handler(event_type, handler)

        # Verify all handlers are properly registered
        registered_types = registry.list_event_types()
        assert len(registered_types) == 10
        assert set(registered_types) == set(event_types)

        # Verify each handler can be retrieved correctly
        for i, (event_type, original_handler) in enumerate(zip(event_types, handlers, strict=True)):
            retrieved = registry.get_handler(event_type)
            assert retrieved is original_handler
            assert isinstance(retrieved, RealWorkflowHandler)
            assert retrieved.name == f"Handler{i}"

    def test_case_sensitive_event_type_handling(self) -> None:
        """Test that event types are case-sensitive."""
        registry = WorkflowRegistry()

        handler1 = RealWorkflowHandler("LowerHandler")
        handler2 = RealWorkflowHandler("UpperHandler")
        handler3 = RealWorkflowHandler("MixedHandler")

        # Register handlers with different case variations
        registry.register_handler("test_event", handler1)
        registry.register_handler("TEST_EVENT", handler2)
        registry.register_handler("Test_Event", handler3)

        # All should be treated as different event types
        assert len(registry.list_event_types()) == 3
        assert registry.get_handler("test_event") is handler1
        assert registry.get_handler("TEST_EVENT") is handler2
        assert registry.get_handler("Test_Event") is handler3

        # Non-matching case should return None
        assert registry.get_handler("Test_event") is None
        assert registry.get_handler("tEST_EVENT") is None

    def test_special_character_event_types(self) -> None:
        """Test event types with special characters and edge cases."""
        registry = WorkflowRegistry()

        # Test various special characters
        special_cases = [
            "event-with-dashes",
            "event_with_underscores",
            "event.with.dots",
            "event:with:colons",
            "event|with|pipes",
            "event123with456numbers",
            "eventWithCamelCase",
            "EVENTALLCAPS",
            "event with spaces",  # Spaces should be allowed
            "event\twith\ttabs",  # Tabs should be allowed
        ]

        handlers: list[RealWorkflowHandler] = []
        for i, event_type in enumerate(special_cases):
            handler = RealWorkflowHandler(f"SpecialHandler{i}")
            handlers.append(handler)
            registry.register_handler(event_type, handler)

        # Verify all special cases work
        registered_types = registry.list_event_types()
        assert len(registered_types) == len(special_cases)

        for event_type, handler in zip(special_cases, handlers, strict=True):
            retrieved = registry.get_handler(event_type)
            assert retrieved is handler

    def test_registry_state_isolation(self) -> None:
        """Test that multiple registry instances are completely isolated."""
        registry1 = WorkflowRegistry()
        registry2 = WorkflowRegistry()

        handler1 = RealWorkflowHandler("Registry1Handler")
        handler2 = RealWorkflowHandler("Registry2Handler")

        # Register same event type in both registries
        registry1.register_handler("shared_event", handler1)
        registry2.register_handler("shared_event", handler2)

        # Each registry should have its own handler
        assert registry1.get_handler("shared_event") is handler1
        assert registry2.get_handler("shared_event") is handler2

        # Different registries shouldn't interfere
        registry1.register_handler("unique1", handler1)
        registry2.register_handler("unique2", handler2)

        assert registry1.get_handler("unique1") is handler1
        assert registry1.get_handler("unique2") is None
        assert registry2.get_handler("unique1") is None
        assert registry2.get_handler("unique2") is handler2

    def test_clear_operation_completeness(self) -> None:
        """Test that clear operation completely resets registry state."""
        registry = WorkflowRegistry()

        # Register multiple handlers
        handlers: list[RealWorkflowHandler] = []
        for i in range(5):
            handler = RealWorkflowHandler(f"Handler{i}")
            handlers.append(handler)
            registry.register_handler(f"event_{i}", handler)

        # Verify registry is populated
        assert len(registry.list_event_types()) == 5
        for i in range(5):
            assert registry.get_handler(f"event_{i}") is handlers[i]

        # Clear registry
        registry.clear()

        # Verify complete reset
        assert registry.list_event_types() == []
        for i in range(5):
            assert registry.get_handler(f"event_{i}") is None

        # Registry should be ready for new registrations
        new_handler = RealWorkflowHandler("NewHandler")
        registry.register_handler("new_event", new_handler)

        assert registry.list_event_types() == ["new_event"]
        assert registry.get_handler("new_event") is new_handler

    @pytest.mark.asyncio
    async def test_handler_state_preservation_across_operations(self) -> None:
        """Test that handler state is preserved across registry operations."""
        registry = WorkflowRegistry()

        # Create handler with state
        handler = RealWorkflowHandler("StateHandler")
        registry.register_handler("state_event", handler)

        # Execute handler to change its state
        retrieved = registry.get_handler("state_event")
        assert retrieved is not None
        event = BaseWorkflowEvent(event_type="state_event", timeout=30.0)

        await retrieved.execute(event)
        assert handler.execution_count == 1

        # Re-retrieve and execute again
        retrieved_again = registry.get_handler("state_event")
        assert retrieved_again is not None
        await retrieved_again.execute(event)

        # State should be preserved (same handler instance)
        assert handler.execution_count == 2
        assert retrieved is retrieved_again is handler

    def test_nonexistent_handler_retrieval(self) -> None:
        """Test behavior when retrieving handlers that don't exist."""
        registry = WorkflowRegistry()

        # Empty registry should return None for any event type
        assert registry.get_handler("nonexistent") is None
        assert registry.get_handler("") is None
        assert registry.get_handler("any_random_type") is None

        # Register one handler
        handler = RealWorkflowHandler("OnlyHandler")
        registry.register_handler("existing_event", handler)

        # Should still return None for non-matching types
        assert registry.get_handler("nonexistent") is None
        assert registry.get_handler("existing_event_typo") is None
        assert registry.get_handler("EXISTING_EVENT") is None  # Case sensitive

        # But should return handler for correct type
        assert registry.get_handler("existing_event") is handler

    def test_event_type_listing_consistency(self) -> None:
        """Test that list_event_types returns consistent and accurate results."""
        registry = WorkflowRegistry()

        # Empty registry
        assert registry.list_event_types() == []

        # Add handlers incrementally and verify listing
        event_types = ["alpha", "beta", "gamma", "delta"]
        registered_types: list[str] = []

        for event_type in event_types:
            handler = RealWorkflowHandler(f"Handler_{event_type}")
            registry.register_handler(event_type, handler)
            registered_types.append(event_type)

            # List should contain exactly what we've registered so far
            current_list = registry.list_event_types()
            assert len(current_list) == len(registered_types)
            assert set(current_list) == set(registered_types)

        # Override a handler - should not change the event type list
        override_handler = RealWorkflowHandler("OverrideHandler")
        registry.register_handler("alpha", override_handler)

        final_list = registry.list_event_types()
        assert len(final_list) == len(event_types)
        assert set(final_list) == set(event_types)
        assert registry.get_handler("alpha") is override_handler
