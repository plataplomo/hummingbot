"""Validation rule registry for unified validation framework.

This module provides the registry that manages all validation rules,
organizing them by category and execution priority.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ValidationCategory


if TYPE_CHECKING:
    from cyberdelta.protocols.validation import ValidationRule

logger = get_logger(__name__)


class ValidationRegistry:
    """Registry for managing validation rules by category.

    The registry organizes rules by their category for efficient execution
    in priority order. Rules within a category are executed in the order
    they were registered.

    This implements the ValidationRegistry protocol defined in protocols/validation.py.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded rule ordering
    - Uses ValidationCategory enum
    - Explicit registration required
    """

    def __init__(self) -> None:
        """Initialize empty validation registry.

        Rules must be explicitly registered after initialization.
        """
        # Use defaultdict to automatically create empty lists for new categories
        self._rules: dict[ValidationCategory, list[ValidationRule]] = defaultdict(list)

        # Track registration order for debugging/logging
        self._registration_order: list[tuple[ValidationCategory, str]] = []

        logger.info("validation_registry_initialized")

    def register(self, rule: ValidationRule) -> None:
        """Register a validation rule in the registry.

        Rules are added to their category and will be executed in
        the order they were registered within that category.

        Args:
            rule: ValidationRule to register

        Note:
            - Duplicate rules (same name in same category) are allowed
            - Rules are executed in registration order within category
        """
        category = rule.category
        rule_name = rule.name

        # Add to category list
        self._rules[category].append(rule)

        # Track registration order
        self._registration_order.append((category, rule_name))

        logger.debug(
            "validation_rule_registered",
            rule_name=rule_name,
            category=category.value,
            enabled=rule.enabled,
            rules_in_category=len(self._rules[category]),
        )

    def get_rules(self, category: ValidationCategory | None = None) -> list[ValidationRule]:
        """Get validation rules, optionally filtered by category.

        Args:
            category: Optional category to filter by. If None, returns all rules.

        Returns:
            List of validation rules in registration order

        Note:
            - Returns all rules, both enabled and disabled
            - Use get_enabled_rules() to get only enabled rules
        """
        if category is not None:
            # Return rules for specific category
            return list(self._rules.get(category, []))

        # Return all rules in category priority order
        all_rules: list[ValidationRule] = []

        # Categories are processed in enum definition order (priority order)
        for cat in ValidationCategory:
            all_rules.extend(self._rules.get(cat, []))

        return all_rules

    def get_enabled_rules(self, category: ValidationCategory | None = None) -> list[ValidationRule]:
        """Get only enabled validation rules.

        Args:
            category: Optional category to filter by. If None, returns all enabled rules.

        Returns:
            List of enabled validation rules in registration order

        Note:
            - Filters out disabled rules
            - Maintains registration order within categories
        """
        rules = self.get_rules(category)
        return [rule for rule in rules if rule.enabled]

    def get_rules_by_category(self) -> dict[ValidationCategory, list[ValidationRule]]:
        """Get all rules organized by category.

        Returns:
            Dictionary mapping categories to their rules

        Note:
            - Includes both enabled and disabled rules
            - Useful for reporting/debugging
        """
        return dict(self._rules)

    def get_enabled_rules_by_category(self) -> dict[ValidationCategory, list[ValidationRule]]:
        """Get enabled rules organized by category.

        Returns:
            Dictionary mapping categories to their enabled rules

        Note:
            - Filters out disabled rules
            - Useful for execution planning
        """
        result: dict[ValidationCategory, list[ValidationRule]] = {}
        for category, rules in self._rules.items():
            enabled = [rule for rule in rules if rule.enabled]
            if enabled:  # Only include categories with enabled rules
                result[category] = enabled
        return result

    def count_rules(self, enabled_only: bool = False) -> dict[ValidationCategory, int]:
        """Count rules by category.

        Args:
            enabled_only: If True, count only enabled rules

        Returns:
            Dictionary mapping categories to rule counts
        """
        if enabled_only:
            return {
                category: len([r for r in rules if r.enabled])
                for category, rules in self._rules.items()
            }
        return {category: len(rules) for category, rules in self._rules.items()}

    def clear(self) -> None:
        """Clear all registered rules.

        Useful for testing or reconfiguration.
        """
        self._rules.clear()
        self._registration_order.clear()
        logger.info("validation_registry_cleared")

    def remove_rule(self, rule_name: str, category: ValidationCategory | None = None) -> bool:
        """Remove a rule by name.

        Args:
            rule_name: Name of the rule to remove
            category: Optional category to search in. If None, searches all categories.

        Returns:
            True if rule was found and removed, False otherwise
        """
        removed = False

        if category is not None:
            # Remove from specific category
            rules = self._rules.get(category, [])
            original_count = len(rules)
            self._rules[category] = [r for r in rules if r.name != rule_name]
            removed = len(self._rules[category]) < original_count
        else:
            # Remove from all categories
            for cat in ValidationCategory:
                rules = self._rules.get(cat, [])
                original_count = len(rules)
                self._rules[cat] = [r for r in rules if r.name != rule_name]
                if len(self._rules[cat]) < original_count:
                    removed = True

        if removed:
            # Update registration order
            self._registration_order = [
                (cat, name) for cat, name in self._registration_order if name != rule_name
            ]
            logger.debug("validation_rule_removed", rule_name=rule_name)

        return removed

    def get_registration_order(self) -> list[tuple[ValidationCategory, str]]:
        """Get the registration order of all rules.

        Returns:
            List of (category, rule_name) tuples in registration order

        Note:
            - Useful for debugging and reporting
            - Shows the exact order rules were registered
        """
        return list(self._registration_order)

    def __repr__(self) -> str:
        """String representation of the registry.

        Returns:
            Summary of registered rules by category
        """
        counts = self.count_rules(enabled_only=False)
        enabled_counts = self.count_rules(enabled_only=True)

        parts: list[str] = []
        for category in ValidationCategory:
            total = counts.get(category, 0)
            enabled = enabled_counts.get(category, 0)
            if total > 0:
                parts.append(f"{category.value}={enabled}/{total}")

        return f"ValidationRegistry({', '.join(parts)})"
