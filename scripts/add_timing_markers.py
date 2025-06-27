#!/usr/bin/env python3
"""Script to add @pytest.mark.timing to test files that use timing operations."""

import re
import sys
from pathlib import Path


# Files that need timing marker based on analysis
FILES_NEEDING_MARKER = [
    # Unit tests
    "tests/unit/apis/connectivity/test_ws_manager_pydantic.py",
    "tests/unit/core/execution/orders/test_market_order.py",
    # Integration tests - Backpack
    "tests/integration/apis/backpack/account/positions/test_bp_positions_positive.py",
    "tests/integration/apis/backpack/perp/positions/test_bp_perp_positions_large.py",
    "tests/integration/apis/backpack/perp/positions/test_bp_perp_positions_positive.py",
    "tests/integration/apis/backpack/perp/positions/test_bp_perp_positions_zero.py",
    "tests/integration/apis/backpack/websockets/test_bp_websocket_api.py",
    "tests/integration/apis/backpack/websockets/test_bp_websocket_subscriptions.py",
    # Integration tests - Hyperliquid
    "tests/integration/apis/hyperliquid/perp/orders/test_hl_perp_order_create_and_cancel.py",
    "tests/integration/apis/hyperliquid/perp/orders/test_hl_perp_order_placement_cancel_all.py",
    "tests/integration/apis/hyperliquid/perp/orders/test_hl_perp_orders_private.py",
    "tests/integration/apis/hyperliquid/websockets/test_hl_api_ws_integration.py",
    # Integration tests - Core
    "tests/integration/core/execution/orders/test_market_order_hyperliquid.py",
    "tests/integration/core/test_execution_handler.py",
    # Integration tests - Root
    "tests/integration/test_core_workflow.py",
    "tests/integration/test_data_handler_integration.py",
    "tests/integration/test_failure_scenarios.py",
    "tests/integration/validation/test_circuit_breaker.py",
    "tests/integration/validation/test_funding_rate_validator.py",
]


def check_timing_operations(content: str) -> bool:
    """Check if file contains timing operations."""
    timing_patterns = [
        r"asyncio\.sleep",
        r"time\.sleep",
        r"asyncio\.wait_for",
        r"asyncio\.timeout",
        r"wait_for_condition.*timeout",
        r"\.sleep\(",
        r"timeout\s*=",
    ]

    return any(re.search(pattern, content) for pattern in timing_patterns)


def has_timing_marker(content: str) -> bool:
    """Check if file already has timing marker."""
    return bool(re.search(r"pytestmark\s*=.*pytest\.mark\.timing", content))


def add_timing_marker(file_path: Path) -> tuple[bool, str]:
    """Add timing marker to a test file."""
    try:
        content = file_path.read_text()

        # Skip if already has marker
        if has_timing_marker(content):
            return True, f"✓ {file_path} - already has timing marker"

        # Skip if no timing operations
        if not check_timing_operations(content):
            return True, f"⚠ {file_path} - no timing operations found"

        # Find the right place to insert the marker
        lines = content.split("\n")
        insert_index = -1

        # Look for the last import statement
        for i, line in enumerate(lines):
            if line.strip() and (line.startswith("import ") or line.startswith("from ")):
                insert_index = i

        # If we found imports, add after them
        if insert_index >= 0:
            # Check if there's already a blank line after imports
            next_index = insert_index + 1
            while next_index < len(lines) and not lines[next_index].strip():
                next_index += 1

            # Insert the marker
            if next_index < len(lines):
                lines.insert(next_index, "pytestmark = pytest.mark.timing")
                lines.insert(next_index + 1, "")
            else:
                lines.append("")
                lines.append("pytestmark = pytest.mark.timing")
                lines.append("")

            # Write back
            file_path.write_text("\n".join(lines))
            return True, f"✅ {file_path} - added timing marker"
        return False, f"❌ {file_path} - could not find insertion point"

    except Exception as e:
        return False, f"❌ {file_path} - error: {e!s}"


def main() -> int:
    """Main script execution."""
    success_count = 0
    skip_count = 0
    error_count = 0

    for file_str in FILES_NEEDING_MARKER:
        file_path = Path(file_str)
        if not file_path.exists():
            error_count += 1
            continue

        success, message = add_timing_marker(file_path)

        if success:
            if "already has" in message or "no timing operations" in message:
                skip_count += 1
            else:
                success_count += 1
        else:
            error_count += 1

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
