#!/usr/bin/env python3
"""Pre-commit hook to check timing marker on tests using timing operations.

This hook ensures that test files using sleep, timeout, or wait operations
have the @pytest.mark.timing marker applied.
"""

import re
import sys
from pathlib import Path


def has_timing_operations(content: str) -> bool:
    """Check if file contains timing operations."""
    timing_patterns = [
        r"asyncio\.sleep",
        r"time\.sleep",
        r"asyncio\.wait_for",
        r"asyncio\.timeout",
        r"wait_for_condition.*timeout",
        r"\.sleep\(",
    ]

    for pattern in timing_patterns:
        if re.search(pattern, content):
            return True
    return False


def has_timing_marker(content: str) -> bool:
    """Check if file has timing marker."""
    # Check for various forms of timing marker
    marker_patterns = [
        r"pytestmark\s*=.*pytest\.mark\.timing",
        r"@pytest\.mark\.timing",
        r"pytest\.mark\.timing",  # Simple pattern to catch it in any context
    ]

    for pattern in marker_patterns:
        if re.search(pattern, content, re.MULTILINE | re.DOTALL):
            return True
    return False


def check_file(file_path: Path) -> tuple[bool, list[str]]:
    """Check a file for proper timing marker usage."""
    issues: list[str] = []

    try:
        content = file_path.read_text()

        # If file has timing operations but no marker
        if has_timing_operations(content) and not has_timing_marker(content):
            issues.append(
                f"{file_path}: Test uses timing operations but missing @pytest.mark.timing marker",
            )

    except Exception as e:
        issues.append(f"{file_path}: Error reading file: {e!s}")

    return len(issues) == 0, issues


def main() -> int:
    """Main pre-commit hook execution."""
    # Get files from command line (pre-commit passes them)
    files = sys.argv[1:] if len(sys.argv) > 1 else []

    if not files:
        return 0

    all_issues: list[str] = []

    for file_str in files:
        file_path = Path(file_str)

        # Only check Python test files
        if not file_path.name.startswith("test_") or not file_path.suffix == ".py":
            continue

        success, issues = check_file(file_path)
        if not success:
            all_issues.extend(issues)

    if all_issues:
        for issue in all_issues:
            sys.stderr.write(issue + "\n")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
