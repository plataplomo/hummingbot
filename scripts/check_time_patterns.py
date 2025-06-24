#!/usr/bin/env python3
"""Pre-commit hook to check for unittest.mock datetime patches.

This hook prevents new test files from using unittest.mock to patch datetime,
encouraging the use of centralized time fixtures instead.
"""

import re
import sys
from pathlib import Path


# Allowed files that already have patches (grandfathered in)
ALLOWED_FILES = {
    # Add existing files here as they are identified
    # These should be migrated over time
    "tests/unit/apis/backpack/mappers/test_bp_market_data_mapper_websocket.py",
    # Add more as needed during transition
}


def _check_datetime_patches(file_path: Path, content: str, lines: list[str]) -> list[str]:
    """Check for datetime patches in file content."""
    issues: list[str] = []
    datetime_patch_patterns = [
        r'@patch\s*\(\s*["\'].*datetime["\']',
        r'with\s+patch\s*\(\s*["\'].*datetime["\']',
        r'patch\s*\(\s*["\'].*datetime\.datetime["\']',
        r'patch\s*\(\s*["\'].*datetime\.now["\']',
    ]

    for i, line in enumerate(lines, 1):
        for pattern in datetime_patch_patterns:
            if re.search(pattern, line):
                # Check if it's using frozen_time fixture
                if "frozen_time" not in content:
                    issues.append(
                        f"{file_path}:{i}: Use 'frozen_time' fixture instead of "
                        f"patching datetime. See tests/fixtures/time_fixtures.py"
                    )
                    break
    return issues


def _check_time_patches(file_path: Path, content: str, lines: list[str]) -> list[str]:
    """Check for time.time patches in file content."""
    issues: list[str] = []
    time_patch_patterns = [
        r'@patch\s*\(\s*["\']time\.time["\']',
        r'with\s+patch\s*\(\s*["\']time\.time["\']',
    ]

    for i, line in enumerate(lines, 1):
        for pattern in time_patch_patterns:
            if re.search(pattern, line):
                # Check if it's using mock_time_patch fixture
                if "mock_time_patch" not in content:
                    issues.append(
                        f"{file_path}:{i}: Use 'mock_time_patch' fixture instead of "
                        f"patching time.time. See tests/fixtures/time_fixtures.py"
                    )
                    break
    return issues


def check_file(file_path: Path) -> tuple[bool, list[str]]:
    """Check a file for unittest.mock datetime patches."""
    # Skip allowed files
    if str(file_path) in ALLOWED_FILES:
        return True, []

    issues: list[str] = []

    try:
        content = file_path.read_text()
        lines = content.split("\n")

        issues.extend(_check_datetime_patches(file_path, content, lines))
        issues.extend(_check_time_patches(file_path, content, lines))

    except Exception as e:
        issues.append(f"{file_path}: Error reading file: {str(e)}")

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
