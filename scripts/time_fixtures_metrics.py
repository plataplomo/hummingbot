#!/usr/bin/env python3
"""Generate metrics for time fixture adoption in the test suite.

This script analyzes the test codebase to track adoption of centralized
time fixtures and generates metrics suitable for CI dashboards.
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def count_time_operations(content: str) -> dict[str, int]:
    """Count various time-related operations in test content.

    Returns:
        Dictionary mapping operation names to their counts in the content.
    """
    patterns = {
        "datetime_now": r"datetime\.now\(",
        "datetime_utcnow": r"datetime\.utcnow\(",
        "time_time": r"time\.time\(\)",
        "time_sleep": r"time\.sleep\(",
        "asyncio_sleep": r"asyncio\.sleep\(",
        "patch_datetime": r"@patch.*datetime|with.*patch.*datetime",
        "patch_time": r"@patch.*time\.time|with.*patch.*time\.time",
        "frozen_time": r"frozen_time|FreezerProtocol",
        "mock_time_patch": r"mock_time_patch",
        "timing_marker": r"@pytest\.mark\.timing|pytestmark.*timing",
    }

    counts: dict[str, int] = {}
    for name, pattern in patterns.items():
        counts[name] = len(re.findall(pattern, content, re.IGNORECASE))

    return counts


def analyze_test_file(file_path: Path) -> dict[str, Any]:
    """Analyze a single test file for time-related patterns.

    Returns:
        Dictionary containing:
        - path: File path as string
        - counts: Dictionary of pattern counts
        - uses_time_operations: Whether file uses time operations
        - uses_old_mocking: Whether file uses old mocking patterns
        - uses_new_fixtures: Whether file uses new fixtures
        - has_timing_marker: Whether file has timing pytest marker
        - needs_timing_marker: Whether file needs timing marker
        - migration_status: Status of migration ("migrated", "needs_migration", or "no_mocking")
        - error: Error message if file couldn't be analyzed (only present on error)
    """
    try:
        content = file_path.read_text()
        counts = count_time_operations(content)

        # Determine if file uses time operations
        uses_time_operations = any([
            counts["datetime_now"] > 0,
            counts["datetime_utcnow"] > 0,
            counts["time_time"] > 0,
            counts["time_sleep"] > 0,
            counts["asyncio_sleep"] > 0,
        ])

        # Determine if file uses old mocking patterns
        uses_old_mocking = any([
            counts["patch_datetime"] > 0,
            counts["patch_time"] > 0,
        ])

        # Determine if file uses new fixtures
        uses_new_fixtures = any([
            counts["frozen_time"] > 0,
            counts["mock_time_patch"] > 0,
        ])

        # Check if properly marked
        has_timing_marker = counts["timing_marker"] > 0
        needs_timing_marker = (
            counts["time_sleep"] > 0 or counts["asyncio_sleep"] > 0
        ) and not has_timing_marker

        return {
            "path": str(file_path),
            "counts": counts,
            "uses_time_operations": uses_time_operations,
            "uses_old_mocking": uses_old_mocking,
            "uses_new_fixtures": uses_new_fixtures,
            "has_timing_marker": has_timing_marker,
            "needs_timing_marker": needs_timing_marker,
            "migration_status": (
                "migrated"
                if uses_new_fixtures
                else ("needs_migration" if uses_old_mocking else "no_mocking")
            ),
        }
    except (OSError, UnicodeDecodeError, SyntaxError) as e:
        return {"path": str(file_path), "error": str(e)}


def generate_metrics(test_dir: Path = Path("tests")) -> dict[str, Any]:
    """Generate comprehensive metrics for time fixture adoption.

    Returns:
        Dictionary containing:
        - timestamp: ISO format timestamp when metrics were generated
        - summary: Dictionary with aggregate statistics
        - time_operations_breakdown: Count totals for each operation type
        - migration_candidates: List of file paths needing migration
        - new_fixture_adopters: List of file paths using new fixtures
    """
    # Find all test files
    test_files = list(test_dir.rglob("test_*.py"))

    # Analyze each file
    results: list[dict[str, Any]] = []
    for test_file in test_files:
        result = analyze_test_file(test_file)
        if "error" not in result:
            results.append(result)

    # Calculate aggregate metrics
    total_files = len(results)
    files_with_time_ops = sum(1 for r in results if r["uses_time_operations"])
    files_with_old_mocking = sum(1 for r in results if r["uses_old_mocking"])
    files_with_new_fixtures = sum(1 for r in results if r["uses_new_fixtures"])
    files_needing_migration = sum(1 for r in results if r["migration_status"] == "needs_migration")
    files_migrated = sum(1 for r in results if r["migration_status"] == "migrated")
    files_needing_marker = sum(1 for r in results if r["needs_timing_marker"])
    files_with_marker = sum(1 for r in results if r["has_timing_marker"])

    # Calculate adoption percentage
    adoption_percentage = 0.0
    if files_with_old_mocking + files_migrated > 0:
        adoption_percentage = (files_migrated / (files_with_old_mocking + files_migrated)) * 100

    # Group by directory for detailed breakdown
    by_directory: dict[str, dict[str, int]] = defaultdict(
        lambda: {"total": 0, "migrated": 0, "needs_migration": 0},
    )
    for result in results:
        dir_path = Path(result["path"]).parent
        relative_dir = dir_path.relative_to(test_dir)

        by_directory[str(relative_dir)]["total"] += 1
        if result["migration_status"] == "migrated":
            by_directory[str(relative_dir)]["migrated"] += 1
        elif result["migration_status"] == "needs_migration":
            by_directory[str(relative_dir)]["needs_migration"] += 1

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "summary": {
            "total_test_files": total_files,
            "files_with_time_operations": files_with_time_ops,
            "files_with_old_mocking": files_with_old_mocking,
            "files_with_new_fixtures": files_with_new_fixtures,
            "files_needing_migration": files_needing_migration,
            "files_migrated": files_migrated,
            "adoption_percentage": round(adoption_percentage, 2),
            "files_with_timing_marker": files_with_marker,
            "files_needing_timing_marker": files_needing_marker,
        },
        "by_directory": dict(by_directory),
        "migration_candidates": [
            r["path"] for r in results if r["migration_status"] == "needs_migration"
        ][:10],  # Top 10 candidates
        "successful_migrations": [
            r["path"] for r in results if r["migration_status"] == "migrated"
        ],
    }


def format_console_output(metrics: dict[str, Any]) -> str:
    """Format metrics for console output.

    Returns:
        Formatted string ready for console display with metrics summary.
    """
    summary = metrics["summary"]

    output: list[str] = []
    output.extend((
        "# Time Fixtures Adoption Metrics",
        "=" * 50,
        f"Generated: {metrics['timestamp']}",
        "",
        "## Summary",
        f"Total test files: {summary['total_test_files']}",
        f"Files with time operations: {summary['files_with_time_operations']}",
        f"Files using old mocking: {summary['files_with_old_mocking']}",
        f"Files using new fixtures: {summary['files_with_new_fixtures']}",
        f"**Adoption rate: {summary['adoption_percentage']}%**",
        "",
        "## Timing Markers",
        f"Files with @pytest.mark.timing: {summary['files_with_timing_marker']}",
        f"Files needing timing marker: {summary['files_needing_timing_marker']}",
        "",
    ))

    if metrics["migration_candidates"]:
        output.append("## Next Migration Candidates")
        output.extend(f"  - {path}" for path in metrics["migration_candidates"])

    return "\n".join(output)


def format_github_output(metrics: dict[str, Any]) -> str:
    """Format metrics for GitHub Actions output.

    Returns:
        Formatted string with GitHub Actions annotations and metrics.
    """
    summary = metrics["summary"]

    # Create GitHub-friendly output with annotations
    lines: list[str] = []

    # Summary badge data
    lines.append(f"::notice::Time Fixtures Adoption: {summary['adoption_percentage']}%")

    if summary["files_needing_migration"] > 0:
        lines.append(
            f"::warning::{summary['files_needing_migration']} files still need "
            f"migration to new time fixtures",
        )

    if summary["files_needing_timing_marker"] > 0:
        lines.append(
            f"::warning::{summary['files_needing_timing_marker']} files need "
            f"@pytest.mark.timing marker",
        )

    # Set output variables for badge generation
    lines.extend((
        f"::set-output name=adoption_percentage::{summary['adoption_percentage']}",
        f"::set-output name=files_migrated::{summary['files_migrated']}",
        f"::set-output name=files_needing_migration::{summary['files_needing_migration']}",
    ))

    return "\n".join(lines)


def main() -> None:
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Generate metrics for time fixture adoption")
    parser.add_argument(
        "--format",
        choices=["console", "json", "github"],
        default="console",
        help="Output format",
    )
    parser.add_argument("--output", type=Path, help="Output file (default: stdout)")
    parser.add_argument(
        "--test-dir",
        type=Path,
        default=Path("tests"),
        help="Test directory to analyze",
    )

    args = parser.parse_args()

    # Generate metrics
    metrics = generate_metrics(args.test_dir)

    # Format output
    if args.format == "json":
        output = json.dumps(metrics, indent=2)
    elif args.format == "github":
        output = format_github_output(metrics)
    else:
        output = format_console_output(metrics)

    # Write output
    if args.output:
        args.output.write_text(output)

        sys.stdout.write(f"Metrics written to {args.output}\n")
    else:
        sys.stdout.write(output + "\n")


if __name__ == "__main__":
    main()
