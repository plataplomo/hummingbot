#!/usr/bin/env python3
"""Analyze time mocking patterns in test files to identify migration candidates.

This script scans test files to find various time mocking patterns and
generates a report of files that need migration to the new time fixtures.
"""

import argparse
import ast
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


class TimeMockingAnalyzer(ast.NodeVisitor):
    """AST visitor to analyze time mocking patterns in test files."""

    def __init__(self) -> None:
        """Initialize the analyzer."""
        self.patterns_found: dict[str, set[str]] = defaultdict(set)
        self.imports: set[str] = set()
        self.has_frozen_time = False
        self.has_mock_time_patch = False

    def visit_Import(self, node: ast.Import) -> None:
        """Track imports."""
        for alias in node.names:
            self.imports.add(alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track from imports."""
        if node.module:
            if node.module == "unittest.mock":
                for alias in node.names:
                    if alias.name in {"patch", "MagicMock", "Mock"}:
                        self.imports.add(f"{node.module}.{alias.name}")
            elif node.module == "tests.fixtures.time_fixtures":
                for alias in node.names:
                    if alias.name == "FreezerProtocol":
                        self.has_frozen_time = True
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Check function definitions for fixtures and decorators."""
        # Check for fixture parameters
        for arg in node.args.args:
            if arg.arg == "frozen_time":
                self.has_frozen_time = True
            elif arg.arg == "mock_time_patch":
                self.has_mock_time_patch = True

        # Check decorators
        for decorator in node.decorator_list:
            decorator_str = ast.unparse(decorator)
            if "@patch" in decorator_str and "datetime" in decorator_str:
                self.patterns_found["@patch_datetime"].add(node.name)
            elif "@patch" in decorator_str and "time.time" in decorator_str:
                self.patterns_found["@patch_time_time"].add(node.name)

        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        """Check with statements for patch context managers."""
        for item in node.items:
            if hasattr(item.context_expr, "func"):
                context_str = ast.unparse(item.context_expr)
                if "patch" in context_str and "datetime" in context_str:
                    self.patterns_found["with_patch_datetime"].add("context_manager")
                elif "patch" in context_str and "time.time" in context_str:
                    self.patterns_found["with_patch_time_time"].add("context_manager")
        self.generic_visit(node)


def analyze_file(file_path: Path) -> dict[str, Any]:
    """Analyze a single file for time mocking patterns.

    Returns:
        Dictionary containing analysis results with keys:
        - patterns: Dict of pattern types to sets of locations where found
        - imports: Set of imported modules
        - has_frozen_time: Whether file uses frozen_time fixture
        - has_mock_time_patch: Whether file uses mock_time_patch fixture
        - has_datetime_now: Whether file contains datetime.now() calls
        - has_time_time: Whether file contains time.time() calls
        - has_sleep: Whether file contains sleep calls
        - has_timing_marker: Whether file has timing pytest markers
        - needs_migration: Whether file needs migration to new time fixtures
        - error: Error message if parsing failed (only present on error)
    """
    try:
        content = file_path.read_text()
        tree = ast.parse(content, filename=str(file_path))

        analyzer = TimeMockingAnalyzer()
        analyzer.visit(tree)

        # Additional regex-based checks for complex patterns
        has_datetime_now = bool(re.search(r"datetime\.now\(", content))
        has_time_time = bool(re.search(r"time\.time\(\)", content))
        has_sleep = bool(re.search(r"(time\.sleep|asyncio\.sleep)", content))
        has_timing_marker = bool(re.search(r"@pytest\.mark\.timing|pytestmark.*timing", content))

        return {
            "patterns": dict(analyzer.patterns_found),
            "imports": analyzer.imports,
            "has_frozen_time": analyzer.has_frozen_time,
            "has_mock_time_patch": analyzer.has_mock_time_patch,
            "has_datetime_now": has_datetime_now,
            "has_time_time": has_time_time,
            "has_sleep": has_sleep,
            "has_timing_marker": has_timing_marker,
            "needs_migration": bool(analyzer.patterns_found) and not analyzer.has_frozen_time,
        }
    except (OSError, UnicodeDecodeError, SyntaxError) as e:
        return {"error": str(e)}


def generate_report(results: dict[Path, dict[str, Any]]) -> None:
    """Generate a comprehensive report of time mocking patterns."""
    # Summary statistics
    sys.stdout.write(f"Total files: {len(results)}\n")
    patches_count = sum(1 for r in results.values() if r.get("patterns"))
    frozen_time_count = sum(1 for r in results.values() if r.get("has_frozen_time"))
    migration_count = sum(1 for r in results.values() if r.get("needs_migration"))
    timing_marker_count = sum(1 for r in results.values() if r.get("has_timing_marker"))
    sleep_count = sum(1 for r in results.values() if r.get("has_sleep"))

    sys.stdout.write(f"Files with patches: {patches_count}\n")
    sys.stdout.write(f"Files using frozen_time: {frozen_time_count}\n")
    sys.stdout.write(f"Files needing migration: {migration_count}\n")
    sys.stdout.write(f"Files with timing marker: {timing_marker_count}\n")
    sys.stdout.write(f"Files with sleep: {sleep_count}\n")


def main() -> None:
    """Main script execution."""
    parser = argparse.ArgumentParser(description="Analyze time mocking patterns in test files")
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=Path("tests"),
        help="Directory to analyze",
    )
    parser.add_argument("--output", type=Path, help="Output file for the report (default: stdout)")

    args = parser.parse_args()

    # Find all test files
    test_files = list(args.target_dir.rglob("test_*.py"))

    sys.stdout.write(f"Analyzing {len(test_files)} test files...\n")

    # Analyze each file
    results: dict[Path, dict[str, Any]] = {}
    for test_file in test_files:
        results[test_file] = analyze_file(test_file)

    # Generate report
    if args.output:
        original_stdout = sys.stdout
        with Path(args.output).open("w", encoding="utf-8") as f:
            sys.stdout = f
            generate_report(results)
        sys.stdout = original_stdout
        sys.stdout.write(f"Report written to {args.output}\n")
    else:
        generate_report(results)


if __name__ == "__main__":
    main()
