#!/usr/bin/env python3
"""Check for JSON anti-patterns in the codebase.

This script scans Python files for dangerous JSON usage patterns that could
lead to performance issues or financial data corruption.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path
from typing import NamedTuple, TextIO

from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)

# Maximum violations to display per rule to avoid excessive output
MAX_VIOLATIONS_DISPLAY = 10


class JSONViolation(NamedTuple):
    """Represents a JSON anti-pattern violation."""

    file_path: Path
    line_number: int
    column: int
    rule_id: str
    message: str
    line_content: str


class JSONAntiPatternChecker(ast.NodeVisitor):
    """AST visitor to check for JSON anti-patterns."""

    def __init__(self, file_path: Path, file_content: str) -> None:
        """Initialize the checker with file path and content.

        Args:
            file_path: Path to the file being checked
            file_content: Content of the file as string
        """
        self.file_path = file_path
        self.lines = file_content.splitlines()
        self.violations: list[JSONViolation] = []

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        """Check for direct json imports (except in security layer)."""
        for alias in node.names:
            if alias.name == "json" and "json_security.py" not in str(self.file_path):
                self._add_violation(
                    node,
                    "JSON001",
                    "Direct 'json' import not allowed. Use cyberdelta.utils.serialization instead",
                )
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        """Check for json module imports."""
        if node.module == "json" and "json_security.py" not in str(self.file_path):
            self._add_violation(
                node,
                "JSON001",
                "Direct 'json' module import not allowed. Use cyberdelta.utils.serialization",
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        """Check for problematic function calls."""
        self._check_json_dumps_default_str(node)
        self._check_float_conversion(node)
        self._check_model_dump_mode(node)
        self.generic_visit(node)

    def _check_json_dumps_default_str(self, node: ast.Call) -> None:
        """Check for json.dumps with default=str."""
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "json"
            and node.func.attr == "dumps"
        ):
            for keyword in node.keywords:
                if (
                    keyword.arg == "default"
                    and isinstance(keyword.value, ast.Name)
                    and keyword.value.id == "str"
                ):
                    self._add_violation(
                        node,
                        "JSON002",
                        "json.dumps with default=str converts all non-JSON types to strings, "
                        "potentially corrupting financial data",
                    )

    def _check_float_conversion(self, node: ast.Call) -> None:
        """Check for float() calls with financial data."""
        if isinstance(node.func, ast.Name) and node.func.id == "float":
            line_content = self._get_line_content(node.lineno)
            financial_keywords = [
                "price",
                "quantity",
                "amount",
                "balance",
                "value",
                "pnl",
                "profit",
                "loss",
            ]
            if any(keyword in line_content.lower() for keyword in financial_keywords):
                self._add_violation(
                    node,
                    "JSON003",
                    "float() conversion detected with financial data. "
                    "Use Decimal to preserve precision",
                )

    def _check_model_dump_mode(self, node: ast.Call) -> None:
        """Check for model_dump without mode='json'."""
        if isinstance(node.func, ast.Attribute) and node.func.attr == "model_dump":
            has_mode_json = any(
                keyword.arg == "mode"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == "json"
                for keyword in node.keywords
            )

            if not has_mode_json:
                line_content = self._get_line_content(node.lineno)
                # Only flag if it's likely being used for JSON serialization
                if any(keyword in line_content for keyword in ["json", "dumps", "serialize"]):
                    self._add_violation(
                        node,
                        "JSON004",
                        "model_dump() should use mode='json' for JSON serialization "
                        "to ensure type safety",
                    )

    def visit_With(self, node: ast.With) -> None:  # noqa: N802
        """Check for synchronous file operations that might block."""
        for item in node.items:
            if (
                isinstance(item.context_expr, ast.Call)
                and isinstance(item.context_expr.func, ast.Name)
                and item.context_expr.func.id == "open"
                and self._in_async_function(node)
            ):
                # Check if it's dealing with JSON/state files
                line_content = self._get_line_content(node.lineno)
                json_keywords = ["json", "state", ".json", "portfolio", "config"]
                if any(keyword in line_content.lower() for keyword in json_keywords):
                    self._add_violation(
                        node,
                        "JSON005",
                        "Synchronous file I/O in async context may block trading operations. "
                        "Use aiofiles.open() instead",
                    )

        self.generic_visit(node)

    def _add_violation(self, node: ast.stmt | ast.expr, rule_id: str, message: str) -> None:
        """Add a violation to the list.

        Args:
            node: AST node where violation occurred
            rule_id: Unique identifier for the rule
            message: Description of the violation
        """
        line_content = self._get_line_content(node.lineno)
        violation = JSONViolation(
            file_path=self.file_path,
            line_number=node.lineno,
            column=node.col_offset,
            rule_id=rule_id,
            message=message,
            line_content=line_content.strip(),
        )
        self.violations.append(violation)

    def _get_line_content(self, line_number: int) -> str:
        """Get the content of a specific line.

        Args:
            line_number: Line number (1-based)

        Returns:
            Line content or empty string if line doesn't exist
        """
        if 1 <= line_number <= len(self.lines):
            return self.lines[line_number - 1]
        return ""

    def _in_async_function(self, node: ast.AST) -> bool:
        """Check if a node is inside an async function.

        Args:
            node: AST node to check

        Returns:
            True if node is inside an async function
        """
        # Walk up the AST to find if we're in an async function
        current: ast.AST = node
        while hasattr(current, "parent"):
            parent = getattr(current, "parent", None)
            if parent is None:
                break
            if isinstance(parent, ast.AsyncFunctionDef):
                return True
            current = parent
        return False


def add_parent_references(node: ast.AST) -> None:
    """Add parent references to AST nodes for traversal.

    Args:
        node: Root AST node
    """
    for child in ast.walk(node):
        for child_node in ast.iter_child_nodes(child):
            # Using setattr is necessary here for dynamic AST node modification
            child_node.parent = child  # type: ignore[attr-defined]


def check_regex_patterns(file_path: Path, content: str) -> list[JSONViolation]:
    """Check for regex-based anti-patterns.

    Args:
        file_path: Path to the file being checked
        content: File content

    Returns:
        List of violations found
    """
    violations: list[JSONViolation] = []
    lines = content.splitlines()

    patterns = [
        (
            r"json\.dumps\([^)]*default\s*=\s*str",
            "JSON002",
            "json.dumps with default=str converts all types to strings",
        ),
        (
            r'\.decode\(\s*["\']utf-8["\']\s*\)\s*\)\s*$',
            "JSON006",
            "Consider using dumps_json_bytes() for binary output instead of decode()",
        ),
        (r"import\s+json\s*(?:#|$)", "JSON001", "Direct json import detected"),
    ]

    for line_num, line in enumerate(lines, 1):
        for pattern, rule_id, message in patterns:
            if re.search(pattern, line):
                # Skip if in security layer
                if "json_security.py" in str(file_path) and "JSON001" in rule_id:
                    continue

                violations.append(
                    JSONViolation(
                        file_path=file_path,
                        line_number=line_num,
                        column=line.find("json") if "json" in line else 0,
                        rule_id=rule_id,
                        message=message,
                        line_content=line.strip(),
                    )
                )

    return violations


def check_file(file_path: Path) -> list[JSONViolation]:
    """Check a single Python file for JSON anti-patterns.

    Args:
        file_path: Path to the Python file

    Returns:
        List of violations found
    """
    try:
        logger.debug("Checking file for JSON anti-patterns", file_path=str(file_path))
        content = file_path.read_text(encoding="utf-8")

        # Regex-based checks
        violations = check_regex_patterns(file_path, content)

        # AST-based checks
        try:
            tree = ast.parse(content)
            add_parent_references(tree)

            checker = JSONAntiPatternChecker(file_path, content)
            checker.visit(tree)
            violations.extend(checker.violations)

        except SyntaxError as e:
            logger.warning(
                "Skipping file with syntax errors", file_path=str(file_path), error=str(e)
            )

        if violations:
            logger.debug(
                "Found JSON anti-pattern violations",
                file_path=str(file_path),
                violation_count=len(violations),
            )

    except UnicodeDecodeError as e:
        logger.debug("Skipping binary file", file_path=str(file_path), error=str(e))
        return []
    except OSError as e:
        logger.warning("Could not process file", file_path=str(file_path), error=str(e))
        return []
    else:
        return violations


def print_violations(
    violations_by_rule: dict[str, list[JSONViolation]], output: TextIO = sys.stdout
) -> None:
    """Print violations in a formatted way.

    Args:
        violations_by_rule: Violations grouped by rule ID
        output: Output stream to write to
    """
    total_violations = sum(len(v) for v in violations_by_rule.values())

    logger.info(
        "JSON anti-pattern violations found",
        total_violations=total_violations,
        rules_triggered=len(violations_by_rule),
    )

    output.write(f"🚨 Found {total_violations} JSON anti-pattern violations:\n\n")

    for rule_id in sorted(violations_by_rule.keys()):
        violations = violations_by_rule[rule_id]
        output.write(f"[{rule_id}] {violations[0].message}\n")
        output.write(f"  Found in {len(violations)} location(s):\n")

        logger.debug(
            "Rule violation details",
            rule_id=rule_id,
            violation_count=len(violations),
            message=violations[0].message,
        )

        for violation in violations[:MAX_VIOLATIONS_DISPLAY]:
            output.write(f"    {violation.file_path}:{violation.line_number}:{violation.column}\n")
            output.write(f"      > {violation.line_content}\n")

        if len(violations) > MAX_VIOLATIONS_DISPLAY:
            remaining = len(violations) - MAX_VIOLATIONS_DISPLAY
            output.write(f"    ... and {remaining} more\n")

        output.write("\n")

    # Summary by rule type
    output.write("Summary by rule:\n")
    for rule_id in sorted(violations_by_rule.keys()):
        count = len(violations_by_rule[rule_id])
        output.write(f"  {rule_id}: {count} violation(s)\n")
        logger.info("Rule summary", rule_id=rule_id, violation_count=count)


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, 1 for violations found)
    """
    parser = argparse.ArgumentParser(description="Check for JSON anti-patterns")
    parser.add_argument(
        "paths", nargs="*", default=["cyberdelta/"], help="Paths to check (default: cyberdelta/)"
    )
    parser.add_argument("--exclude", action="append", default=[], help="Patterns to exclude")
    parser.add_argument(
        "--fail-on-violations", action="store_true", help="Exit with code 1 if violations found"
    )

    args = parser.parse_args()

    logger.info(
        "Starting JSON anti-pattern check",
        paths=args.paths,
        exclude_patterns=args.exclude,
        fail_on_violations=args.fail_on_violations,
    )

    all_violations: list[JSONViolation] = []
    files_processed = 0

    for path_str in args.paths:
        path = Path(path_str)

        if path.is_file() and path.suffix == ".py":
            all_violations.extend(check_file(path))
            files_processed += 1
        elif path.is_dir():
            for py_file in path.rglob("*.py"):
                # Skip excluded patterns
                if any(exclude in str(py_file) for exclude in args.exclude):
                    logger.debug("Skipping excluded file", file_path=str(py_file))
                    continue

                all_violations.extend(check_file(py_file))
                files_processed += 1
        else:
            logger.warning("Path not found or not a Python file", path=str(path))

    logger.info(
        "File processing complete",
        files_processed=files_processed,
        total_violations=len(all_violations),
    )

    # Group violations by rule
    violations_by_rule: dict[str, list[JSONViolation]] = {}
    for violation in all_violations:
        violations_by_rule.setdefault(violation.rule_id, []).append(violation)

    if not all_violations:
        logger.info("No JSON anti-patterns found - scan successful")
        sys.stdout.write("✅ No JSON anti-patterns found!\n")
        return 0

    print_violations(violations_by_rule)

    exit_code = 1 if args.fail_on_violations else 0
    logger.info(
        "JSON anti-pattern check complete",
        exit_code=exit_code,
        fail_on_violations=args.fail_on_violations,
    )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
