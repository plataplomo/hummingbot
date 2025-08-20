#!/usr/bin/env python3
"""Verify no hard-coded fee configurations exist in the codebase.

This script checks that critical financial calculation files properly use
configuration instead of hard-coded values for include_fees parameters.
Suitable for CI/CD pipelines and GitHub Actions.
"""

import argparse
import ast
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def check_hardcoded_fees(filepath: Path) -> list[dict[str, Any]]:
    """Check for hard-coded include_fees values in the file.

    Args:
        filepath: Path to the Python file to check

    Returns:
        List of dictionaries containing line number and issue details
    """
    try:
        content = filepath.read_text(encoding="utf-8")
        tree = ast.parse(content)
    except (SyntaxError, OSError) as e:
        return [{"error": str(e)}]

    return [
        {
            "line": node.lineno,
            "value": node.value.value,
            "issue": f"Hard-coded: include_fees={node.value.value}",
        }
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.keyword)
            and node.arg == "include_fees"
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, bool)
        )
    ]


def check_config_usage(filepath: Path) -> dict[str, bool]:
    """Check if fee configuration is properly cached and used.

    Args:
        filepath: Path to the Python file to check

    Returns:
        Dictionary with caching status
    """
    try:
        content = filepath.read_text(encoding="utf-8")
    except OSError:
        return {"has_cache": False, "uses_cache": False}

    # Check for various caching patterns
    cache_patterns = [
        "_include_fees_in_pnl = config.financial.pnl.include_fees_in_pnl",
        "_include_fees_default = self._financial_config.pnl.include_fees_in_pnl",
        "_include_fees = config.financial.pnl.include_fees_in_pnl",
    ]

    usage_patterns = [
        "include_fees=self._include_fees_in_pnl",
        "include_fees=self._include_fees_default",
        "include_fees=self._include_fees",
    ]

    has_cache = any(pattern in content for pattern in cache_patterns)
    uses_cache = any(pattern in content for pattern in usage_patterns)

    return {"has_cache": has_cache, "uses_cache": uses_cache}


def analyze_file(filepath: Path) -> dict[str, Any]:
    """Analyze a single file for hard-coded fee configurations.

    Returns:
        Dictionary containing analysis results
    """
    if not filepath.exists():
        return {"path": str(filepath), "exists": False, "error": "File not found"}

    hardcoded_issues = check_hardcoded_fees(filepath)
    config_usage = check_config_usage(filepath)

    # Check if it's an error result
    if hardcoded_issues and "error" in hardcoded_issues[0]:
        return {"path": str(filepath), "exists": True, "error": hardcoded_issues[0]["error"]}

    return {
        "path": str(filepath),
        "exists": True,
        "hardcoded_count": len(hardcoded_issues),
        "hardcoded_issues": hardcoded_issues,
        "has_config_cache": config_usage["has_cache"],
        "uses_config_cache": config_usage["uses_cache"],
        "status": "fail" if hardcoded_issues else "pass",
    }


def generate_metrics(check_paths: list[Path] | None = None) -> dict[str, Any]:
    """Generate comprehensive metrics for hard-coded fee detection.

    Args:
        check_paths: Optional list of paths to check. If None, scans all Python files
            in cyberdelta/ directory.

    Returns:
        Dictionary containing metrics and analysis results
    """
    if check_paths is None:
        # Scan all Python files in cyberdelta/ directory
        cyberdelta_path = Path("cyberdelta")
        if not cyberdelta_path.exists():
            return {
                "timestamp": datetime.now(UTC).isoformat(),
                "summary": {
                    "total_files_checked": 0,
                    "files_found": 0,
                    "error": "cyberdelta directory not found"
                },
                "files": [],
                "violations": []
            }

        # Find all Python files recursively
        check_paths = list(cyberdelta_path.rglob("*.py"))

        # Exclude __pycache__ and test files if needed
        check_paths = [
            p for p in check_paths
            if "__pycache__" not in str(p) and not str(p).startswith("test_")
        ]

    results = [analyze_file(filepath) for filepath in check_paths]

    # Calculate summary metrics
    total_files = len(results)
    files_found = sum(1 for r in results if r.get("exists", False))
    files_with_errors = sum(1 for r in results if "error" in r)
    files_with_hardcoded = sum(1 for r in results if r.get("hardcoded_count", 0) > 0)
    total_hardcoded = sum(r.get("hardcoded_count", 0) for r in results)
    files_with_config = sum(1 for r in results if r.get("has_config_cache", False))
    files_using_config = sum(1 for r in results if r.get("uses_config_cache", False))

    # Calculate compliance percentage
    compliance_percentage = 0.0
    if files_found > 0:
        compliant_files = files_found - files_with_hardcoded
        compliance_percentage = (compliant_files / files_found) * 100

    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "summary": {
            "total_files_checked": total_files,
            "files_found": files_found,
            "files_with_errors": files_with_errors,
            "files_with_hardcoded": files_with_hardcoded,
            "total_hardcoded_instances": total_hardcoded,
            "files_with_config_cache": files_with_config,
            "files_using_config_cache": files_using_config,
            "compliance_percentage": round(compliance_percentage, 2),
            "all_compliant": files_with_hardcoded == 0,
        },
        "files": results,
        "violations": [
            {"file": r["path"], "issues": r.get("hardcoded_issues", [])}
            for r in results
            if r.get("hardcoded_count", 0) > 0
        ],
    }


def format_console_output(metrics: dict[str, Any]) -> str:
    """Format metrics for console output.

    Returns:
        Formatted string for console display
    """
    summary = metrics["summary"]

    output: list[str] = []
    output.extend([
        "=" * 60,
        "Hard-coded Fee Configuration Check",
        "=" * 60,
        f"Generated: {metrics['timestamp']}",
        "",
        "## Summary",
        f"Files checked: {summary['total_files_checked']}",
        f"Files found: {summary['files_found']}",
        f"Files with hard-coded values: {summary['files_with_hardcoded']}",
        f"Total hard-coded instances: {summary['total_hardcoded_instances']}",
        f"Files with config caching: {summary['files_with_config_cache']}",
        f"**Compliance: {summary['compliance_percentage']}%**",
        "",
    ])

    if metrics["violations"]:
        output.append("## Violations Found")
        for violation in metrics["violations"]:
            output.append(f"\n{violation['file']}:")
            output.extend(
                f"  Line {issue['line']}: {issue['issue']}" for issue in violation["issues"]
            )
        output.append("")

    # Final status
    output.append("=" * 60)
    if summary["all_compliant"]:
        output.extend([
            "✅ SUCCESS: No hard-coded fee configurations found!",
            "All files properly use configuration for fee inclusion.",
        ])
    else:
        output.extend([
            f"❌ FAILED: Found {summary['total_hardcoded_instances']} "
            "hard-coded fee configuration(s)",
            "These must be fixed to ensure proper configuration usage.",
        ])
    output.append("=" * 60)

    return "\n".join(output)


def format_github_output(metrics: dict[str, Any]) -> str:
    """Format metrics for GitHub Actions output.

    Returns:
        Formatted string for GitHub Actions
    """
    summary = metrics["summary"]

    lines: list[str] = []

    # Create GitHub annotations
    if summary["all_compliant"]:
        lines.append(
            f"::notice::✅ Fee Configuration Check Passed: "
            f"{summary['compliance_percentage']}% compliance"
        )
    else:
        lines.append(
            f"::error::❌ Fee Configuration Check Failed: "
            f"{summary['files_with_hardcoded']} files with hard-coded values"
        )

    # Add warnings for each violation
    lines.extend(
        f"::error file={violation['file']},line={issue['line']}::"
        f"Hard-coded fee configuration: include_fees={issue['value']}"
        for violation in metrics["violations"]
        for issue in violation["issues"]
    )

    # Set output variables for badge generation
    compliant_files = summary["files_found"] - summary["files_with_hardcoded"]
    lines.extend([
        f"::set-output name=compliance_percentage::{summary['compliance_percentage']}",
        f"::set-output name=files_compliant::{compliant_files}",
        f"::set-output name=files_with_hardcoded::{summary['files_with_hardcoded']}",
        f"::set-output name=total_violations::{summary['total_hardcoded_instances']}",
    ])

    return "\n".join(lines)


def main() -> int:
    """Main script execution.

    Returns:
        Exit code (0 for success, 1 for violations found)
    """
    parser = argparse.ArgumentParser(
        description="Verify no hard-coded fee configurations exist in the codebase"
    )
    parser.add_argument(
        "--format",
        choices=["console", "json", "github"],
        default="console",
        help="Output format (default: console)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        type=Path,
        help="Specific files to check (default: critical financial files)",
    )
    parser.add_argument(
        "--exit-on-failure",
        action="store_true",
        help="Exit with non-zero code if violations found",
    )

    args = parser.parse_args()

    # Generate metrics
    metrics = generate_metrics(args.files)

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
        sys.stdout.write(f"Results written to {args.output}\n")
    else:
        sys.stdout.write(output + "\n")

    # Exit code
    if args.exit_on_failure and not metrics["summary"]["all_compliant"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
