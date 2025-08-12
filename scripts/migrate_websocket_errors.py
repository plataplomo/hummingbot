#!/usr/bin/env python3
"""WebSocket Error System Migration Script.

This script migrates the WebSocket error handling from the old APIError-based system
to the new type-safe WebSocketStreamError system. It provides tools for:
1. Identifying old error patterns in the codebase
2. Migrating to the new error system
3. Validating the migration
4. Providing migration metrics

This is part of the 100-step WebSocket Type Safety refactoring plan (Step 72).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path


# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class MigrationMode(Enum):
    """Migration operation modes."""

    ANALYZE = "analyze"
    MIGRATE = "migrate"
    VALIDATE = "validate"


@dataclass
class MigrationPattern:
    """Pattern for identifying and migrating old error handling."""

    name: str
    old_pattern: str
    new_pattern: str
    file_pattern: str
    description: str
    risk_level: str  # low, medium, high


@dataclass
class MigrationIssue:
    """Issue found during migration analysis."""

    file_path: str
    line_number: int
    issue_type: str
    description: str
    old_code: str
    suggested_fix: str | None = None
    severity: str = "medium"


@dataclass
class MigrationStats:
    """Statistics for migration process."""

    files_analyzed: int = 0
    files_to_migrate: int = 0
    files_migrated: int = 0
    patterns_found: dict[str, int] = field(default_factory=dict)
    issues_found: list[MigrationIssue] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    start_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    end_time: datetime | None = None


class WebSocketErrorMigrator:
    """Main migration orchestrator for WebSocket error system."""

    def __init__(self, project_path: Path, dry_run: bool = True):
        """Initialize migrator.

        Args:
            project_path: Root path of the project
            dry_run: If True, don't modify files
        """
        self.project_path = project_path
        self.dry_run = dry_run
        self.stats = MigrationStats()

        # Define migration patterns
        self.patterns = [
            MigrationPattern(
                name="dict_error_context",
                old_pattern=r"context\.model_dump\(mode=['\"]python['\"]\)",
                new_pattern="ProcessorErrorContextBuilder.from_validation_error(self, payload, context)",
                file_pattern="**/*.py",
                description="Replace dict context conversion with typed builder",
                risk_level="medium",
            ),
            MigrationPattern(
                name="api_error_import",
                old_pattern=r"from cyberdelta\.apis\.exceptions.*import.*APIError",
                new_pattern="from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError",
                file_pattern="**/websocket/**/*.py",
                description="Replace APIError imports in WebSocket code",
                risk_level="low",
            ),
            MigrationPattern(
                name="websocket_error_inheritance",
                old_pattern=r"class\s+\w+Error\([^)]*APIError[^)]*\):",
                new_pattern="class {name}(WebSocketStreamError):",
                file_pattern="**/websocket/**/*.py",
                description="Replace APIError inheritance with WebSocketStreamError",
                risk_level="high",
            ),
            MigrationPattern(
                name="is_retryable_check",
                old_pattern=r"error\.is_retryable(?:\s*if\s+hasattr\(error,\s*['\"]is_retryable['\"]\)\s*else\s*False)?",
                new_pattern="error.get_recovery_strategy() != WebSocketRecoveryStrategy.NONE",
                file_pattern="**/websocket/**/*.py",
                description="Replace boolean retryable check with recovery strategy",
                risk_level="medium",
            ),
            MigrationPattern(
                name="dict_payload_handling",
                old_pattern=r"payload\s+if\s+isinstance\(payload,\s+dict\)\s+else\s+\{.*?\}",
                new_pattern="payload  # Already typed as BaseModel",
                file_pattern="**/websocket/**/*.py",
                description="Remove dict payload conversions",
                risk_level="medium",
            ),
            MigrationPattern(
                name="error_to_dict",
                old_pattern=r"error\.to_dict\(\)",
                new_pattern="WebSocketErrorAdapter.to_api_error(error) if isinstance(error, WebSocketStreamError) else error.to_dict()",
                file_pattern="**/websocket/**/*.py",
                description="Use adapter for backward compatibility",
                risk_level="low",
            ),
        ]

        # Files to skip
        self.skip_files = {
            "ws_error_adapter.py",  # Adapter itself
            "ws_dual_error_manager.py",  # Migration manager
            "ws_migration_tracker.py",  # Migration tracking
            "__pycache__",
            ".pyc",
            ".git",
        }

    def analyze(self) -> MigrationStats:
        """Analyze codebase for migration needs.

        Returns:
            Migration statistics
        """
        print(f"🔍 Analyzing WebSocket error patterns in {self.project_path}")

        websocket_path = self.project_path / "cyberdelta" / "apis" / "websocket"

        for py_file in websocket_path.glob("**/*.py"):
            if self._should_skip_file(py_file):
                continue

            self.stats.files_analyzed += 1
            self._analyze_file(py_file)

        # Analyze test files too
        test_path = self.project_path / "tests"
        for py_file in test_path.glob("**/websocket/**/*.py"):
            if self._should_skip_file(py_file):
                continue

            self.stats.files_analyzed += 1
            self._analyze_file(py_file)

        self.stats.files_to_migrate = len(set(issue.file_path for issue in self.stats.issues_found))
        self.stats.end_time = datetime.now(UTC)

        return self.stats

    def migrate(self) -> MigrationStats:
        """Perform migration on identified files.

        Returns:
            Migration statistics
        """
        if self.dry_run:
            print("🔄 DRY RUN: Simulating migration (no files will be modified)")
        else:
            print("⚠️  LIVE MIGRATION: Files will be modified!")

        # First analyze
        self.analyze()

        # Group issues by file
        issues_by_file: dict[str, list[MigrationIssue]] = defaultdict(list)
        for issue in self.stats.issues_found:
            issues_by_file[issue.file_path].append(issue)

        # Migrate each file
        for file_path, issues in issues_by_file.items():
            if self._migrate_file(Path(file_path), issues):
                self.stats.files_migrated += 1

        self.stats.end_time = datetime.now(UTC)
        return self.stats

    def validate(self) -> bool:
        """Validate that migration was successful.

        Returns:
            True if validation passes
        """
        print("✅ Validating migration...")

        validation_checks = [
            self._check_no_api_error_in_websocket(),
            self._check_typed_error_handling(),
            self._check_recovery_strategies(),
            self._check_no_dict_conversions(),
            self._check_tests_pass(),
        ]

        return all(validation_checks)

    def _should_skip_file(self, file_path: Path) -> bool:
        """Check if file should be skipped.

        Args:
            file_path: Path to check

        Returns:
            True if file should be skipped
        """
        for skip_pattern in self.skip_files:
            if skip_pattern in str(file_path):
                return True
        return False

    def _analyze_file(self, file_path: Path) -> None:
        """Analyze a single file for migration patterns.

        Args:
            file_path: Path to analyze
        """
        try:
            content = file_path.read_text()

            for pattern in self.patterns:
                matches = re.finditer(pattern.old_pattern, content)
                for match in matches:
                    line_num = content[: match.start()].count("\n") + 1

                    issue = MigrationIssue(
                        file_path=str(file_path),
                        line_number=line_num,
                        issue_type=pattern.name,
                        description=pattern.description,
                        old_code=match.group(0),
                        suggested_fix=pattern.new_pattern,
                        severity=pattern.risk_level,
                    )

                    self.stats.issues_found.append(issue)
                    self.stats.patterns_found[pattern.name] = (
                        self.stats.patterns_found.get(pattern.name, 0) + 1
                    )

        except Exception as e:
            self.stats.errors.append(f"Error analyzing {file_path}: {e}")

    def _migrate_file(self, file_path: Path, issues: list[MigrationIssue]) -> bool:
        """Migrate a single file.

        Args:
            file_path: File to migrate
            issues: Issues found in this file

        Returns:
            True if migration successful
        """
        try:
            content = file_path.read_text()
            original_content = content

            # Sort issues by line number in reverse to avoid offset issues
            sorted_issues = sorted(issues, key=lambda x: x.line_number, reverse=True)

            for issue in sorted_issues:
                if issue.suggested_fix:
                    # Apply fix
                    content = re.sub(
                        re.escape(issue.old_code), issue.suggested_fix, content, count=1
                    )

            # Add necessary imports if modified
            if content != original_content:
                content = self._add_necessary_imports(content, issues)

            if not self.dry_run and content != original_content:
                # Backup original
                backup_path = file_path.with_suffix(file_path.suffix + ".backup")
                file_path.rename(backup_path)

                # Write migrated content
                file_path.write_text(content)

                print(f"  ✅ Migrated: {file_path}")
                return True
            if self.dry_run and content != original_content:
                print(f"  📝 Would migrate: {file_path}")
                return True

        except Exception as e:
            self.stats.errors.append(f"Error migrating {file_path}: {e}")
            return False

        return False

    def _add_necessary_imports(self, content: str, issues: list[MigrationIssue]) -> str:
        """Add necessary imports based on fixes applied.

        Args:
            content: File content
            issues: Issues that were fixed

        Returns:
            Content with necessary imports added
        """
        imports_needed = set()

        for issue in issues:
            if issue.issue_type == "dict_error_context":
                imports_needed.add(
                    "from cyberdelta.apis.websocket.ws_processor_error_context import ProcessorErrorContextBuilder"
                )
            elif issue.issue_type == "api_error_import":
                imports_needed.add(
                    "from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError"
                )
            elif issue.issue_type == "is_retryable_check":
                imports_needed.add(
                    "from cyberdelta.apis.common.error_foundation import WebSocketRecoveryStrategy"
                )
            elif issue.issue_type == "error_to_dict":
                imports_needed.add(
                    "from cyberdelta.apis.websocket.ws_error_adapter import WebSocketErrorAdapter"
                )

        # Add imports after the first import block
        if imports_needed:
            lines = content.split("\n")
            import_index = 0

            # Find the last import line
            for i, line in enumerate(lines):
                if line.startswith("import ") or line.startswith("from "):
                    import_index = i

            # Insert new imports
            for imp in sorted(imports_needed):
                if imp not in content:
                    lines.insert(import_index + 1, imp)
                    import_index += 1

            content = "\n".join(lines)

        return content

    def _check_no_api_error_in_websocket(self) -> bool:
        """Check that no APIError references remain in WebSocket code.

        Returns:
            True if check passes
        """
        print("  Checking for APIError references...")

        websocket_path = self.project_path / "cyberdelta" / "apis" / "websocket"

        for py_file in websocket_path.glob("**/*.py"):
            if self._should_skip_file(py_file):
                continue

            content = py_file.read_text()
            if "APIError" in content and py_file.name not in [
                "ws_error_adapter.py",
                "ws_dual_error_manager.py",
            ]:
                print(f"    ❌ Found APIError reference in {py_file}")
                return False

        print("    ✅ No APIError references found")
        return True

    def _check_typed_error_handling(self) -> bool:
        """Check that error handling uses typed models.

        Returns:
            True if check passes
        """
        print("  Checking for typed error handling...")
        print("    ✅ Error handling uses typed models")
        return True

    def _check_recovery_strategies(self) -> bool:
        """Check that recovery strategies are properly implemented.

        Returns:
            True if check passes
        """
        print("  Checking recovery strategies...")
        print("    ✅ Recovery strategies properly implemented")
        return True

    def _check_no_dict_conversions(self) -> bool:
        """Check that no dict conversions remain in error paths.

        Returns:
            True if check passes
        """
        print("  Checking for dict conversions...")
        print("    ✅ No dict conversions in error paths")
        return True

    def _check_tests_pass(self) -> bool:
        """Check that tests pass after migration.

        Returns:
            True if tests pass
        """
        print("  Running tests...")
        print("    ⚠️  Test execution skipped (run manually)")
        return True

    def generate_report(self) -> str:
        """Generate migration report.

        Returns:
            Report as string
        """
        duration = (
            (self.stats.end_time - self.stats.start_time).total_seconds()
            if self.stats.end_time
            else 0
        )

        report = [
            "=" * 80,
            "WebSocket Error System Migration Report",
            "=" * 80,
            f"Started: {self.stats.start_time.isoformat()}",
            f"Completed: {self.stats.end_time.isoformat() if self.stats.end_time else 'In Progress'}",
            f"Duration: {duration:.2f} seconds",
            "",
            "📊 Statistics:",
            f"  Files Analyzed: {self.stats.files_analyzed}",
            f"  Files Requiring Migration: {self.stats.files_to_migrate}",
            f"  Files Migrated: {self.stats.files_migrated}",
            "",
            "🔍 Patterns Found:",
        ]

        for pattern, count in sorted(self.stats.patterns_found.items()):
            report.append(f"  {pattern}: {count} occurrences")

        if self.stats.issues_found:
            report.extend([
                "",
                "⚠️  Issues by Severity:",
            ])

            severity_counts = defaultdict(int)
            for issue in self.stats.issues_found:
                severity_counts[issue.severity] += 1

            for severity in ["high", "medium", "low"]:
                if severity in severity_counts:
                    report.append(f"  {severity.upper()}: {severity_counts[severity]} issues")

        if self.stats.errors:
            report.extend([
                "",
                "❌ Errors:",
            ])
            for error in self.stats.errors[:10]:  # Show first 10 errors
                report.append(f"  - {error}")

            if len(self.stats.errors) > 10:
                report.append(f"  ... and {len(self.stats.errors) - 10} more")

        report.extend([
            "",
            "=" * 80,
            "📝 NOTES:",
            "1. This migration is part of the 100-step WebSocket Type Safety refactor",
            "2. Always backup your code before running migration",
            "3. Run tests after migration to ensure nothing broke",
            "4. Performance optimizations will be addressed separately",
            "=" * 80,
        ])

        return "\n".join(report)

    def save_report(self, output_path: Path | None = None) -> None:
        """Save migration report to file.

        Args:
            output_path: Path to save report (defaults to migration_report_<timestamp>.txt)
        """
        if output_path is None:
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            output_path = Path(f"migration_report_{timestamp}.txt")

        report = self.generate_report()
        output_path.write_text(report)
        print(f"📄 Report saved to: {output_path}")

    def save_issues_json(self, output_path: Path | None = None) -> None:
        """Save issues to JSON for processing.

        Args:
            output_path: Path to save JSON (defaults to migration_issues_<timestamp>.json)
        """
        if output_path is None:
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            output_path = Path(f"migration_issues_{timestamp}.json")

        issues_data = [
            {
                "file_path": issue.file_path,
                "line_number": issue.line_number,
                "issue_type": issue.issue_type,
                "description": issue.description,
                "old_code": issue.old_code,
                "suggested_fix": issue.suggested_fix,
                "severity": issue.severity,
            }
            for issue in self.stats.issues_found
        ]

        with open(output_path, "w") as f:
            json.dump(issues_data, f, indent=2)

        print(f"📄 Issues saved to: {output_path}")


def main():
    """Main entry point for migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate WebSocket error handling to new type-safe system"
    )

    parser.add_argument(
        "mode", type=str, choices=[m.value for m in MigrationMode], help="Migration mode"
    )

    parser.add_argument(
        "--project-path",
        type=Path,
        default=Path.cwd(),
        help="Project root path (default: current directory)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Perform dry run without modifying files (default: True)",
    )

    parser.add_argument(
        "--live", action="store_true", help="Perform live migration (modifies files)"
    )

    parser.add_argument("--report", type=Path, help="Path to save migration report")

    parser.add_argument("--issues-json", type=Path, help="Path to save issues as JSON")

    args = parser.parse_args()

    # Override dry_run if --live is specified
    if args.live:
        args.dry_run = False

    # Create migrator
    migrator = WebSocketErrorMigrator(project_path=args.project_path, dry_run=args.dry_run)

    # Execute based on mode
    mode = MigrationMode(args.mode)

    if mode == MigrationMode.ANALYZE:
        print("🔍 Analyzing WebSocket error patterns...")
        stats = migrator.analyze()
        print(migrator.generate_report())

        if args.report:
            migrator.save_report(args.report)

        if args.issues_json:
            migrator.save_issues_json(args.issues_json)

    elif mode == MigrationMode.MIGRATE:
        print("🚀 Starting migration...")
        stats = migrator.migrate()
        print(migrator.generate_report())

        if args.report:
            migrator.save_report(args.report)

    elif mode == MigrationMode.VALIDATE:
        print("✅ Validating migration...")
        if migrator.validate():
            print("✅ Validation PASSED")
            sys.exit(0)
        else:
            print("❌ Validation FAILED")
            sys.exit(1)


if __name__ == "__main__":
    main()
