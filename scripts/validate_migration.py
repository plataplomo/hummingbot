#!/usr/bin/env python
"""Migration validation script for event bus refactor.

This script validates the migration from DomainEvent to msgspec events.
"""

import ast
import sys
from pathlib import Path

from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class MigrationValidator:
    """Validates the migration from DomainEvent to msgspec."""

    def __init__(self, project_root: Path) -> None:
        """Initialize validator with project root."""
        self.project_root = project_root
        self.issues: list[str] = []
        self.warnings: list[str] = []
        self.stats = {
            "files_checked": 0,
            "domain_event_refs": 0,
            "event_type_refs": 0,
            "entity_type_refs": 0,
            "old_event_bus_refs": 0,
            "msgspec_events": 0,
        }

    def validate_file(self, file_path: Path) -> None:
        """Validate a single Python file."""
        self.stats["files_checked"] += 1

        try:
            content = file_path.read_text(encoding="utf-8")

            # Check for old imports
            if "from cyberdelta.models.events import DomainEvent" in content:
                self.issues.append(f"{file_path}: Still imports DomainEvent")
                self.stats["domain_event_refs"] += 1

            if (
                "from cyberdelta.application.event_bus import EventBus" in content
                and "event_bus.py" not in str(file_path)
            ):
                self.warnings.append(f"{file_path}: Still imports old EventBus")
                self.stats["old_event_bus_refs"] += 1

            if "from cyberdelta.enums.events import EventType" in content:
                self.issues.append(f"{file_path}: Still imports EventType enum")
                self.stats["event_type_refs"] += 1

            if "from cyberdelta.enums.events import EntityType" in content:
                self.issues.append(f"{file_path}: Still imports EntityType enum")
                self.stats["entity_type_refs"] += 1

            # Check for old patterns
            if "DomainEvent(" in content:
                self.issues.append(f"{file_path}: Still creates DomainEvent instances")

            if "event.data.get(" in content or 'event.data["' in content:
                self.warnings.append(f"{file_path}: Still uses event.data dictionary access")

            # Check for new imports (good)
            if "from cyberdelta.models.events.core import" in content:
                self.stats["msgspec_events"] += 1

            if "from cyberdelta.infrastructure.event_bus import EventBus" in content:
                self.stats["msgspec_events"] += 1

        except (OSError, UnicodeDecodeError, ValueError) as e:
            logger.warning("file_read_error", file_path=str(file_path), error=str(e))
            self.warnings.append(f"{file_path}: Error reading file: {e}")

    def validate_directory(self, directory: Path) -> None:
        """Recursively validate all Python files in directory."""
        for py_file in directory.rglob("*.py"):
            # Skip test files and migration-related files
            if "test" in str(py_file) or "migration" in str(py_file):
                continue
            # Skip __pycache__
            if "__pycache__" in str(py_file):
                continue
            self.validate_file(py_file)

    def check_imports(self) -> None:
        """Check for problematic imports using AST."""
        cyberdelta_path = self.project_root / "cyberdelta"
        self._analyze_files_in_path(cyberdelta_path)

    def _analyze_files_in_path(self, path: Path) -> None:
        """Analyze Python files in the given path."""
        for py_file in path.rglob("*.py"):
            if "__pycache__" in str(py_file):
                continue
            self._analyze_single_file(py_file)

    def _analyze_single_file(self, py_file: Path) -> None:
        """Analyze a single Python file for imports."""
        try:
            content = py_file.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(py_file))
            self._check_imports_in_tree(tree, py_file)
        except (OSError, UnicodeDecodeError, SyntaxError) as e:
            logger.warning("ast_parse_error", file_path=str(py_file), error=str(e))
            self.warnings.append(f"{py_file}: AST parse error: {e}")

    def _check_imports_in_tree(self, tree: ast.AST, py_file: Path) -> None:
        """Check imports in the AST tree."""
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "cyberdelta.models.events":
                self._check_domain_event_imports(node, py_file)

    def _check_domain_event_imports(self, node: ast.ImportFrom, py_file: Path) -> None:
        """Check for DomainEvent imports in the given node."""
        for alias in node.names:
            if alias.name == "DomainEvent":
                self.issues.append(f"{py_file}: AST found DomainEvent import")

    def generate_report(self) -> str:
        """Generate validation report.

        Returns:
            str: Formatted validation report
        """
        report = [
            "=" * 60,
            "EVENT BUS MIGRATION VALIDATION REPORT",
            "=" * 60,
        ]

        # Statistics
        report.extend([
            "\n## Statistics",
            f"Files checked: {self.stats['files_checked']}",
            f"DomainEvent references: {self.stats['domain_event_refs']}",
            f"EventType enum references: {self.stats['event_type_refs']}",
            f"EntityType enum references: {self.stats['entity_type_refs']}",
            f"Old EventBus references: {self.stats['old_event_bus_refs']}",
            f"Files using msgspec events: {self.stats['msgspec_events']}",
        ])

        # Issues (must be fixed)
        if self.issues:
            report.extend(["\n## ISSUES (Must Fix)", *[f"  ❌ {issue}" for issue in self.issues]])
        else:
            report.append("\n## ✅ No Critical Issues Found")

        # Warnings (should review)
        if self.warnings:
            report.extend([
                "\n## WARNINGS (Should Review)",
                *[f"  ⚠️  {warning}" for warning in self.warnings],
            ])

        # Migration status
        report.extend([
            "\n## Migration Status",
            (
                "✅ No DomainEvent references found"
                if self.stats["domain_event_refs"] == 0
                else f"❌ {self.stats['domain_event_refs']} files still use DomainEvent"
            ),
            (
                "✅ No EventType enum references found"
                if self.stats["event_type_refs"] == 0
                else f"❌ {self.stats['event_type_refs']} files still use EventType enum"
            ),
        ])

        if self.stats["msgspec_events"] > 0:
            report.append(f"✅ {self.stats['msgspec_events']} files using new msgspec events")

        report.append("=" * 60)
        return "\n".join(report)

    def run(self) -> bool:
        """Run validation and return success status.

        Returns:
            bool: True if no critical issues found, False otherwise
        """
        cyberdelta_path = self.project_root / "cyberdelta"

        logger.info("migration_validation_started", project_root=str(self.project_root))
        self.validate_directory(cyberdelta_path)
        self.check_imports()

        report = self.generate_report()
        logger.info(
            "migration_validation_completed",
            files_checked=self.stats["files_checked"],
            issues_found=len(self.issues),
            warnings_found=len(self.warnings),
        )

        # Write report to file
        report_path = self.project_root / "migration_validation_report.txt"
        report_path.write_text(report, encoding="utf-8")
        logger.info("report_saved", report_path=str(report_path))

        # Also output report to console for immediate viewing
        logger.info("migration_report", report=report)

        # Return True if no critical issues
        return len(self.issues) == 0


def _search_pattern_in_file(pattern: str, py_file: Path, max_matches: int) -> list[str]:
    """Search for pattern in a single file.

    Returns:
        list[str]: List of matching lines with file path and line number
    """
    matches: list[str] = []
    try:
        content = py_file.read_text(encoding="utf-8")
        if pattern not in content:
            return matches

        for line_num, line in enumerate(content.split("\n"), 1):
            if pattern in line and len(matches) < max_matches:
                matches.append(f"{py_file}:{line_num}: {line.strip()}")
    except (OSError, UnicodeDecodeError):
        pass
    return matches


def check_specific_patterns() -> None:
    """Check for specific patterns that need migration."""
    max_matches_per_pattern = 5

    patterns_to_check = [
        ("EventType.", "EventType enum usage"),
        ("EntityType.", "EntityType enum usage"),
        ("DomainEvent(", "DomainEvent instantiation"),
        ("event.data.get(", "Dictionary access pattern"),
        ("event.data[", "Dictionary access pattern"),
        ("EventBus", "Old EventBus class"),
    ]

    logger.info("pattern_search_started")
    cyberdelta_path = Path("cyberdelta")

    if not cyberdelta_path.exists():
        logger.warning("cyberdelta_directory_not_found")
        return

    for pattern, description in patterns_to_check:
        logger.info("searching_pattern", pattern=pattern, description=description)
        all_matches: list[str] = []

        for py_file in cyberdelta_path.rglob("*.py"):
            file_matches = _search_pattern_in_file(pattern, py_file, max_matches_per_pattern)
            all_matches.extend(file_matches)
            if len(all_matches) >= max_matches_per_pattern:
                break

        for match in all_matches[:max_matches_per_pattern]:
            logger.info("pattern_match", pattern=pattern, match=match)


def main() -> int:
    """Main entry point.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    # Get project root
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent

    logger.info("migration_validation_main", project_root=str(project_root))

    # Run validation
    validator = MigrationValidator(project_root)
    success = validator.run()

    # Additional pattern checking
    check_specific_patterns()

    # Return exit code
    exit_code = 0 if success else 1
    logger.info("migration_validation_complete", success=success, exit_code=exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
