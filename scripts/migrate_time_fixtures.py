#!/usr/bin/env python3
"""Script to migrate unittest.mock datetime patches to pytest-freezer fixtures.

This script automates the migration of simple @patch patterns to use the
centralized time fixtures from tests/fixtures/time_fixtures.py.
"""

import argparse
import ast
import re
import sys
from pathlib import Path


class TimePatchMigrator(ast.NodeTransformer):
    """AST transformer to migrate time patches to fixture usage."""

    def __init__(self) -> None:
        """Initialize the migrator."""
        self.has_frozen_time_fixture = False
        self.has_mock_time_patch_fixture = False
        self.patches_found: list[ast.expr] = []
        self.imports_to_add: set[str] = set()
        self.imports_to_remove: set[str] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
        """Visit function definitions to check for patches and fixtures."""
        # Check for existing fixtures
        for arg in node.args.args:
            if arg.arg == "frozen_time":
                self.has_frozen_time_fixture = True
            elif arg.arg == "mock_time_patch":
                self.has_mock_time_patch_fixture = True

        # Check decorators for patches
        new_decorators: list[ast.expr] = []
        for decorator in node.decorator_list:
            if self._is_datetime_patch(decorator):
                self.patches_found.append(decorator)
                # Add fixture parameter if not present
                if not self.has_frozen_time_fixture:
                    node.args.args.append(ast.arg(arg="frozen_time", annotation=None))
                    self.has_frozen_time_fixture = True
                    self.imports_to_add.add("frozen_time")
            elif self._is_time_patch(decorator):
                self.patches_found.append(decorator)
                # Add fixture parameter if not present
                if not self.has_mock_time_patch_fixture:
                    node.args.args.append(ast.arg(arg="mock_time_patch", annotation=None))
                    self.has_mock_time_patch_fixture = True
                    self.imports_to_add.add("mock_time_patch")
            else:
                new_decorators.append(decorator)

        node.decorator_list = new_decorators
        self.generic_visit(node)
        return node

    def _is_datetime_patch(self, decorator: ast.AST) -> bool:
        """Check if decorator is a datetime patch."""
        if (isinstance(decorator, ast.Call)) and (
            (isinstance(decorator.func, ast.Name) and decorator.func.id == "patch")
            and (decorator.args and isinstance(decorator.args[0], ast.Constant))
        ):
            patch_target = decorator.args[0].value
            return "datetime" in patch_target and "time.time" not in patch_target
        return False

    def _is_time_patch(self, decorator: ast.AST) -> bool:
        """Check if decorator is a time.time patch."""
        if (isinstance(decorator, ast.Call)) and (
            (isinstance(decorator.func, ast.Name) and decorator.func.id == "patch")
            and (decorator.args and isinstance(decorator.args[0], ast.Constant))
        ):
            patch_target = decorator.args[0].value
            return bool(patch_target == "time.time")
        return False


def _check_migration_needed(content: str, file_path: Path) -> tuple[bool, str] | None:
    """Check if file needs migration. Returns None if migration should proceed."""
    # Quick check if file needs migration
    if not re.search(r"@patch.*datetime|@patch.*time\.time", content):
        return True, f"⚠ {file_path} - no datetime/time patches found"

    # Check if already migrated
    if "frozen_time" in content or "mock_time_patch" in content:
        if re.search(r"@patch.*datetime|@patch.*time\.time", content):
            return True, f"⚠ {file_path} - partially migrated, manual review needed"
        return True, f"✓ {file_path} - already migrated"

    return None


def _process_ast_migration(content: str, file_path: Path) -> tuple[TimePatchMigrator, str]:
    """Process AST transformation for migration."""
    # Parse the AST
    tree = ast.parse(content, filename=str(file_path))

    # Transform the AST
    migrator = TimePatchMigrator()
    new_tree = migrator.visit(tree)

    # Generate new code
    new_content = ast.unparse(new_tree)

    return migrator, new_content


def _add_imports_and_cleanup(migrator: TimePatchMigrator, new_content: str) -> str:
    """Add necessary imports and clean up unused ones."""
    lines = new_content.split("\n")
    import_index = -1

    # Find last import
    for i, line in enumerate(lines):
        if line.strip() and (line.startswith("import ") or line.startswith("from ")):
            import_index = i

    # Add time fixture imports if needed
    if migrator.imports_to_add and import_index >= 0:
        fixture_imports: list[str] = []
        if "frozen_time" in migrator.imports_to_add:
            fixture_imports.append("from tests.fixtures.time_fixtures import FreezerProtocol")

        # Insert after last import
        for imp in reversed(fixture_imports):
            lines.insert(import_index + 1, imp)

    # Remove unittest.mock import if no longer needed
    new_lines: list[str] = []
    for line in lines:
        if "from unittest.mock import patch" in line and not re.search(
            r'@patch\s*\((?!".*datetime|"time\.time")', "\n".join(lines)
        ):
            continue  # Skip this import
        new_lines.append(line)

    return "\n".join(new_lines)


def migrate_file(file_path: Path, dry_run: bool = False) -> tuple[bool, str]:
    """Migrate a single file from unittest.mock to pytest-freezer."""
    try:
        content = file_path.read_text()

        # Check if migration is needed
        check_result = _check_migration_needed(content, file_path)
        if check_result is not None:
            return check_result

        # Process AST migration
        migrator, new_content = _process_ast_migration(content, file_path)

        if not migrator.patches_found:
            return True, f"⚠ {file_path} - no patches found in AST"

        # Add imports and cleanup
        final_content = _add_imports_and_cleanup(migrator, new_content)

        # Write back if not dry run
        if not dry_run:
            file_path.write_text(final_content)

        return True, f"✅ {file_path} - migrated {len(migrator.patches_found)} patches"

    except Exception as e:
        return False, f"❌ {file_path} - error: {e!s}"


def find_migration_candidates(directory: Path) -> list[Path]:
    """Find test files that are candidates for migration."""
    candidates: list[Path] = []

    for test_file in directory.rglob("test_*.py"):
        content = test_file.read_text()
        # Look for unittest.mock datetime patches
        if (
            re.search(r"@patch.*datetime|with\s+patch.*datetime", content)
            and "frozen_time" not in content
        ):
            candidates.append(test_file)

    return candidates


def create_migration_example(file_path: Path) -> str:
    """Create an example of how to manually migrate complex patterns."""
    return f"""
Manual Migration Guide for {file_path}
=====================================

For complex patterns that can't be automatically migrated:

1. Replace @patch decorators:
   Before:
   ```python
   @patch("module.datetime")
   def test_something(mock_datetime):
       mock_datetime.now.return_value = fixed_time
   ```

   After:
   ```python
   def test_something(frozen_time):
       frozen_time.move_to(fixed_time)
   ```

2. Replace context managers:
   Before:
   ```python
   with patch("module.datetime") as mock_dt:
       mock_dt.now.return_value = fixed_time
   ```

   After:
   ```python
   def test_something(frozen_time):
       frozen_time.move_to(fixed_time)
   ```

3. For time.time() patches:
   Before:
   ```python
   @patch("time.time", return_value=1234567890)
   ```

   After:
   ```python
   def test_something(mock_time_patch):
       mock_time_patch.return_value = 1234567890
   ```
"""


def main() -> int:
    """Main script execution."""
    parser = argparse.ArgumentParser(
        description="Migrate unittest.mock datetime patches to pytest-freezer fixtures",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=Path("tests/unit/apis/backpack/mappers"),
        help="Directory to search for files to migrate",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without modifying files",
    )
    parser.add_argument("--file", type=Path, help="Migrate a specific file")

    args = parser.parse_args()

    if args.file:
        # Migrate single file
        success, message = migrate_file(args.file, args.dry_run)
        return 0 if success else 1

    # Find candidates
    candidates = find_migration_candidates(args.target_dir)

    if not candidates:
        return 0

    success_count = 0
    skip_count = 0
    error_count = 0

    for file_path in candidates:
        success, message = migrate_file(file_path, args.dry_run)

        if success:
            if "migrated" in message and "✅" in message:
                success_count += 1
            else:
                skip_count += 1
        else:
            error_count += 1

    if args.dry_run:
        pass

    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
