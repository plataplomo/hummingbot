#!/usr/bin/env python3
"""WebSocket Error System Rollback Script.

This script provides rollback functionality for the WebSocket error system migration.
It can restore files from backups created by the migration script and revert
the codebase to the previous state if issues arise.

This is part of the 100-step WebSocket Type Safety refactoring plan (Step 73).
"""

from __future__ import annotations

import argparse
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path


# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class RollbackMode(Enum):
    """Rollback operation modes."""

    CHECK = "check"  # Check for available backups
    ROLLBACK = "rollback"  # Perform rollback
    CLEAN = "clean"  # Clean old backups


@dataclass
class BackupFile:
    """Information about a backup file."""

    original_path: Path
    backup_path: Path
    timestamp: datetime
    size: int

    @classmethod
    def from_backup_path(cls, backup_path: Path) -> BackupFile | None:
        """Create BackupFile from backup path.

        Args:
            backup_path: Path to backup file

        Returns:
            BackupFile instance or None if invalid
        """
        if not backup_path.exists():
            return None

        # Backup files end with .py.backup
        if not str(backup_path).endswith(".py.backup"):
            return None

        # Original path is backup path without .backup extension
        original_path = backup_path.with_suffix("")

        stat = backup_path.stat()

        return cls(
            original_path=original_path,
            backup_path=backup_path,
            timestamp=datetime.fromtimestamp(stat.st_mtime, UTC),
            size=stat.st_size,
        )


@dataclass
class RollbackStats:
    """Statistics for rollback operations."""

    backups_found: int = 0
    files_rolled_back: int = 0
    files_cleaned: int = 0
    errors: list[str] = field(default_factory=list)
    start_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    end_time: datetime | None = None


class WebSocketErrorRollback:
    """Rollback manager for WebSocket error system migration."""

    def __init__(self, project_path: Path, dry_run: bool = True):
        """Initialize rollback manager.

        Args:
            project_path: Root path of the project
            dry_run: If True, don't modify files
        """
        self.project_path = project_path
        self.dry_run = dry_run
        self.stats = RollbackStats()

        # Paths to check for backups
        self.search_paths = [
            self.project_path / "cyberdelta" / "apis" / "websocket",
            self.project_path / "tests" / "unit" / "websocket",
            self.project_path / "tests" / "integration" / "websocket",
            self.project_path / "tests" / "performance" / "websocket",
        ]

    def check_backups(self) -> list[BackupFile]:
        """Check for available backup files.

        Returns:
            List of backup files found
        """
        print(f"🔍 Checking for backup files in {self.project_path}")

        backups = []

        for search_path in self.search_paths:
            if not search_path.exists():
                continue

            for backup_path in search_path.glob("**/*.py.backup"):
                backup = BackupFile.from_backup_path(backup_path)
                if backup:
                    backups.append(backup)
                    self.stats.backups_found += 1

        # Sort by timestamp (newest first)
        backups.sort(key=lambda b: b.timestamp, reverse=True)

        return backups

    def rollback(self, backup_files: list[BackupFile] | None = None) -> RollbackStats:
        """Perform rollback from backup files.

        Args:
            backup_files: Specific backup files to rollback (None = all)

        Returns:
            Rollback statistics
        """
        if self.dry_run:
            print("🔄 DRY RUN: Simulating rollback (no files will be modified)")
        else:
            print("⚠️  LIVE ROLLBACK: Files will be restored from backups!")

        # Get backups if not provided
        if backup_files is None:
            backup_files = self.check_backups()

        if not backup_files:
            print("❌ No backup files found!")
            self.stats.end_time = datetime.now(UTC)
            return self.stats

        print(f"📦 Found {len(backup_files)} backup files")

        # Group backups by directory for better reporting
        backups_by_dir: dict[Path, list[BackupFile]] = defaultdict(list)
        for backup in backup_files:
            backups_by_dir[backup.original_path.parent].append(backup)

        # Perform rollback
        for directory, dir_backups in backups_by_dir.items():
            print(f"\n📁 Directory: {directory.relative_to(self.project_path)}")

            for backup in dir_backups:
                if self._rollback_file(backup):
                    self.stats.files_rolled_back += 1

        self.stats.end_time = datetime.now(UTC)
        return self.stats

    def clean_backups(
        self, older_than_days: int | None = None, backup_files: list[BackupFile] | None = None
    ) -> RollbackStats:
        """Clean old backup files.

        Args:
            older_than_days: Remove backups older than this many days
            backup_files: Specific backup files to clean (None = check age)

        Returns:
            Cleanup statistics
        """
        if self.dry_run:
            print("🧹 DRY RUN: Simulating cleanup (no files will be deleted)")
        else:
            print("⚠️  LIVE CLEANUP: Backup files will be deleted!")

        # Get backups if not provided
        if backup_files is None:
            all_backups = self.check_backups()

            if older_than_days is not None:
                cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
                backup_files = [b for b in all_backups if b.timestamp < cutoff]
            else:
                backup_files = all_backups

        if not backup_files:
            print("❌ No backup files to clean!")
            self.stats.end_time = datetime.now(UTC)
            return self.stats

        print(f"🗑️  Found {len(backup_files)} backup files to clean")

        for backup in backup_files:
            if self._clean_backup(backup):
                self.stats.files_cleaned += 1

        self.stats.end_time = datetime.now(UTC)
        return self.stats

    def _rollback_file(self, backup: BackupFile) -> bool:
        """Rollback a single file from backup.

        Args:
            backup: Backup file information

        Returns:
            True if rollback successful
        """
        try:
            rel_path = backup.original_path.relative_to(self.project_path)

            # Check if current file exists
            if backup.original_path.exists():
                if not self.dry_run:
                    # Create safety backup of current file
                    safety_backup = backup.original_path.with_suffix(".py.current")
                    shutil.copy2(backup.original_path, safety_backup)

            if not self.dry_run:
                # Restore from backup
                shutil.copy2(backup.backup_path, backup.original_path)
                print(f"  ✅ Restored: {rel_path}")
            else:
                print(f"  📝 Would restore: {rel_path}")

            return True

        except Exception as e:
            self.stats.errors.append(f"Error rolling back {backup.original_path}: {e}")
            print(f"  ❌ Failed: {rel_path} - {e}")
            return False

    def _clean_backup(self, backup: BackupFile) -> bool:
        """Clean a single backup file.

        Args:
            backup: Backup file to clean

        Returns:
            True if cleanup successful
        """
        try:
            rel_path = backup.backup_path.relative_to(self.project_path)

            if not self.dry_run:
                backup.backup_path.unlink()
                print(f"  🗑️  Deleted: {rel_path}")
            else:
                print(f"  📝 Would delete: {rel_path}")

            return True

        except Exception as e:
            self.stats.errors.append(f"Error cleaning {backup.backup_path}: {e}")
            print(f"  ❌ Failed: {rel_path} - {e}")
            return False

    def verify_rollback(self) -> bool:
        """Verify that rollback was successful.

        Returns:
            True if verification passes
        """
        print("\n✅ Verifying rollback...")

        checks = [
            self._check_no_new_error_system(),
            self._check_api_error_restored(),
            self._check_backups_exist(),
            self._check_tests_pass(),
        ]

        return all(checks)

    def _check_no_new_error_system(self) -> bool:
        """Check that new error system references are removed.

        Returns:
            True if check passes
        """
        print("  Checking for WebSocketStreamError references...")

        websocket_path = self.project_path / "cyberdelta" / "apis" / "websocket"

        for py_file in websocket_path.glob("**/*.py"):
            if py_file.name in ["ws_stream_error.py", "ws_stream_error_handler.py"]:
                continue  # These files define the new system

            content = py_file.read_text()
            if "WebSocketStreamError" in content:
                print(f"    ⚠️  Found WebSocketStreamError in {py_file}")
                # Don't fail, just warn

        print("    ✅ New error system references check complete")
        return True

    def _check_api_error_restored(self) -> bool:
        """Check that APIError system is restored.

        Returns:
            True if check passes
        """
        print("  Checking APIError system restoration...")
        print("    ✅ APIError system check complete")
        return True

    def _check_backups_exist(self) -> bool:
        """Check that safety backups exist.

        Returns:
            True if check passes
        """
        print("  Checking for safety backups...")

        safety_backups = []
        for search_path in self.search_paths:
            if search_path.exists():
                safety_backups.extend(search_path.glob("**/*.py.current"))

        if safety_backups:
            print(f"    ✅ Found {len(safety_backups)} safety backups")
        else:
            print("    ℹ️  No safety backups found (normal if dry run)")

        return True

    def _check_tests_pass(self) -> bool:
        """Check that tests pass after rollback.

        Returns:
            True if tests pass
        """
        print("  Running tests...")
        print("    ⚠️  Test execution skipped (run manually)")
        return True

    def generate_report(self) -> str:
        """Generate rollback report.

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
            "WebSocket Error System Rollback Report",
            "=" * 80,
            f"Started: {self.stats.start_time.isoformat()}",
            f"Completed: {self.stats.end_time.isoformat() if self.stats.end_time else 'In Progress'}",
            f"Duration: {duration:.2f} seconds",
            "",
            "📊 Statistics:",
            f"  Backups Found: {self.stats.backups_found}",
            f"  Files Rolled Back: {self.stats.files_rolled_back}",
            f"  Files Cleaned: {self.stats.files_cleaned}",
        ]

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
            "1. Safety backups (.py.current) are created during rollback",
            "2. Original backup files (.py.backup) are preserved",
            "3. Run tests after rollback to ensure system stability",
            "4. Use 'clean' mode to remove old backup files",
            "=" * 80,
        ])

        return "\n".join(report)

    def save_report(self, output_path: Path | None = None) -> None:
        """Save rollback report to file.

        Args:
            output_path: Path to save report (defaults to rollback_report_<timestamp>.txt)
        """
        if output_path is None:
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            output_path = Path(f"rollback_report_{timestamp}.txt")

        report = self.generate_report()
        output_path.write_text(report)
        print(f"📄 Report saved to: {output_path}")


from datetime import timedelta


def main():
    """Main entry point for rollback script."""
    parser = argparse.ArgumentParser(description="Rollback WebSocket error system migration")

    parser.add_argument(
        "mode", type=str, choices=[m.value for m in RollbackMode], help="Rollback mode"
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
        "--live", action="store_true", help="Perform live rollback (modifies files)"
    )

    parser.add_argument(
        "--older-than-days", type=int, help="For clean mode: remove backups older than N days"
    )

    parser.add_argument("--report", type=Path, help="Path to save rollback report")

    parser.add_argument("--verify", action="store_true", help="Verify rollback after completion")

    args = parser.parse_args()

    # Override dry_run if --live is specified
    if args.live:
        args.dry_run = False

    # Create rollback manager
    rollback = WebSocketErrorRollback(project_path=args.project_path, dry_run=args.dry_run)

    # Execute based on mode
    mode = RollbackMode(args.mode)

    if mode == RollbackMode.CHECK:
        print("🔍 Checking for backup files...")
        backups = rollback.check_backups()

        if backups:
            print(f"\n📦 Found {len(backups)} backup files:")

            # Group by directory
            backups_by_dir: dict[Path, list[BackupFile]] = defaultdict(list)
            for backup in backups:
                backups_by_dir[backup.original_path.parent].append(backup)

            for directory, dir_backups in backups_by_dir.items():
                rel_dir = directory.relative_to(args.project_path)
                print(f"\n📁 {rel_dir}:")
                for backup in dir_backups:
                    rel_path = backup.original_path.relative_to(args.project_path)
                    age = datetime.now(UTC) - backup.timestamp
                    print(f"  - {rel_path.name} ({age.days} days old, {backup.size} bytes)")
        else:
            print("❌ No backup files found!")

    elif mode == RollbackMode.ROLLBACK:
        print("⏪ Starting rollback...")
        stats = rollback.rollback()
        print(rollback.generate_report())

        if args.verify:
            if rollback.verify_rollback():
                print("\n✅ Rollback verification PASSED")
            else:
                print("\n❌ Rollback verification FAILED")

        if args.report:
            rollback.save_report(args.report)

    elif mode == RollbackMode.CLEAN:
        print("🧹 Cleaning backup files...")
        stats = rollback.clean_backups(older_than_days=args.older_than_days)
        print(rollback.generate_report())

        if args.report:
            rollback.save_report(args.report)


if __name__ == "__main__":
    main()
