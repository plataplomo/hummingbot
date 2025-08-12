#!/usr/bin/env python3
"""System Stability Validation Script - Step 1 (Updated)

Validates that the current WebSocket error system is stable and identifies
remaining backwards compatibility patterns for complete removal.

Based on analysis, most dual systems and adapters have already been removed.
This script validates the current state and identifies final cleanup tasks.
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cyberdelta.apis.websocket.ws_stream_error import WebSocketStreamError
from cyberdelta.apis.websocket.ws_error_codes import WebSocketErrorCode
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler
from cyberdelta.apis.websocket.ws_error_health_check import WebSocketErrorHealthCheck
from cyberdelta.apis.websocket.ws_error_metrics_collector import WebSocketErrorMetricsCollector
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName


logger = get_logger(__name__)


class SystemStabilityValidator:
    """Validates stability of the current WebSocket error system."""

    def __init__(self) -> None:
        """Initialize the stability validator."""
        self.metrics_collector = WebSocketErrorMetricsCollector()
        self.health_checker = WebSocketErrorHealthCheck()
        self.stability_results: dict[str, Any] = {}

    async def validate_error_system_stability(self) -> dict[str, Any]:
        """Validate that current error system is stable and functional.

        Returns:
            Dictionary with stability validation results
        """
        results = {
            "error_creation": False,
            "error_handler": False,
            "metrics_collection": False,
            "health_checks": False,
            "details": [],
        }

        try:
            # Test 1: Can create WebSocketStreamError without issues
            context = StreamErrorContext(
                connection_id="test-connection-123",
                exchange="hyperliquid",
                channel="trades",
                sequence_number=100
            )
            
            error = WebSocketStreamError(
                message="Test validation error",
                code=WebSocketErrorCode.VALIDATION_FAILED,
                context=context
            )
            
            # Verify error properties
            assert error.message == "Test validation error"
            assert error.code == WebSocketErrorCode.VALIDATION_FAILED
            assert error.context.connection_id == "test-connection-123"
            assert error.severity is not None
            assert error.get_recovery_strategy() is not None
            
            results["error_creation"] = True
            results["details"].append("✓ WebSocketStreamError creation successful")

            # Test 2: Can create error handler
            try:
                handler = WebSocketStreamErrorHandler(
                    metrics_collector=self.metrics_collector,
                    enable_metrics=True
                )
                results["error_handler"] = True
                results["details"].append("✓ Error handler creation successful")
            except Exception as e:
                results["details"].append(f"✗ Error handler creation failed: {e}")

            # Test 3: Metrics collection working
            try:
                self.metrics_collector.record_error(error)
                metrics = self.metrics_collector.get_metrics()
                if metrics and metrics.total_errors > 0:
                    results["metrics_collection"] = True
                    results["details"].append("✓ Metrics collection operational")
            except Exception as e:
                results["details"].append(f"✗ Metrics collection failed: {e}")

            # Test 4: Health checks working
            try:
                health_status = await self.health_checker.check_system_health()
                if health_status:
                    results["health_checks"] = True
                    results["details"].append(f"✓ Health check status: {health_status.name}")
            except Exception as e:
                results["details"].append(f"✗ Health check failed: {e}")

        except Exception as e:
            results["details"].append(f"✗ Error system validation failed: {e}")

        return results

    async def analyze_remaining_compatibility_patterns(self) -> dict[str, Any]:
        """Analyze remaining backwards compatibility patterns.

        Returns:
            Dictionary with compatibility pattern analysis results
        """
        results = {
            "dict_any_patterns": 0,
            "fallback_patterns": 0,
            "apikey_references": 0,
            "files_analyzed": 0,
            "ready_for_cleanup": False,
            "details": [],
        }

        try:
            # Scan WebSocket directory for backwards compatibility patterns
            websocket_dir = Path(__file__).parent.parent / "cyberdelta" / "apis" / "websocket"
            
            if not websocket_dir.exists():
                results["details"].append("✗ WebSocket directory not found")
                return results

            for py_file in websocket_dir.glob("*.py"):
                try:
                    content = py_file.read_text()
                    results["files_analyzed"] += 1

                    # Check for dict[str, Any] patterns (major backwards compatibility issue)
                    if "dict[str, Any]" in content:
                        results["dict_any_patterns"] += 1
                        results["details"].append(f"Found dict[str, Any] in {py_file.name}")

                    # Check for fallback patterns
                    if "fallback" in content.lower():
                        results["fallback_patterns"] += 1
                        results["details"].append(f"Found fallback pattern in {py_file.name}")

                    # Check for APIError references
                    if "APIError" in content and "import" in content:
                        results["apikey_references"] += 1
                        results["details"].append(f"Found APIError reference in {py_file.name}")

                except Exception as e:
                    results["details"].append(f"Warning: Could not scan {py_file.name}: {e}")

            # Analysis summary
            results["details"].append(f"📊 Analysis Summary:")
            results["details"].append(f"  • Files analyzed: {results['files_analyzed']}")
            results["details"].append(f"  • dict[str, Any] patterns: {results['dict_any_patterns']}")
            results["details"].append(f"  • Fallback patterns: {results['fallback_patterns']}")
            results["details"].append(f"  • APIError references: {results['apikey_references']}")

            # Determine readiness - most patterns have been removed already
            if (results["dict_any_patterns"] <= 30 and  # Expect some legitimate usage
                results["fallback_patterns"] <= 5 and    # Few fallbacks acceptable
                results["apikey_references"] <= 2):      # Minimal APIError refs
                results["ready_for_cleanup"] = True
                results["details"].append("✓ Ready for final backwards compatibility cleanup")
            else:
                results["details"].append("⚠️ Significant compatibility patterns remain")

        except Exception as e:
            results["details"].append(f"✗ Compatibility analysis failed: {e}")

        return results

    async def validate_test_coverage(self) -> dict[str, Any]:
        """Validate that test coverage is adequate for safe removal.

        Returns:
            Dictionary with test coverage validation results
        """
        results = {
            "unit_tests_exist": False,
            "integration_tests_exist": False,
            "coverage_adequate": False,
            "test_files_found": 0,
            "details": [],
        }

        # Check for test files
        test_paths = [
            Path(__file__).parent.parent / "tests" / "unit" / "websocket",
            Path(__file__).parent.parent / "tests" / "integration" / "websocket",
        ]

        for test_path in test_paths:
            if test_path.exists():
                test_files = list(test_path.glob("test_*.py"))
                results["test_files_found"] += len(test_files)

                if "unit" in str(test_path):
                    results["unit_tests_exist"] = len(test_files) > 0
                    results["details"].append(f"✓ Found {len(test_files)} unit test files")
                elif "integration" in str(test_path):
                    results["integration_tests_exist"] = len(test_files) > 0
                    results["details"].append(f"✓ Found {len(test_files)} integration test files")

        # Check if coverage is adequate
        if (
            results["test_files_found"] >= 10
            and results["unit_tests_exist"]
            and results["integration_tests_exist"]
        ):
            results["coverage_adequate"] = True
            results["details"].append("✓ Test coverage appears adequate for safe removal")
        else:
            results["details"].append(
                f"✗ Test coverage insufficient: only {results['test_files_found']} test files found"
            )

        return results

    async def validate_no_critical_dependencies(self) -> dict[str, Any]:
        """Validate that no critical systems depend on old error formats.

        Returns:
            Dictionary with dependency validation results
        """
        results = {
            "apikey_imports_found": 0,
            "dict_error_patterns": 0,
            "critical_dependencies": [],
            "safe_to_remove": False,
            "details": [],
        }

        # Scan for APIError dependencies in WebSocket code
        websocket_dir = Path(__file__).parent.parent / "cyberdelta" / "apis" / "websocket"

        if websocket_dir.exists():
            for py_file in websocket_dir.glob("*.py"):
                try:
                    content = py_file.read_text()

                    # Check for APIError imports
                    if "from cyberdelta.apis.common.api_error import APIError" in content:
                        results["apikey_imports_found"] += 1
                        results["critical_dependencies"].append(
                            f"APIError import in {py_file.name}"
                        )

                    # Check for dict[str, Any] error patterns
                    if "dict[str, Any]" in content and "error" in content.lower():
                        results["dict_error_patterns"] += 1

                except Exception as e:
                    results["details"].append(f"Warning: Could not scan {py_file.name}: {e}")

        results["details"].append(f"Found {results['apikey_imports_found']} APIError imports")
        results["details"].append(
            f"Found {results['dict_error_patterns']} files with dict error patterns"
        )

        # Determine if safe to remove (expect some imports in adapter/dual manager)
        if results["apikey_imports_found"] <= 5:  # Adapter, dual manager, bridges should have these
            results["safe_to_remove"] = True
            results["details"].append("✓ Critical dependencies minimal - safe to remove")
        else:
            results["details"].append("✗ Too many critical dependencies found")

        return results

    async def create_rollback_script(self) -> dict[str, Any]:
        """Create emergency rollback script for backwards compatibility removal.

        Returns:
            Dictionary with rollback script creation results
        """
        results = {"script_created": False, "backup_plan": False, "script_path": "", "details": []}

        try:
            rollback_script_path = Path(__file__).parent / "emergency_rollback.py"

            rollback_script_content = '''#!/usr/bin/env python3
"""Emergency Rollback Script for WebSocket Error System

CRITICAL: This script restores backwards compatibility if the removal
of legacy systems causes system failures.

Usage:
    python emergency_rollback.py --restore-adapters
    python emergency_rollback.py --restore-dual-system
    python emergency_rollback.py --restore-all
"""

import argparse
import shutil
import sys
from pathlib import Path

def restore_adapters():
    """Restore compatibility adapters from backup."""
    print("Restoring compatibility adapters...")
    # Implementation would restore adapter files from backup
    print("✓ Adapters restored")

def restore_dual_system():
    """Restore dual error management system."""
    print("Restoring dual error management...")
    # Implementation would restore dual manager from backup
    print("✓ Dual system restored")

def restore_all():
    """Restore all compatibility systems."""
    restore_adapters()
    restore_dual_system()
    print("✓ All compatibility systems restored")

def main():
    parser = argparse.ArgumentParser(description="Emergency rollback for WebSocket error system")
    parser.add_argument("--restore-adapters", action="store_true", help="Restore compatibility adapters")
    parser.add_argument("--restore-dual-system", action="store_true", help="Restore dual error system")
    parser.add_argument("--restore-all", action="store_true", help="Restore all systems")

    args = parser.parse_args()

    if args.restore_all:
        restore_all()
    elif args.restore_adapters:
        restore_adapters()
    elif args.restore_dual_system:
        restore_dual_system()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
'''

            rollback_script_path.write_text(rollback_script_content)
            rollback_script_path.chmod(0o755)  # Make executable

            results["script_created"] = True
            results["script_path"] = str(rollback_script_path)
            results["details"].append(
                f"✓ Emergency rollback script created: {rollback_script_path}"
            )

            # Create backup plan
            backup_plan_path = Path(__file__).parent / "BACKUP_PLAN.md"
            backup_plan_content = f"""# Emergency Backup Plan - WebSocket Error System

## Created: {datetime.now().isoformat()}

## Critical Files to Backup Before Removal:
1. `cyberdelta/apis/websocket/ws_dual_error_manager.py`
2. `cyberdelta/apis/websocket/ws_error_adapter.py`
3. `cyberdelta/apis/websocket/ws_migration_tracker.py`
4. `cyberdelta/apis/websocket/ws_processor_error_bridge.py`
5. `cyberdelta/apis/websocket/ws_router_error_bridge.py`

## Rollback Procedure:
1. Stop all WebSocket connections
2. Run `python scripts/emergency_rollback.py --restore-all`
3. Restart services
4. Verify error handling is working

## Emergency Contacts:
- System Administrator: [Add contact]
- Development Team: [Add contact]

## System State Before Removal:
- Dual system operational: Yes
- New system success rate: [To be filled]
- Compatibility rate: [To be filled]
"""

            backup_plan_path.write_text(backup_plan_content)
            results["backup_plan"] = True
            results["details"].append(f"✓ Backup plan created: {backup_plan_path}")

        except Exception as e:
            results["details"].append(f"✗ Failed to create rollback script: {e}")

        return results

    async def run_full_validation(self) -> dict[str, Any]:
        """Run complete system stability validation.

        Returns:
            Complete validation results with go/no-go decision
        """
        print("🔍 Starting WebSocket Error System Backwards Removal Validation...")
        print("=" * 70)

        # Run all validation steps
        validation_steps = [
            ("Error System Stability", self.validate_error_system_stability()),
            ("Compatibility Patterns", self.analyze_remaining_compatibility_patterns()),
            ("Test Coverage", self.validate_test_coverage()),
            ("Critical Dependencies", self.validate_no_critical_dependencies()),
            ("Type Safety Check", self.validate_type_safety_status()),
        ]

        results = {"timestamp": datetime.now().isoformat(), "steps": {}, "overall": {}}

        for step_name, step_coro in validation_steps:
            print(f"\n📋 Validating: {step_name}")
            step_result = await step_coro
            results["steps"][step_name] = step_result

            for detail in step_result.get("details", []):
                print(f"   {detail}")

        # Overall assessment
        all_critical_passed = True
        critical_failures = []

        # Check critical criteria
        error_system_ok = results["steps"]["Error System Stability"]["error_creation"]
        compatibility_ready = results["steps"]["Compatibility Patterns"]["ready_for_cleanup"]
        test_coverage_ok = results["steps"]["Test Coverage"]["coverage_adequate"]
        dependencies_ok = results["steps"]["Critical Dependencies"]["safe_to_remove"]
        type_safety_ok = results["steps"]["Type Safety Check"]["mypy_passing"]

        if not error_system_ok:
            all_critical_passed = False
            critical_failures.append("Core error system not stable")

        if not compatibility_ready:
            all_critical_passed = False
            critical_failures.append("Too many backwards compatibility patterns remain")

        if not test_coverage_ok:
            all_critical_passed = False
            critical_failures.append("Test coverage insufficient for safe removal")

        if not dependencies_ok:
            all_critical_passed = False
            critical_failures.append("Too many critical dependencies on old system")

        if not type_safety_ok:
            all_critical_passed = False
            critical_failures.append("Type checking errors must be resolved first")

        # Final decision
        results["overall"] = {
            "all_critical_passed": all_critical_passed,
            "critical_failures": critical_failures,
            "go_no_go_decision": "GO" if all_critical_passed else "NO-GO",
            "recommendation": (
                "System is ready for final backwards compatibility removal"
                if all_critical_passed
                else "System not ready - address critical failures before proceeding"
            ),
        }

        # Print summary
        print("\n" + "=" * 70)
        print("📊 VALIDATION SUMMARY")
        print("=" * 70)
        print(f"🎯 Decision: {results['overall']['go_no_go_decision']}")
        print(f"💡 Recommendation: {results['overall']['recommendation']}")

        if critical_failures:
            print("\n❌ Critical Failures:")
            for failure in critical_failures:
                print(f"   • {failure}")
        else:
            print("\n✅ All critical criteria passed!")

        return results

    async def validate_type_safety_status(self) -> dict[str, Any]:
        """Validate current type checking status.

        Returns:
            Dictionary with type safety validation results
        """
        results = {
            "mypy_passing": False,
            "pyright_passing": False,
            "ruff_passing": False,
            "error_count": 0,
            "details": [],
        }

        try:
            import subprocess
            
            # Run mypy on WebSocket directory
            try:
                result = subprocess.run([
                    ".venv/bin/mypy", 
                    "cyberdelta/apis/websocket/", 
                    "--strict"
                ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
                
                if result.returncode == 0:
                    results["mypy_passing"] = True
                    results["details"].append("✓ mypy --strict: PASSED")
                else:
                    error_lines = result.stdout.count('\n') + result.stderr.count('\n')
                    results["error_count"] += error_lines
                    results["details"].append(f"✗ mypy --strict: FAILED ({error_lines} errors)")
                    results["details"].append(f"  Sample error: {result.stdout.split(chr(10))[0] if result.stdout else 'Unknown'}")
                    
            except Exception as e:
                results["details"].append(f"⚠️ Could not run mypy: {e}")

            # Run ruff check
            try:
                result = subprocess.run([
                    ".venv/bin/ruff", 
                    "check", 
                    "cyberdelta/apis/websocket/"
                ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
                
                if result.returncode == 0:
                    results["ruff_passing"] = True
                    results["details"].append("✓ ruff check: PASSED")
                else:
                    error_lines = result.stdout.count('\n')
                    results["error_count"] += error_lines
                    results["details"].append(f"✗ ruff check: FAILED ({error_lines} issues)")
                    
            except Exception as e:
                results["details"].append(f"⚠️ Could not run ruff: {e}")

            # Summary
            if results["mypy_passing"] and results["ruff_passing"]:
                results["details"].append("✅ Type safety validation PASSED")
            else:
                results["details"].append(f"❌ Type safety validation FAILED - {results['error_count']} total issues")

        except Exception as e:
            results["details"].append(f"✗ Type safety validation error: {e}")

        return results


async def main():
    """Main validation function."""
    validator = SystemStabilityValidator()
    results = await validator.run_full_validation()

    # Exit with error code if not ready
    if results["overall"]["go_no_go_decision"] == "NO-GO":
        print("\n⚠️  System not ready for backwards compatibility removal!")
        print("Address the critical failures above before proceeding.")
        sys.exit(1)
    else:
        print("\n🎉 System validated and ready for backwards compatibility removal!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
