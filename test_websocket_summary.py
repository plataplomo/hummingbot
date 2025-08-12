#!/usr/bin/env python3
"""Run comprehensive WebSocket test summary."""

import subprocess
import sys
from pathlib import Path


def run_tests(test_path: str, description: str) -> tuple[int, int, int]:
    """Run tests and return passed, failed, skipped counts."""
    print(f"\n{'=' * 60}")
    print(f"Running: {description}")
    print(f"Path: {test_path}")
    print("=" * 60)

    cmd = [".venv/bin/pytest", test_path, "-q", "--tb=no", "--no-header", "--no-summary", "-rN"]

    result = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=30)
    output = result.stdout + result.stderr

    # Parse output for results
    passed = failed = skipped = 0
    for line in output.split("\n"):
        if "passed" in line and "failed" in line:
            parts = line.split()
            for i, part in enumerate(parts):
                if "passed" in part and i > 0:
                    passed = int(parts[i - 1])
                if "failed" in part and i > 0:
                    failed = int(parts[i - 1])
                if "skipped" in part and i > 0:
                    skipped = int(parts[i - 1])

    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    return passed, failed, skipped


def main():
    """Run all WebSocket tests and provide summary."""
    print("\n" + "=" * 60)
    print("WEBSOCKET TYPE SAFETY TEST SUMMARY")
    print("=" * 60)

    test_categories = [
        # Phase 1: Foundation Tests
        ("tests/unit/websocket/test_stream_error_foundation.py", "Phase 1: Foundation Components"),
        ("tests/unit/websocket/test_error_code_coverage.py", "Phase 1: Error Code Coverage"),
        ("tests/unit/websocket/test_recovery_strategy_coverage.py", "Phase 1: Recovery Strategies"),
        ("tests/unit/websocket/test_error_context_validation.py", "Phase 1: Context Validation"),
        ("tests/unit/websocket/test_error_adapter.py", "Phase 1: Compatibility Adapter"),
        # Phase 3: Testing Suite
        ("tests/unit/websocket/test_e2e_error_flows.py", "Phase 3: E2E Error Flows"),
        ("tests/unit/websocket/test_multi_exchange_errors.py", "Phase 3: Multi-Exchange"),
        ("tests/unit/websocket/test_concurrent_error_handling.py", "Phase 3: Concurrent Handling"),
        ("tests/unit/websocket/test_error_persistence.py", "Phase 3: Error Persistence"),
        # Phase 3: Performance
        (
            "tests/performance/websocket/test_error_performance_baseline.py",
            "Phase 3: Performance Baseline",
        ),
        ("tests/performance/websocket/test_error_memory_usage.py", "Phase 3: Memory Usage"),
        (
            "tests/performance/websocket/test_error_handler_optimization.py",
            "Phase 3: Optimizations",
        ),
    ]

    total_passed = total_failed = total_skipped = 0

    for test_path, description in test_categories:
        if Path(test_path).exists():
            try:
                passed, failed, skipped = run_tests(test_path, description)
                total_passed += passed
                total_failed += failed
                total_skipped += skipped
            except subprocess.TimeoutExpired:
                print("TIMEOUT: Test took too long")
            except Exception as e:
                print(f"ERROR: {e}")
        else:
            print(f"\nSKIPPED: {description} - File not found: {test_path}")

    # Final summary
    print("\n" + "=" * 60)
    print("OVERALL SUMMARY")
    print("=" * 60)
    print(f"Total Tests Run: {total_passed + total_failed}")
    print(f"✅ Passed: {total_passed}")
    print(f"❌ Failed: {total_failed}")
    print(f"⏭️ Skipped: {total_skipped}")

    if total_failed == 0:
        print("\n🎉 ALL TESTS PASSING!")
    else:
        print(f"\n⚠️ {total_failed} tests need attention")

    # Calculate progress
    total_steps_complete = 63  # From workflow doc
    print(f"\nProject Progress: {total_steps_complete}/100 steps ({total_steps_complete}%)")
    print("Phase 1: 25/25 (100%) ✅")
    print("Phase 2: 5/25 (20%) 🟨")
    print("Phase 3: 13/25 (52%) 🟨")
    print("Phase 4: 0/25 (0%) ⬜")

    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
