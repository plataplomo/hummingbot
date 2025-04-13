# Plan for Next 3 Days (Aug 10 - Aug 12, 2025)

This plan focuses on resolving critical blockers identified on Aug 9 and stabilizing the codebase for Prototype 0.0.1.

## Day 5 (Aug 10): Integration Fixes & Risk Refinement
*   **[BLOCKER] Priority 1: Resolve Safety System Integration Test Runtime Errors (`test_safety_systems.py`)**
    *   **Task 5.1:** Debug & Fix `SignalGenerator` fixture `AttributeError` (config `symbols` format - list vs dict).
    *   **Task 5.2:** Debug & Fix `RiskManager` `decimal.InvalidOperation` (handling `None` `expected_profit` in `basic_opportunity` fixture).
    *   **Task 5.3:** Debug & Fix `PositionReconciliationSystem` `AttributeError` (incorrect `get_api_client` method call).
    *   **Goal:** Get `test_safety_systems.py` to run without runtime errors (passing/failing status TBD).
*   **Task 5.4:** Implement & Test **Circuit Breaker** integration fully (EH respecting CB state, state transitions).
*   **Task 5.5:** **Refine Risk Manager** implementation (review/test hard limits, margin/liquidation checks).
*   **Task 5.6:** Implement initial **Failure Injection Tests** for Circuit Breakers (e.g., simulate API errors triggering CB).
*   **End-of-Day Goal:** Safety system integration tests unblocked. CB integration tested. Risk Manager refinement reviewed/tested. Initial CB failure tests implemented.

## Day 6 (Aug 11): Stabilize Safety Systems & Testing
*   **Focus:** Get Safety System integration tests passing and expand failure coverage.
*   **Task 6.1:** Rerun `test_safety_systems.py`. Debug any *test logic* failures now that runtime errors are fixed.
*   **Task 6.2:** Implement & Test **Funding Rate Validator** integration fully.
*   **Task 6.3:** Implement & Test **Position Reconciliation** integration fully (including discrepancy scenarios).
*   **Task 6.4:** Expand **Failure Scenario Tests** to cover:
    *   Partial fills during execution.
    *   Reconciliation detecting discrepancies (and potential auto-correct if enabled).
    *   Specific safety system triggers (Validator flags, Recon flags, CB types).
*   **Task 6.5:** Run the *entire* test suite (Unit + Integration + Failure) and fix any regressions or newly identified issues.
*   **End-of-Day Goal:** All integration tests (core workflow + safety systems) passing. Expanded failure scenario coverage. Full test suite stable.
*   **Contingency:** If safety system tests prove difficult, focus shifts to getting *at least* CB and Reconciler integration stable.

## Day 7 (Aug 12): Final Polish, Documentation & Review
*   **Focus:** Code cleanup, documentation updates, final verification.
*   **Task 7.1:** Perform code review of recent fixes (integration blockers, safety systems, risk manager).
*   **Task 7.2:** Run linters (`ruff check . --fix`, `ruff format .`) and type checkers (`mypy src/ tests/`) and address any remaining issues.
*   **Task 7.3:** Update core documentation (`README.md`, key module docstrings) to reflect the finalized state of v0.0.1 components and safety features.
*   **Task 7.4:** Update workflow documentation (`implementation_status.md`, `safety_systems_summary.md`, etc.) with final status.
*   **Task 7.5:** Final run of the complete test suite to ensure stability.
*   **Task 7.6 (Optional):** Consider tagging a pre-release candidate commit (e.g., `v0.0.1-rc1`).
*   **End-of-Day Goal:** Codebase is clean, documented, fully tested, and stable. Ready for potential v0.0.1 tagging. 