# Phase 4: Foundational Stability & Testing - Implementation Plan (Revised Aug 6, 2025)

**Goal:** Address critical feedback by focusing on foundational stability, configuration cleanup, comprehensive testing (unit, integration, failure), and completing/testing safety systems. Build a reliable Prototype 0.0.1 core.

**Timeline:** August 6 - August 10 (5 days)

**Mandates (from Critic):**
1.  Fix `config.yaml` (Clean, Lean, Consolidated).
2.  Fix all remaining Unit Tests (~20).
3.  Build Integration Test Framework & achieve >70% coverage.
4.  Build Failure Scenario Tests (basic coverage).
5.  Finalize Implementation & Testing of Safety Systems (Validation, Recon, CBs).
6.  Simplify Risk Manager (Hard Limits only) & Test.

## Detailed Daily Plan

**Day 1: Wednesday, August 6**
- **Focus:** Configuration Cleanup & Unit Test Triage
- **Tasks:**
    - [ ] **Task 1.1:** Refactor `config.yaml` - Remove duplicates, unused sections (MA/RSI/BB, backtesting), consolidate `risk`/`trading` params. Ensure only v0.0.1 relevant params remain.
    - [ ] **Task 1.2:** Verify `secrets.yaml` loading mechanism is robust.
    - [ ] **Task 1.3:** Identify all failing/missing unit tests across RM, EH, DH, Strategy, Safety Systems (~20 tests).
    - [ ] **Task 1.4:** Begin fixing high-priority unit tests (e.g., RM config/param validation, DH connection).
    - [ ] **Task 1.5:** Setup basic pre-commit hooks (ruff check/format, mypy) or initial manual checks.
    - [ ] **Task 1.6:** Pin project dependencies (`requirements.txt` or `pyproject.toml`).
- **Goal:** Clean `config.yaml` committed. List of failing unit tests created. Start fixing tests. Dependencies pinned.

**Day 2: Thursday, August 7**
- **Focus:** Complete Unit Tests & Start Integration Framework
- **Tasks:**
    - [ ] **Task 2.1:** Continue fixing remaining unit tests.
    - [ ] **Task 2.2:** Complete implementation of Safety System unit tests.
    - [ ] **Task 2.3:** Implement simplified Risk Manager (Hard limits only, remove Kelly/VaR). Add unit tests for hard limits & margin checks.
    - [ ] **Task 2.4:** Design and begin implementing `MockExchange` framework (basic API simulation, error injection capability).
- **Goal:** All unit tests (including simplified RM and Safety Systems) passing (100%). Basic MockExchange structure in place.

**Day 3: Friday, August 8**
- **Focus:** Integration Test Implementation (Core Flow)
- **Tasks:**
    - [ ] **Task 3.1:** Implement core integration test fixtures (using MockExchange).
    - [ ] **Task 3.2:** Implement integration tests for the main data flow: DataHandler -> SignalGenerator -> RiskManager (simplified) -> ExecutionHandler -> PortfolioTracker.
    - [ ] **Task 3.3:** Implement basic integration tests for safety systems (e.g., CB blocking EH, Reconciler fetching from MockExchange/PT).
- **Goal:** Core workflow integration tests implemented and passing. Basic safety system integration tests passing.

**Day 4: Saturday, August 9**
- **Focus:** Failure Scenario Testing & Safety System Finalization
- **Tasks:**
    - [ ] **Task 4.1:** Finalize implementation of all Safety System logic (Validation, Reconciliation, CB states).
    - [ ] **Task 4.2:** Develop failure injection helpers/framework.
    - [ ] **Task 4.3:** Implement failure scenario tests: Simulate API errors (during order placement, data fetch), network drops, timeouts.
    - [ ] **Task 4.4:** Implement failure scenario tests: Simulate reconciliation discrepancies, CB trigger conditions (e.g., inject high volatility).
    - [ ] **Task 4.5:** Run integration tests and measure coverage.
- **Goal:** Safety system implementation complete. Basic failure scenario tests implemented and passing. Integration coverage >50%.

**Day 5: Sunday, August 10**
- **Focus:** Test Refinement, Coverage Increase, Documentation
- **Tasks:**
    - [ ] **Task 5.1:** Refine existing integration and failure tests based on results.
    - [ ] **Task 5.2:** Add more integration/failure tests to reach >70% coverage target.
    - [ ] **Task 5.3:** Verify all Safety Systems are fully tested (unit, integration, failure).
    - [ ] **Task 5.4:** Update relevant documentation (README, architecture diagrams, component docs) to reflect final state of v0.0.1 (simplified RM, tested safety systems).
    - [ ] **Task 5.5:** Consolidate/cleanup workflow documentation.
- **Goal:** Stable Prototype 0.0.1 - Clean config, 100% unit tests passing, >70% integration coverage, basic failure tests passing, safety systems implemented and tested, documentation updated.

## Deferred Tasks (Previously in Phase 4/5)
- Implementation of HL Perp vs BP Perp strategy logic.
- Implementation of Enhanced Position Sizing (Kelly, VaR).
- Implementation of Multi-Tier Signal Verification.
- Implementation of full Synchronized/Atomic Order Execution.
- Performance Optimizations.
- CI/CD Pipeline Setup (moved to post-stabilization).

## Contingency
- If testing reveals major flaws requiring significant redesign, pause and reassess. Priority is stability, not meeting the deadline with a broken system.
- If integration/failure test coverage targets are hard to meet, focus on covering the most critical paths and failure modes first. 