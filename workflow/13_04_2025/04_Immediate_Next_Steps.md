# CyberDeltaEngine - Immediate Next Steps (as of 2025-04-13 ~21:13 UTC-5)

1.  **Validate `synchronized_order_submission.py`:**
    *   Run `ruff check --fix cyberdelta/core/execution/synchronized_order_submission.py` one more time to ensure all auto-fixable issues are resolved.
    *   Run `mypy cyberdelta/core/execution/synchronized_order_submission.py` to confirm the type errors within this specific file are resolved (ignoring known base class/cache issues if they reappear).

2.  **Address Remaining `mypy` Errors in `cyberdelta/core/`:**
    *   Run `mypy cyberdelta/core/` to get the full list of current errors in the core directory.
    *   Systematically analyze and fix the reported errors in the remaining core files (e.g., `data_handler.py`, `risk_manager.py`, `signal_generator.py`, `balance_monitor.py`, `engine.py`, `backtesting.py`, `results.py`, `strategy_manager.py`), prioritizing `arg-type`, `operator`, `attr-defined`, and `Decimal`-related issues. Adhere strictly to the Python File Validation rule (check/format after each file modification).

3.  **Analyze and Fix Test Errors:**
    *   Once `cyberdelta/core/` is clean according to `mypy` (or known issues are documented), run `mypy tests/core/ tests/unit/` and `ruff check tests/core/ tests/unit/`.
    *   Address the errors reported in the test files (`tests/core/` and `tests/unit/`), paying close attention to:
        *   Updating test fixtures and assertions to use the refactored `Order` model (`id`, `type`, `time`, `avg_fill_price`).
        *   Ensuring all financial literals in tests use `Decimal('...')`.
        *   Fixing `None` checks before operations on `Decimal | None` types.
        *   Adding missing type annotations (`ANN*` errors).