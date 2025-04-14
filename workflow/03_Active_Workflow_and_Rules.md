# CyberDeltaEngine - Active Workflow and Rules (as of 2025-04-13 ~21:13 UTC-5)

## Active Workflow Phase

We are currently in the **Foundational Stability & Testing** phase. The primary objective is to refactor core components (`cyberdelta/core/` and related tests) to ensure:
*   Strict type safety (clean `mypy` analysis).
*   Adherence to coding standards (`ruff` checks and formatting).
*   Correct and consistent use of `Decimal` for all financial calculations (Rule: `decimal.md`).
*   Robustness against `None` values and potential errors.

## Key Guiding Rules (Project-Specific)

The following rules are actively being enforced:

*   **Python File Validation:** Run `ruff check` and `mypy` immediately after modifying any `.py`/`.pyi` file. Report results and fix errors before proceeding. (`.roo/rules-code/python_file_validation.md`)
*   **Virtual Environment Execution:** All Python tools (`ruff`, `mypy`, `python`) MUST be executed using the explicit `.venv/bin/` path. (`.roo/rules-code/venv_execution.md`)
*   **Tool Configuration Integrity:** DO NOT modify `pyproject.toml` or `mypy.ini` configurations unless explicitly instructed by the user. Report blockers requiring config changes. (`.roo/rules-code/configuration.md`)
*   **Mandatory `Decimal` Usage:** Use `Decimal` (initialized from strings) exclusively for all financial quantities. No `float` for finance. Check for `None` before operations. (`.roo/rules-code/decimal.md`)
*   **Code-Level Documentation:** Maintain comprehensive docstrings (modules, classes, functions, methods) and necessary inline comments. (`.roo/rules-code/comments.md`)
*   **Code Formatting and Style:** Adhere strictly to `ruff format` and configured `ruff check` rules. Use standard Python naming conventions. (`.roo/rules-code/codeformatting.md`)
*   **Python Standards:** Target Python 3.13+. Use Ruff and Mypy as primary static analysis tools. (`.roo/rules-code/python_coding.md`)
*   **Security:** Adhere to secure coding practices (input validation, secrets management, etc.). (`.roo/rules-code/security.md`)
*   **Focused Workflow Documentation:** Use `current_workflow/` for significant decisions/context (like this save point). (`.roo/rules-code/workflow.md`)

## AI Persona & Communication

*   **Persona:** Angel - Experienced, meticulous, safety-conscious Senior Software Engineer/Architect.
*   **Style:** Professional, collaborative, constructive, precise English. Prioritize correctness, robustness, security, and testing. Challenge unsafe actions. (`.roo/rules-code/rules.md`)