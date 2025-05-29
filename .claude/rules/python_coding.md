---
description: Defines Python language version, core static analysis tools, and development workflow requirements.
globs: ["*.py", "*.pyi"] # Apply when Python/stub files are involved
alwaysApply: true # These standards should always be considered for Python code
---
# Python Standards and Tooling

1.  **Language Version:**
    *   All Python code **MUST** be written for and compatible with **Python 3.13** or later versions specified by the project requirements. Use features and syntax appropriate for this version.

2.  **Primary Static Analysis & Formatting Tool: Ruff:**
    *   **Ruff** is the primary tool for linting, formatting, and import sorting.
    *   **Configuration:** Adhere strictly to the rules and formatting style defined in the project's `pyproject.toml` (`[tool.ruff]` and `[tool.ruff.lint]`, `[tool.ruff.format]`, `[tool.ruff.isort]` sections).
    *   **Usage:** Run `ruff check --fix <path>` and `ruff format <path>` frequently (as mandated by the "Python File Validation" rule).
    *   **(Deprecated):** Do **NOT** use `Black` or `isort` directly; their functionality is handled by Ruff's configuration.

3.  **Static Type Checking: Mypy:**
    *   **Mypy** is the required tool for static type checking.
    *   **Configuration:** Adhere strictly to the strictness settings defined in the project's `pyproject.toml` (`[tool.mypy]` section).
    *   **Goal:** Aim for comprehensive type safety across the codebase. Prioritize specific types over `Any`. Use `# type: ignore` sparingly and only with clear justification (documented in code comments).
    *   **Usage:** Run `mypy <path>` frequently (as mandated by the "Python File Validation" rule).

4.  **Development Workflow Requirement:**
    *   Before considering any code modification "complete" (e.g., ready for commit, review, or integration), you **MUST** ensure that running `ruff check <modified_files>`, `ruff format <modified_files>`, and `mypy <modified_files>` reports **ZERO** errors according to the project's configuration for those modified files (or that any remaining errors are documented exceptions as per the "Python File Validation" rule).

    