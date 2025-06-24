---
description: Enforces consistent code style, formatting, naming, and structural conventions.
globs: ["*.py", "*.pyi", "*.md"] # Apply primarily to Python, but principles extend to Markdown
alwaysApply: true # These are foundational style guidelines
---
# Code Formatting and Style

1.  **Core Principles:**
    *   **Readability:** Prioritize writing code that is easy for humans (including future self and AI assistants) to understand and maintain.
    *   **Simplicity:** Prefer clear, direct solutions over overly complex or "clever" ones, unless complexity is justified by significant performance or functional requirements (and is well-documented).
    *   **Consistency:** Adhere strictly to the project's established formatting and naming conventions across the entire codebase.

2.  **Formatting (Handled by Ruff):**
    *   **Tool:** **Ruff** (`ruff format`) is the **sole authority** for code formatting.
    *   **Configuration:** Formatting rules are defined in `pyproject.toml` (`[tool.ruff.format]`). **DO NOT** manually deviate from the style enforced by Ruff after running `ruff format`.
    *   **Workflow:** Run `ruff format <path>` frequently (as mandated by the "Python File Validation" rule) to ensure consistent formatting.

3.  **Linting & Style Checks (Handled by Ruff):**
    *   **Tool:** **Ruff** (`ruff check`) is the primary tool for identifying style violations, potential bugs, and code smells.
    *   **Configuration:** Linting rules are defined in `pyproject.toml` (`[tool.ruff.lint]`). Adhere to the selected rules.
    *   **Workflow:** Run `ruff check --fix <path>` frequently (as mandated by the "Python File Validation" rule) to identify and automatically fix applicable issues. Manually address remaining reported issues.

4.  **Naming Conventions:**
    *   Use clear, descriptive, and unambiguous names for variables, functions, classes, methods, modules, and files.
    *   Follow standard Python naming conventions (PEP 8):
        *   `snake_case` for variables, functions, methods, modules, packages.
        *   `PascalCase` (or `CapWords`) for classes.
        *   `UPPER_SNAKE_CASE` for constants.
        *   Use leading underscore (`_`) for internal/protected attributes/methods where appropriate (though Python doesn't enforce privacy). Avoid double leading underscores (`__`) unless necessary for name mangling.
    *   Avoid single-letter variable names except in very short, localized contexts (e.g., loop counters like `i`, simple math `x, y`).

5.  **Structure & Modularity:**
    *   **Single Responsibility:** Each function, method, and class should ideally have one well-defined responsibility.
    *   **File Focus:** Keep files focused on a cohesive set of related functionality (e.g., a specific class, a group of related utility functions).
    *   **File Length Guideline:** *Aim* to keep Python files concise (e.g., ideally under 500 lines, including docstrings/comments). If a file significantly exceeds this, consider if it can be logically split into smaller, more focused modules or classes. This is a guideline, not a hard rule – prioritize logical coherence over arbitrary line counts.
    *   **Imports:** Use absolute imports where possible. Keep imports organized (stdlib -> third-party -> first-party), managed automatically by `ruff format` (via its `isort` integration).

6.  **Language & Clarity (Code & Docs):**
    *   Use clear, precise English in code (names, comments, docstrings) and documentation.
    *   Prefer straightforward language and avoid unnecessary jargon.
    *   Write short, direct sentences in comments and documentation.
