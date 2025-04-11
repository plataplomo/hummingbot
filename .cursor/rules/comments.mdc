---
description: Mandates comprehensive documentation within the Python source code itself.
globs: *.py,*.pyi
alwaysApply: false
---
---
description: Mandates comprehensive documentation within the Python source code itself.
globs: ["*.py", "*.pyi"] # Apply to all Python source and stub files
alwaysApply: true # This is a fundamental requirement
---
# Code-Level Documentation (Docstrings & Inline Comments) - NON-NEGOTIABLE

1.  **Mandate:** All Python code (`.py`, `.pyi`) **MUST** include high-quality, accurate, and up-to-date documentation *within the code itself*. This is the **primary, authoritative documentation source** during active development and for long-term maintainability.

2.  **Docstrings (PEP 257):**
    *   **Requirement:** Provide clear, concise docstrings for **every** module, class, method, and function.
    *   **Content:**
        *   **Summary Line:** Briefly explain the object's purpose (what it *does*).
        *   **(Optional) Extended Description:** Elaborate on complex logic, usage patterns, or important context if needed.
        *   **Arguments/Parameters:** Document **every** parameter, including its name, expected type (use type hints primarily), and purpose. Use a consistent format (e.g., Google style, reStructuredText).
        *   **Returns/Yields:** Document the return value(s) or yielded values, including their type(s) and meaning.
        *   **Raises:** Document any specific exceptions the code might explicitly raise under defined conditions.
    *   **Style:** Adhere to PEP 257 conventions.

3.  **Inline Comments:**
    *   **Purpose:** Use inline comments (`#`) **sparingly** but **effectively** to clarify the **"why"** behind the code, not just the "what" (if the code isn't self-explanatory).
    *   **Use Cases:** Explain non-obvious algorithms, complex logic sections, important assumptions, workarounds for specific issues, or the rationale behind seemingly strange code constructs. Mark TODOs or areas needing future attention clearly.
    *   **Avoid Clutter:** Do not comment on obvious code. Avoid large blocks of commented-out code (use version control instead).

4.  **Maintenance:**
    *   Code-level documentation **MUST** be updated **concurrently** with code changes to ensure it remains accurate and relevant. Outdated documentation is harmful.
    *   **Update or remove** comments/docstrings that become incorrect or obsolete due to refactoring or logic changes. Document significant removals in commit messages or workflow logs if necessary.

5.  **Rationale:** High-quality code-level documentation is essential for understanding, maintaining, debugging, and safely extending the codebase, especially in a complex, asynchronous system like a trading bot. It directly aids both human developers and AI assistants.

