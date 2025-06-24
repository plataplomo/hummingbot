---
description: Mandates immediate static analysis checks after modifying Python files.
globs: ["*.py", "*.pyi"] # Apply when Python/stub files are involved
alwaysApply: true # Check should always happen after relevant modifications
---
# Python File Validation: Immediate Checks Required

1.  **Check Timing & Requirement:**
    *   **Immediately after** completing any significant modification or generation of code within a `.py` or `.pyi` file (or a small, related set of such files), you **MUST** perform static analysis checks before proceeding to the next task, file, or logical step.
    *   Do **NOT** wait to batch checks after multiple files are edited. Catch errors early and often.

2.  **Tools & Scope:**
    *   Run **BOTH** `ruff check <modified_file(s)>` AND `mypy <modified_file(s)>` using the explicit virtual environment path (e.g., `.venv/bin/python -m ruff check ...`).
    *   The checks **MUST** target the specific file(s) that were just modified to ensure focused validation. Optionally, run checks on the entire relevant module or package if changes might have broader implications, but *always* check the directly modified files first.

3.  **Configuration:**
    *   Adhere strictly to the configurations defined in the project's `pyproject.toml` (for Ruff) and `pyproject.toml` (for Mypy).
    *   **DO NOT modify these configuration files** under any circumstances unless explicitly instructed by the user.

4.  **Handling Errors:**
    *   If Ruff or Mypy reports **any errors or warnings** (subject to the configured severity levels):
        *   **Attempt to fix** the reported issues directly in the code, prioritizing fixes that enhance correctness and adhere to type safety principles (prefer specific types over `Any`, avoid unnecessary `type: ignore`).
        *   If a fix requires a logical change or is potentially unsafe, **present the issue and the proposed fix** for user confirmation before applying.
        *   If an issue cannot be resolved without altering the tool configurations (which is forbidden), **clearly document the specific error, the file/line, and the reason it cannot be fixed** in the response and workflow documentation. **DO NOT** proceed with logically dependent tasks until the user addresses or acknowledges the limitation.

5.  **Workflow Enforcement & Reporting:**
    *   Before proceeding to the next step after modifying Python code, you **MUST explicitly report** the outcome of the Ruff and Mypy checks for the modified file(s).
    *   Example Reporting:
        *   "Ran `ruff check cyberdelta/core/data_handler.py` and `mypy cyberdelta/core/data_handler.py`. No issues found."
        *   "Ran `ruff check cyberdelta/apis/backpack.py` and `mypy cyberdelta/apis/backpack.py`. Ruff reported 3 minor style issues (fixed). Mypy reported 1 type error on line 152 (fixed by adding type hint `Optional[str]`). No further issues."
        *   "Ran `ruff check cyberdelta/risk/risk_manager.py` and `mypy cyberdelta/risk/risk_manager.py`. Mypy reported error `[misc]` on line 88: Cannot infer type of 'x'. Cannot fix without changing logic or using `Any`. Documenting issue and awaiting guidance."
    *   Only proceed once fixes are applied (or issues documented/acknowledged by user).
