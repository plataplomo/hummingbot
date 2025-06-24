---
description: Mandates execution of Python tools and scripts using explicit paths within the virtual environment.
globs: ["*"] # Applies globally to any task involving tool/script execution
alwaysApply: true # This is fundamental for environment consistency
---
# Virtual Environment Execution (`.venv`) - Direct Path Mandate

1.  **Core Principle:**
    *   All Python-related executables (tools like `mypy`, `ruff`, `pip`, `pytest`, or project scripts) **MUST** be invoked using their **explicit, direct path** located within the project's virtual environment `bin` directory (e.g., `.venv/bin/`).
    *   This ensures the correct interpreter and installed package versions from the isolated `.venv` are used, preventing conflicts and ensuring reproducibility.

2.  **Correct Usage Format:**
    *   **MANDATORY Format:** `.venv/bin/<tool_executable_name> [arguments...]`
    *   **Examples:**
        *   Mypy: `.venv/bin/mypy src/ tests/`
        *   Ruff Check: `.venv/bin/ruff check .`
        *   Ruff Format: `.venv/bin/ruff format .`
        *   Pip Install: `.venv/bin/pip install -r requirements.txt`
        *   Pytest: `.venv/bin/pytest tests/`
        *   Running Main Script: `.venv/bin/python main.py [args...]` *(Note: Using `.venv/bin/python` directly to run a `.py` file is acceptable and often necessary)*

3.  **Forbidden Practices:**
    *   **DO NOT** use `python -m <module>` via the venv path (e.g., `.venv/bin/python -m pytest`) due to observed unreliability in specific execution environments (like Cursor). Stick to the direct executable path (`.venv/bin/pytest`).
    *   **DO NOT** rely on shell activation (`source .venv/bin/activate` or equivalent) for programmatic execution by the AI or in automated scripts. The AI must always use the explicit `.venv/bin/...` path.
    *   **NEVER** use system-wide installations (e.g., `/usr/bin/python`, `/usr/local/bin/pip`) of Python or its tools for this project.
    *   **NEVER** use the `--break-system-packages` flag or similar mechanisms with `pip`. Resolve all dependency issues *within* the `.venv`.

4.  **Troubleshooting (AI Context):**
    *   If execution fails, first verify the existence and executability of the tool at the specified path (e.g., `ls -l .venv/bin/ruff`).
    *   Check that the tool was correctly installed within the `.venv` (e.g., `.venv/bin/pip list | grep ruff`).
    *   Ensure the virtual environment itself is not corrupted. Suggest recreating it (`rm -rf .venv && python -m venv .venv && .venv/bin/pip install -r requirements.txt`) as a potential fix if persistent, unexplained errors occur.

5.  **Rationale:**
    *   Guarantees consistent use of the isolated project environment and its specific dependencies.
    *   Avoids conflicts with system-installed packages or other Python environments.
    *   Enhances reliability and reproducibility of development, testing, and build tasks.
    *   Works around observed instability with the `python -m` invocation method in certain environments.
