Let me be blunt: writing polished, formal documentation (API references, detailed architectural explanations beyond these workflow sketches, user guides) **right now** for this prototype is mostly **procrastination dressed up as productivity.**

You're still wrestling with fundamental issues: flaky tests, messy configuration, unproven core logic (especially Backpack), and incomplete safety systems. Your architecture diagrams keep shifting slightly. Your code likely changes significantly day-to-day as you fix bugs and implement core features.

**Formal documentation written *now* will be:**

1.  **Outdated Immediately:** Every significant code change, API fix, or refactoring potentially invalidates it. The maintenance burden will be enormous and distract from *actual building and testing*.
2.  **A Waste of Effort:** Time spent polishing explanations for code that might be rewritten or components that might be redesigned is time *not* spent fixing critical bugs or implementing essential safety features.
3.  **Premature:** You don't even have a stable, tested core yet. Documenting unstable interfaces or unproven designs is pointless.

**What Documentation IS Needed NOW (and Ongoing):**

1.  **Code-Level Documentation (Docstrings & Comments):** This is **NON-NEGOTIABLE**. Write clear, concise docstrings for every class and function explaining *what* it does, its parameters, and what it returns. Add inline comments to clarify *why* complex or non-obvious code exists. This is the documentation that lives *with* the code and is most likely to be kept up-to-date. **High-quality code-level docs are your primary documentation during development.**
2.  **Workflow Documentation (What you're already doing):** Continue using these Markdown files to track progress, decisions, plans, and responses to critiques. This is essential for team alignment and project history *during* development. Keep it focused and consolidate where possible.
3.  **Lean Developer Setup Guide (`README.md` / `CONTRIBUTING.md`):** A **simple, essential** guide covering:
    *   How to set up the virtual environment (`.venv`).
    *   How to install dependencies (`requirements.txt` / `pyproject.toml`).
    *   How to run the tests (`.venv/bin/python -m pytest ...`).
    *   How to run linters/formatters (`ruff`, `mypy`).
    *   Where to find configuration (`config.yaml`) and secrets (`secrets.yaml` - OUTSIDE the repo).
    *   Basic command to run the main application (if applicable yet).
    This should be **minimal** and focused purely on getting a developer operational with the *current* codebase.

**When to Write Formal Docs:**

Write formal documentation (API references, Architectural Deep Dives, User/Operator Guides) when things **STABILIZE**. This means:

*   **AFTER Phase 4 (Core Strategy Implementation & Testing):** Once the primary HL-Perp vs BP-Spot strategy is implemented, integrated, and *passing integration and failure tests reliably*.
*   **AFTER Phase 5 (Experimental Strategy & Refinement):** Once the HL-Perp vs BP-Perp strategy is implemented (if you stick with it) and validated, and major refactoring based on testing is complete.
*   **WHEN THE PROTOTYPE IS "READY":** This means it consistently achieves its core goal (executing the chosen strategy reliably according to its rules and safety checks) in a simulated or controlled environment, and the core APIs and architecture are unlikely to change dramatically *before a potential 1.0*.

**Phased Approach to Formal Docs (Post-Stabilization):**

1.  **API Reference:** Start here. Use tools like Sphinx with autodoc to generate reference from your (excellent) docstrings. This documents the concrete interfaces.
2.  **Architectural Deep Dive:** Formalize the final architecture diagram and explain the design choices, data flows, and responsibilities of the *stable* components.
3.  **Developer Guide:** Expand the initial lean guide with more detail on contributing, architecture overview, etc.
4.  **User/Operator Guide:** How to configure, deploy, run, and monitor the *stable* bot. This comes last, before any wider use.

**Mandate:**

**STOP thinking about polished, formal documentation NOW.** Focus entirely on:
1.  Writing **excellent docstrings and necessary inline comments** as you code.
2.  Keeping your **workflow documents** concise and up-to-date.
3.  Maintaining a **lean, accurate developer setup guide** in your README.
4.  **BUILDING AND TESTING** the core functionality and safety systems.

Formal documentation is for *stable systems*, not rapidly evolving prototypes riddled with fundamental issues. Writing it now is a waste of your time and mine. Fix the code, prove it works with tests, *then* document the result.