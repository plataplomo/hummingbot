---
id: RULE-CONFIG-INTEGRITY-V3
title: Strict Tool Configuration Integrity & Compliant Fix Protocol
description: Mandates strict preservation of static analysis configurations and defines a protocol that requires exhaustive attempts at compliant source code fixes before halting on configuration-related blockers.
globs: ["pyproject.toml", "mypy.ini", "*.py", "*.pyi"] # Files governed by this rule.
alwaysApply: true
severity: critical # Violation of this rule is a critical failure.
---

# Strict Tool Configuration Integrity & Compliant Fix Protocol

1.  **Configuration Immutability (Strict):**
    *   You are **STRICTLY PROHIBITED** from modifying the project's static analysis configuration files (`pyproject.toml` for `[tool.ruff.*]`, `[tool.mypy]`, `[tool.pyright]`; `mypy.ini`) or suggesting modifications unless explicitly instructed by the User with User-provided justification.
    *   Using command-line flags or other mechanisms to effectively bypass the project's established configuration for Ruff, Mypy, or Pyright is also forbidden.

2.  **Protocol for Configuration-Related Blockers (Iterative Fix Attempt Required):**
    *   If a static analysis error (from Ruff, Mypy, or Pyright, run according to `RULE-STATIC-ANALYSIS-V3`) is reported:
        *   **A. Attempt Compliant Fixes:** You **MUST** first exhaust **all reasonable possibilities** to fix the error by modifying **only the Python source code (`.py`, `.pyi`)**. These attempted fixes **MUST** strictly adhere to **ALL other project rules** (especially `RULE-NO-SILENCING-V4`'s prohibition on `Any`, ignores, casts; and `RULE-RUNTIME-SAFETY-V2`'s requirements for runtime checks). Iterate on potential compliant source code fixes if the first attempt fails static analysis again.
        *   **B. Identify True Config Blocker:** Only after demonstrating that **no compliant source code modification** can resolve the static analysis error *without* violating other core project rules, can the issue be declared a configuration-related blocker.
        *   **C. HALT and Document (If Blocked):** If, and **only if**, a compliant source code fix is proven impossible after diligent attempts (as per step B), you **MUST HALT** work on the current task and document **precisely**:
            *   The specific static analysis error(s) (Tool, Code, Message, File, Line).
            *   The problematic code snippet.
            *   A summary of the compliant source code fixes attempted and why they failed or violated other rules.
            *   A clear statement explaining why the *existing configuration* prevents a compliant fix.
            *   The final explicit statement: **"HALTED: Configuration conflict prevents compliant source code fix. Awaiting user guidance or configuration review."**
        *   **D. DO NOT Suggest Config Changes:** Even when halted, do not suggest specific configuration changes.

3.  **Rationale:** Project configurations define the non-negotiable quality baseline. This rule mandates exhaustive effort to achieve compliance via source code changes first. It ensures that halting due to configuration is a last resort, only occurring after demonstrating that compliance within the current ruleset is genuinely impossible for the specific issue, thereby requiring high-level User intervention.