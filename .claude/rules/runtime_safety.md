---
id: RULE-RUNTIME-SAFETY-V4
title: Pragmatic Runtime Safety & Explicit Contracts
description: Mandates strict Decimal usage and explicit runtime checks for Optionals and finiteness, prioritizing robustness. Relaxes redundant isinstance checks *after* Pydantic validation but requires documentation for necessary static analysis conflicts.
alwaysApply: true
severity: critical
---

# Pragmatic Runtime Safety & Explicit Contracts

**Mandate:** Code **MUST** prioritize demonstrable runtime safety, especially regarding financial types (`Decimal`) and optional values (`None`), while trusting Pydantic models as the primary type/structure validation boundary. Static type hints define intent; runtime checks guard against operational errors and `None`/non-finite states.

**Guidelines:**

1.  **Strict `Decimal` Usage:**
    *   All financial calculations, storage, and representations **MUST** use `decimal.Decimal`.
    *   `Decimal` instances **MUST** be initialized from `str` literals or other `Decimal` instances. Direct initialization from `float` is **STRICTLY FORBIDDEN**.
    *   No `float` type hints or instances permitted for financial values.

2.  **Mandatory Pre-Operation Checks for Optionals:**
    *   Before **any** operation (arithmetic, comparison, function call, attribute access) on a variable `var` hinted as `Optional[T]`:
        *   An `if var is None:` or `if var is not None:` check **MUST** immediately precede the operation block.
        *   The `None` case **MUST** be explicitly handled (raise error, safe default, control flow change). Implicitly proceeding is forbidden.
    *   For `Optional[Decimal]` or `Optional[float]` (non-financial only), the check **MUST** be combined: `if var is not None and var.is_finite():`. The `else` block must handle both `None` and non-finite cases.

3.  **Trust Pydantic Boundaries (Relaxed `isinstance`):**
    *   After data has been successfully validated and parsed into a **project-defined Pydantic `BaseModel` instance** (either a Raw model or an Internal model), subsequent code receiving that *validated instance* **SHOULD generally trust** the types defined by the model's fields based on the successful validation.
    *   **Explicit `isinstance` checks** (like `if not isinstance(p.field, ExpectedType):`) immediately after receiving a validated Pydantic model instance are **NOT mandatory** and should be avoided unless there is a *specific, documented reason* why Pydantic's validation might be insufficient for runtime safety in that context (e.g., complex `Union` types, interaction with poorly typed external libraries *after* initial validation, highly critical security boundaries).

4.  **Mandatory `is_finite()` Checks Post-Parsing/Calculation:**
    *   Immediately after parsing external data to a `Decimal` (e.g., within a Pydantic validator using `parse_decimal_value`) or performing calculations that could yield non-finite `Decimal` results (`Infinity`, `NaN`), the resulting `Decimal` **MUST** be checked using `.is_finite()`. Raise `ValueError` if non-finite unless explicitly permitted and documented for that context. *(Note: This check is crucial *during* parsing/calculation; Guideline #2 handles checks *before use* of potentially non-finite Optionals).*

5.  **Documenting Necessary Static Analysis Conflicts:**
    *   If a mandatory runtime check (per Guidelines #2, #4) causes `mypy` or `ruff` to report an error (`[unreachable]`, `[redundant-expr]`, etc.) due to type hints:
        *   The runtime check **MUST** be kept.
        *   A comment **MUST** be added following the format: `# DEFENSIVE CHECK: [Brief reason]. Mypy=[<code>] Ruff=[<code>]`
        *   **NO** silencing directives (`# type: ignore`, `# noqa`, `cast`) are permitted in core code (`RULE-NO-SILENCING-V4`).

6.  **Strict Adherence:** Violations, especially suppressing documented conflicts, are critical failures.

**Rationale for Hardening:** This version maintains strictness on `Decimal` usage, `Optional` handling, and `is_finite` checks. However, it explicitly relaxes the requirement for universal `isinstance` checks *after* data has passed a Pydantic validation boundary, trusting Pydantic as the primary type enforcer in most cases. This reduces boilerplate while still mandating critical runtime checks for `None` and non-finite states and requiring documentation for unavoidable static analysis conflicts arising from those necessary checks.