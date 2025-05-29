---
id: RULE-NO-SILENCING-V4
title: Strict Control over Static Analysis Silencing (Minimal Cast Exception)
description: Strictly prohibits `# type: ignore` and `# noqa` in core code. Forbids `typing.cast` except under rare, explicitly justified, and runtime-verified conditions. Requires justification for silencing in tests.
globs: ["*.py", "*.pyi"] # Apply to all Python source and stub files
alwaysApply: true
severity: critical # Violation is a critical failure
---

# Strict Control over Static Analysis Silencing (Minimal Cast Exception)

1.  **Zero Tolerance for Inline Ignores:**
    *   The use of inline silencing directives (`# type: ignore[...]`, `# noqa[...]`) is **ABSOLUTELY PROHIBITED** in all core application source code (`.py` files outside designated test directories) and stub files (`.pyi`).

2.  **Strict Control over `typing.cast` in Core Code:**
    *   The use of `typing.cast` is **STRONGLY DISCOURAGED** and generally **FORBIDDEN** in core application code. Its use indicates a potential failure in static typing or code structure that **MUST** ideally be resolved through:
        *   Improved type hinting (`Union`, generics, `Literal`, etc.).
        *   Using `TypeGuard` or `Protocol` for complex type narrowing.
        *   Using `@overload` for functions with multiple signatures.
        *   Refactoring code/data structures for better type inference.
    *   **Exception Protocol:** In **rare and demonstrably necessary circumstances** where alternative typing solutions are proven impractical (e.g., unavoidable interaction with fundamentally untypable external library APIs, complex metaprogramming results), `cast(TargetType, var)` *may* be permitted **ONLY IF ALL** following conditions are met **PER INSTANCE**:
        *   **A. Exhaustive Justification:** A mandatory, detailed multi-line comment **MUST** precede the `cast`, explaining:
            *   Why the type checker cannot infer the type correctly.
            *   What alternative typing solutions (`TypeGuard`, `Protocol`, refactoring, etc.) were considered and why they were deemed insufficient or impractical *in this specific case*.
            *   Why the developer is certain the `cast` is safe at this point in the code flow.
        *   **B. Mandatory Runtime Verification:** The line immediately following the `cast` **MUST** contain an `assert isinstance(var, TargetType)` check (using the same `TargetType` as the cast) to verify the cast's validity during development and testing. This assertion **MUST NOT** be removed.
        *   **C. Explicit User Review Flag:** The justification comment **MUST** include the tag `#[CAST-REVIEW-REQUIRED]` to flag it for mandatory User review during code integration. The User reserves the right to reject the use of `cast` if the justification is deemed insufficient.
    *   Routine use of `cast` to bypass type errors is **NOT** an acceptable exception.

3.  **Limited Silencing/Cast in Tests:**
    *   Limited use of `# type: ignore[...]` or `cast` *may* be permissible **only** within test files (`tests/.../*.py`) and **only** when strictly necessary for mocking complex objects or interacting with untyped test fixtures where alternatives are impractical.
    *   **Each instance MUST** have a comment on the same line explaining *precisely* why it's unavoidable. Excessive use is discouraged and subject to User review.

4.  **Mandatory Resolution Path:**
    *   All *other* static analysis errors/warnings (not covered by the `cast` exception protocol) **MUST** be addressed by fixing the code compliantly or documenting as a blocker per `RULE-STATIC-ANALYSIS-V3` and `RULE-CONFIG-INTEGRITY-V3`.

5.  **Rationale:** This rule maintains an extremely high bar against silencing static analysis feedback. Inline ignores are forbidden in core code. `cast` is treated as a code smell requiring extraordinary justification, runtime verification, and explicit flagging for User review. This ensures type safety remains paramount, while providing a strictly controlled escape hatch for truly unavoidable typing limitations, preventing them from becoming silent blockers.