---
id: RULE-ARCH-MODEL-DESIGN-V2
title: Strict Core Model Architecture, Validation & Typing Policy
description: Defines mandatory architectural separation, naming, validation philosophy, typing constraints, and design patterns (incl. Idea 5) for Raw API and Internal Domain models. Integrates with runtime safety and no-silencing rules.
alwaysApply: true
severity: critical # Adherence to these patterns is critical for consistency and robustness.
---

# Strict Core Model Architecture, Validation & Typing Policy

This document codifies the **non-negotiable, strictly enforced** architectural design principles for all data models within the CyberDeltaEngine project.

**1. Absolute Separation of Concerns:**

*   **Raw API Models:**
    *   **Location:** Exclusively within `cyberdelta/apis/<exchange_name>/models/`. No Raw models are permitted in `core/`.
    *   **Purpose:** Strict validation boundary. Represent the **exact structure and raw data types** (`str`, `int`, `bool`, `list`, `dict`, potentially `float` for non-financial) received from external APIs *before* transformation. Validate the *external contract*.
    *   **Naming:** Mandatory prefix `Raw` appended to concept, preceded by exchange identifier (e.g., `BackpackRawOrder`, `HyperliquidRawFill`).
    *   **Dependencies:** MUST NOT import from or depend on `cyberdelta/core/models/`. May import project `Enum`s ONLY if the raw API *directly* uses the exact same string representation as the enum value.
    *   **Logic Prohibition:** MUST NOT contain internal business logic, complex calculations, data enrichment, or state modification logic.
*   **Internal Domain Models:**
    *   **Location:** Exclusively within `cyberdelta/core/models/` (and submodules).
    *   **Purpose:** Represent the **unified, canonical business concepts**, using clean internal types. Act as the target for transformation from validated Raw models. Define the application's internal data language.
    *   **Naming:** Clear business concept names. No `Raw` or exchange prefixes (e.g., `Order`, `SpotBalance`).
    *   **Dependencies:** MUST NOT import from or depend on `cyberdelta/apis/` models. Define fields using internal types ONLY.

**2. Pydantic `BaseModel` Mandatory:**

*   All Raw API Models and Internal Domain Models **MUST** inherit from `pydantic.BaseModel`.

**3. Raw Model Validation Policy (Strict Boundary):**

*   **Mandatory Validators:** **Every field** MUST have a `@field_validator(..., mode='before')` unless Pydantic's default handling for simple primitives (`int`, `bool`) is demonstrably sufficient *and* safe.
*   **Focus:** Validate the external API contract ONLY. Structural integrity, raw type adherence, basic format validity.
*   **Checks:**
    *   **Structure:** Validate overall structure (e.g., list length, dict keys if necessary).
    *   **Raw Type:** Explicitly check `isinstance` against expected *raw* input type(s).
    *   **Strings:** Use `validate_str_field`: check non-emptiness (for required), `max_length`, valid UTF-8. **NO** `cast` or ignores (`RULE-NO-SILENCING-V4`).
    *   **Numeric Strings:** Use `validate_str_field` THEN `parse_decimal_value`: ensure parseable to **finite** `Decimal`/`int`/`float`. Check non-negativity (`>=0`) ONLY if the constraint is *fundamental* to the raw data format (e.g., counts, sizes usually >=0), not based on business logic.
    *   **Ints/Floats:** Check `isinstance`. Check finiteness for floats. Check non-negativity for counts/IDs/timestamps if applicable.
    *   **Booleans:** Check `isinstance(v, bool)`. Reject string coercion ('true'/'false').
    *   **Enums (Rarely Applicable):** Use `validate_enum_field` *only* if the raw API *guarantees* specific string literals from a known, fixed set.
*   **Configuration:** MUST use `model_config = ConfigDict(extra='forbid', frozen=True)`. Use `populate_by_name=True` if aliases (`Field(alias=...)`) are used. *(Note: Nested raw components might use `extra='ignore'` if parent forbids).*
*   **NO Business Logic:** Absolutely NO checks like `price > 0`, `high >= low`, cross-field business rules.

**4. Internal Model Design & Validation Policy (Internal Consistency):**

*   **Strict Internal Types:** MUST use precise internal types: `Decimal` (NO `float` for finance), `datetime` (UTC required, use `default_factory` or validator), `core/models/enums.py::EnumType`, `bool`, `str`, `int`. **NO `typing.Any`** (`RULE-NO-SILENCING-V4`). Use standard collections (`list`, `dict`) with specific type arguments.
*   **Validation Focus:** Enforce internal consistency, domain invariants, and safe types *after* parsing/coercion.
*   **Implementation:**
    *   Use `Field(...)` constraints for simple bounds (`gt`, `ge`, `max_length`).
    *   Use `@field_validator(..., mode='before')` **only** for parsing/coercing diverse inputs (str/int/float -> `Decimal`; ts -> `datetime`) via `utils.parsing` helpers. Ensure output matches field type hint and is finite for numerics.
    *   Use `@model_validator(mode='after')` **only** for essential cross-field consistency checks (`high >= low`, `fee_asset` required, `entry_price`/`size` logic). Keep these validators focused and minimal.
*   **Extensibility (Idea 5 - Mandatory):** Models for `Trade`, `Order`, `DerivativePosition` (and others identified as needing exchange-specific enrichment) **MUST** use the "Core + Typed Extension Slots" pattern precisely as defined in previous discussions (lean core, separate immutable `*Details(BaseModel)` with `extra='ignore'`, optional slots `*_details` on core model using `Field(default=None)`).
*   **Immutability/Mutability:** Snapshots (`SpotBalance`, `Ticker`, `Candle`, `Trade`, `MarginAccountSummary`, `*Details`) **MUST** use `frozen=True`. State aggregates (`Order`, `DerivativePosition`) **MUST NOT** use `frozen=True`.
*   **Configuration:** Core models MUST use `model_config = ConfigDict(extra='forbid', validate_assignment=True, ...)`. Include `frozen=True` as appropriate per above.
*   **Runtime Safety:** Adhere strictly to `RULE-RUNTIME-SAFETY-V4` (e.g., mandatory `None`/`finite` checks before operations, defensive `isinstance`). Document necessary static analysis conflicts generated by these checks.

**5. Helper Usage & Consistency:**

*   Parsing/Validation helpers from `cyberdelta.utils.parsing` **MUST** be used consistently where applicable.
*   Validation logic (e.g., finite decimal check, non-empty string check) **MUST** be applied consistently across all relevant models (Raw and Internal).

**Rationale:** This hardened rule provides an unambiguous specification for model design, separating external validation from internal logic, mandating specific patterns (Idea 5), enforcing strict typing and validation at both layers, and integrating with other project rules to ensure maximum robustness, consistency, and maintainability. Violations require immediate correction or formal escalation.
