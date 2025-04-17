# CyberDeltaEngine Model Alignment & Pydantic Refactoring Plan

## 1. **Current State: Model Inventory & Comparison**

### A. **Core Models (`cyberdelta/core/models/`)**

- **All major trading, portfolio, and strategy models are implemented as Pydantic `BaseModel` subclasses.**
- **Key models:**
  - `Order`, `Trade`, `OrderBook`, `Ticker`, `FundingRate`, `MarketData` (in `market.py`)
  - `Balance`, `Position` (in `portfolio.py`)
  - `TradeSignal`, `ArbitrageOpportunity` (in `strategy.py`)
  - **Enums:** `OrderSide`, `OrderType`, `OrderStatus`, `SignalType`, `TimeInForce` (in `enums.py`)
- **Features:**
  - Strict validation (`ConfigDict(extra="forbid", validate_assignment=True, frozen=True)` where appropriate)
  - Decimal usage for all financial fields (per project rule)
  - Field and model validators for type and business logic
  - Serialization helpers (`to_dict`)
  - Comprehensive docstrings

### B. **API Models (`cyberdelta/core/models/api.py`)**

- **Purpose:** API boundary/request/response models (e.g., `PlaceOrderRequest`, `OrdersResponse`)
- **Some fields use `Any` as a placeholder** (e.g., `orders: list[Any]`), with comments indicating intent to use core models.

### C. **Base API Layer (`cyberdelta/apis/base.py`)**

- **Abstract base class (`ExchangeAPI`)** defines the interface for all exchange APIs.
- **Error handling:** `APIErrorCode` (Enum), `APIErrorModel` (Pydantic), `APIError` (Exception wrapping the model)
- **Method signatures** for all exchange operations reference core models (e.g., `Order`, `Trade`, `Balance`, etc.), but do not define their own models.
- **No redundant model definitions**—relies on imports from `cyberdelta.core.models`.

---

## 2. **Gaps & Inconsistencies**

- **API models (`api.py`) sometimes use `Any` instead of the correct Pydantic model.**
- **Abstract base (`base.py`) is not itself a Pydantic model** (which is correct for an ABC, but its error model is Pydantic).
- **Some error handling logic is custom (not Pydantic), but the error model is.**
- **No business logic is duplicated between `base.py` and the core models.**

---

## 3. **Refactoring Plan: Pydantic-First, DRY, and Robust**

### **A. API Models: Strict Typing**

- **Replace all `Any` placeholders in API request/response models with the correct Pydantic models from `core.models`.**
  - Example:  
    ```python
    # Before
    orders: list[Any]  # Should be list[Order], but import from models.py if needed

    # After
    from cyberdelta.core.models.market import Order
    orders: list[Order]
    ```
- **Rationale:**  
  - Ensures full validation, serialization, and type safety at API boundaries.
  - Prevents runtime errors and enforces business logic.

### **B. Abstract Base API (`base.py`): Pydantic Everywhere**

- **Continue to use core models for all method signatures and return types.**
- **If any new data models are needed for base API logic, define them as Pydantic models in `core.models` and import them.**
- **Ensure all error models and error handling logic use Pydantic models for structure and validation.**
- **Document in the base class docstring that all data returned or accepted by the API must be a Pydantic model.**

### **C. Error Handling: Pydantic-Only**

- **Retain `APIErrorModel` as the canonical error structure.**
- **Ensure all exceptions raised by the API layer are constructed from or wrap a Pydantic model.**
- **If additional error types are needed, define them as Pydantic models and use them consistently.**

### **D. Enums: Pydantic-Compatible**

- **All enums used in models must be compatible with Pydantic v2+ (standard Python `Enum` is sufficient).**
- **If any custom serialization is needed, use Pydantic's `TypeAdapter` or field validators.**

### **E. Documentation & Testing**

- **Update all docstrings to reference the correct Pydantic models.**
- **Add/expand tests to ensure that all API endpoints and exchange implementations accept and return only valid Pydantic models.**
- **Test error serialization and deserialization using the Pydantic error models.**

---

## 4. **Implementation Steps**

### **Step 1: Refactor API Models**

- Update all request/response models in `cyberdelta/core/models/api.py` to use the correct Pydantic models for all fields.
- Remove all `Any` placeholders.

### **Step 2: Audit Abstract Base API**

- Review all method signatures in `cyberdelta/apis/base.py` to ensure they use only Pydantic models from `core.models`.
- If any method returns or accepts a dict, list, or primitive, consider if a Pydantic model is more appropriate.

### **Step 3: Error Handling Consistency**

- Ensure all error handling in `base.py` and exchange implementations uses `APIErrorModel` or other Pydantic error models.
- Remove any legacy or ad-hoc error structures.

### **Step 4: Documentation**

- Update all docstrings in `base.py` and `api.py` to reference the correct models.
- Add a section to the `base.py` module docstring explaining the Pydantic-first approach.

### **Step 5: Testing**

- Add/expand unit and integration tests to:
  - Validate all API request/response models.
  - Ensure all exchange implementations return/accept only valid Pydantic models.
  - Test error handling and serialization.

---

## 5. **Risks & Mitigations**

- **Breaking Change:**  
  - All downstream code (exchange implementations, strategies, API endpoints) must be updated to use the new, strictly-typed models.
- **Performance:**  
  - Pydantic validation adds overhead, but this is acceptable for API and data boundary layers.
- **Testing Required:**  
  - Comprehensive tests are mandatory to catch regressions and ensure correctness.

---

## 6. **Summary Table: Model Alignment**

| Model/Type         | In `core/models` | In `base.py` | Pydantic? | Action Needed?         |
|--------------------|------------------|--------------|-----------|-----------------------|
| Order              | Yes              | Used         | Yes       | Use core model        |
| Trade              | Yes              | Used         | Yes       | Use core model        |
| OrderBook          | Yes              | Used         | Yes       | Use core model        |
| Ticker             | Yes              | Used         | Yes       | Use core model        |
| FundingRate        | Yes              | Used         | Yes       | Use core model        |
| Balance            | Yes              | Used         | Yes       | Use core model        |
| Position           | Yes              | Used         | Yes       | Use core model        |
| TradeSignal        | Yes              | (Strategy)   | Yes       | Use core model        |
| APIErrorModel      | Yes (in base.py) | Yes          | Yes       | Already aligned       |
| Enums              | Yes              | Used         | Yes       | Already aligned       |
| API Request/Resp   | Yes (api.py)     | N/A          | Yes       | Replace Any w/ model  |

---

## 7. **References**

- See `workflow/pydantic.md` for Pydantic usage patterns and rationale.
- See `workflow/models_interaction_diagram.md` for model relationships and data flow.

---

## 8. **Next Steps**

1. **Refactor API models to use strict Pydantic types.**
2. **Audit and update all method signatures in `base.py` to use only Pydantic models.**
3. **Standardize error handling on Pydantic models.**
4. **Update documentation and add/expand tests.**
5. **Review and test all downstream code for compatibility.**

---

*Drafted by Angel, CyberDeltaEngine Senior Software Engineer/Architect.  
Requires human review and approval before production integration.* 