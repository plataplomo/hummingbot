# Security Audit Report: Part 1 - Input Validation

**Rule Reference:** `Assume_Hostile_Input_Validation.mdc` (Implied rule - based on user prompt)

**Assessment Summary:** Critical Gaps

**Detailed Findings:**

The application's approach to validating external inputs (API responses, configuration files, persisted state) is critically insufficient and relies heavily on trusting the structure and types provided by external sources.

1.  **API Response Validation (Critical, High Severity):**
    *   Both `BackpackAPI` (`cyberdelta/apis/backpack.py`) and `HyperliquidAPI` (`cyberdelta/apis/hyperliquid.py`) parse JSON responses and directly instantiate `@dataclass` models from `cyberdelta/core/models.py`.
    *   Validation is primarily limited to basic type conversions (e.g., to `Decimal`) and `None` checks within the `@dataclass` `__post_init__` methods.
    *   There is no strict schema validation (e.g., using Pydantic) to ensure all required fields are present, no unexpected fields exist, and values conform to expected types, ranges, or formats *before* attempting to create application objects.
    *   Reliance on direct dictionary key access (`response['key']`) or `.get()` without comprehensive checks makes the code brittle and vulnerable to `KeyError` or `TypeError` if the API response deviates slightly. Malformed numeric strings or unexpected data types can cause `ValueError` or `InvalidOperation` during `Decimal` conversion, potentially crashing processing loops.
    *   This applies to critical data like Tickers, Order Books, Balances, Positions, and Orders.

2.  **Configuration File Validation (High Severity):**
    *   `ConfigManager` (`cyberdelta/config/config_manager.py`) uses `yaml.safe_load`, preventing YAML code execution vulnerabilities.
    *   However, its `_validate_config` method only checks for the presence of a few top-level sections (`general`, `exchanges`, `strategies`, `risk`) and checks if specific exchanges are enabled.
    *   It **lacks schema validation** for the *content* of the configuration. Types, formats (e.g., URL syntax), value ranges, and allowed string values within the YAML are not validated.
    *   Incorrectly formatted API endpoints, invalid numerical parameters (e.g., risk limits), or misspelled strategy names could pass validation but cause runtime failures or misconfigurations.

3.  **Persisted State Validation (Medium Severity):**
    *   `StateManager` (`cyberdelta/utils/state_manager.py`) loads state using `json.load`.
    *   It performs an integrity check using `_verify_state_integrity`, which validates the presence of top-level `state` and `metadata` keys and compares a basic checksum (`str(hash(json.dumps(...)))`).
    *   The checksum provides weak protection against accidental corruption but minimal security against tampering.
    *   Crucially, it **does not validate the contents or structure of the loaded `state` dictionary**. Corrupted or maliciously crafted state data (e.g., invalid position quantities, incorrect asset names) could be loaded, leading to application errors or incorrect behavior upon resuming operations.

**Code Snippets:**

*   **API Parsing Weakness (`cyberdelta/apis/backpack.py` - similar pattern in `hyperliquid.py`):**
    ```python
    # Example from get_ticker in backpack.py - Direct instantiation relies on __post_init__
    ticker = Ticker(
        symbol=response["symbol"], # Assumes 'symbol' key exists and is correct type
        bid=Decimal(str(response["bidPrice"])), # Assumes 'bidPrice' exists, is string convertible to Decimal
        ask=Decimal(str(response["askPrice"])), # Assumes 'askPrice' exists, is string convertible to Decimal
        price=Decimal(str(response["lastPrice"])), # Assumes 'lastPrice' exists, is string convertible to Decimal
        volume=Decimal(str(response["volume"])), # Assumes 'volume' exists, is string convertible to Decimal
        timestamp=int(response["time"]), # Assumes 'time' exists and is convertible to int
    )
    ```

*   **Config Validation Weakness (`cyberdelta/config/config_manager.py`):**
    ```python
    # _validate_config only checks for section presence, not content/types/schema
    required_sections = ["general", "exchanges", "strategies", "risk"]
    missing_sections = []
    for section in required_sections:
        if section not in self.config:
            missing_sections.append(section)
    # ... further checks are similarly superficial ...
    ```

*   **State Loading Weakness (`cyberdelta/utils/state_manager.py`):**
    ```python
    # load_state uses json.load directly
    with open(self.state_file) as file:
        state_data = json.load(file)

    # _verify_state_integrity only checks checksum and top-level keys, not state content
    if not self._verify_state_integrity(state_data):
        # ... recovery logic ...
        return self._recover_from_backup()

    # No validation of state_data["state"] content itself
    self.current_state = state_data["state"]
    ```

**Mermaid Snippets:**

*   **API Data Flow:**
    ```mermaid
    graph LR
        A[External API (e.g., Backpack)] -- JSON Response --> B(APIs Module);
        B -- Raw Dict --> C(Dataclass Constructor);
        C -- Weak __post_init__ check --> D{Application Logic};
        subgraph Vulnerability
            direction LR
            B -- Unvalidated Data --> C;
        end
        D -- Uses potentially invalid data --> E(Trading Decisions / State);
    ```

*   **Config/State Loading Flow:**
    ```mermaid
    graph LR
        A[Config/State File (YAML/JSON)] -- Read File --> B(Config/State Manager);
        B -- Basic Checks (Presence/Checksum) --> C{Application Logic};
        subgraph Vulnerability
            direction LR
            A -- Unvalidated Content --> B;
        end
        C -- Uses potentially invalid config/state --> D(System Behavior);
    ```

**Recommendations:**

1.  **Adopt Pydantic:** Refactor all data models in `cyberdelta/core/models.py` to use Pydantic `BaseModel` instead of `@dataclass`. Define strict types, constraints (e.g., `gt=0` for positive numbers), and validation rules directly in the models. (Critical)
2.  **Implement Strict API Parsing:** Modify the `parse_*` methods in `cyberdelta/apis/base.py` (and implementations in `backpack.py`, `hyperliquid.py`) to parse the incoming JSON dictionary into the corresponding Pydantic model *first*. Use `model_validate()` or `model_validate_json()`. Handle Pydantic `ValidationError` exceptions specifically, logging detailed error context and potentially raising a standardized `APIError` with `INVALID_PARAMS`. (Critical)
3.  **Implement Configuration Schema:** Define Pydantic models representing the expected structure and types for `config.yaml`. In `ConfigManager.load`, after `yaml.safe_load`, validate the loaded dictionary against these Pydantic models. Fail loading on validation errors. (High)
4.  **Implement State Schema Validation:** Define Pydantic models for the expected structure of the persisted state. In `StateManager.load_state` and `_recover_from_backup`, after loading the JSON and verifying the basic integrity/checksum, validate the `state_data['state']` dictionary against these Pydantic models *before* assigning it to `self.current_state`. Handle `ValidationError` appropriately (e.g., log error, potentially discard corrupted state/backup). (Medium)
5.  **Centralized Error Handling:** Ensure that validation errors (Pydantic `ValidationError`, `KeyError`, `TypeError`, `ValueError` during parsing) are caught specifically, logged with context (input source, problematic data), and mapped to appropriate application-level errors or recovery strategies, rather than relying on broad `except Exception`. (High)

**Severity Assessment:**

*   API Response Validation: **Critical**
*   Configuration File Validation: **High**
*   Persisted State Validation: **Medium**