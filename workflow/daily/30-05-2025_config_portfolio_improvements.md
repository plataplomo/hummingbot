
**Prompt for AI Coder (Angel): Integrate `PortfolioTrackerConfig` into `AppSettings`**

**Project:** CyberDeltaEngine
**Context:**
The `PortfolioTracker` class currently defines and instantiates its own `PortfolioTrackerConfig` with hardcoded default values (e.g., `data_freshness_seconds`). For better central configuration management and adherence to our Pydantic modeling strategy, this configuration should be part of the main `AppSettings` and loaded from `config.yaml`.

**Goal:**
1.  Move the `PortfolioTrackerConfig` Pydantic model definition to `cyberdelta/config/config_models.py`.
2.  Add a new field `portfolio_tracker: PortfolioTrackerConfig` to the `AppSettings` model.
3.  Update `config.yaml` (and `config.yaml.example`) to include a `portfolio_tracker:` section with relevant settings (e.g., `data_freshness_seconds`).
4.  Refactor `PortfolioTracker.__init__` to accept an instance of `PortfolioTrackerConfig` (sourced from `AppSettings`) instead of instantiating it internally.
5.  Ensure `PortfolioTracker._initialize_from_config()` uses the `PortfolioTrackerConfig` passed via `__init__`.

**Why:**
*   Centralizes all application configuration within `AppSettings` and `config.yaml`.
*   Allows users to configure `PortfolioTracker` behavior (like data freshness) externally.
*   Ensures `PortfolioTracker`'s configuration is validated by Pydantic as part of the overall app settings load.
*   Improves consistency in how components receive their configuration.

**What to do (Step-by-Step):**

1.  **Define/Move `PortfolioTrackerConfig`:**
    *   **File:** `cyberdelta/config/config_models.py`
    *   **Action:** Define (or move if it already exists elsewhere and is suitable) the `PortfolioTrackerConfig` Pydantic model.
    *   **Fields (example, based on current `PortfolioTracker` defaults):**
        ```python
        class PortfolioTrackerConfig(BaseModel):
            model_config = ConfigDict(extra="forbid", frozen=True)
            data_freshness_seconds: int = Field(DEFAULT_DATA_FRESHNESS_SECONDS, gt=0)
            # Add other relevant fields if identified, e.g., initial_balances, initial_positions
            # For initial_balances: dict[ExchangeId, dict[str, str]] = Field(default_factory=dict)
            # For initial_positions: list[DerivativePositionSchema] = Field(default_factory=list)
            # Note: If initial_positions are complex, you might need a schema for them here
            # or reference the core DerivativePosition (but be mindful of config vs. runtime models)
            # For now, let's focus on data_freshness_seconds. Initial balances/positions
            # are often better handled via state loading or specific setup scripts.
        ```
        *   Ensure `DEFAULT_DATA_FRESHNESS_SECONDS` is defined or replaced with a literal.

2.  **Update `AppSettings`:**
    *   **File:** `cyberdelta/config/config_models.py`
    *   **Class:** `AppSettings`
    *   **Action:** Add a new field:
        ```python
        portfolio_tracker: PortfolioTrackerConfig
        ```
        *   Ensure `PortfolioTrackerConfig` is imported.

3.  **Update `config.yaml` and `config.yaml.example`:**
    *   **Action:** Add a new top-level section:
        ```yaml
        portfolio_tracker:
          data_freshness_seconds: 60 # Example value
          # initial_balances: # Example for future
          #   hyperliquid:
          #     USDC: "10000.00"
          # initial_positions: [] # Example for future
        ```

4.  **Refactor `PortfolioTracker.__init__`:**
    *   **File:** `cyberdelta/core/portfolio_tracker.py`
    *   **Class:** `PortfolioTracker`
    *   **Change `__init__` signature:**
        ```python
        # Old: def __init__(self, app_settings: AppSettings, ...)
        # New:
        def __init__(
            self,
            app_settings: AppSettings, # Keep for other general settings if needed
            pt_config: PortfolioTrackerConfig, # New parameter
            api_clients: dict[str, ExchangeAPI] | None = None,
            # ... other existing parameters like symbol_mapper
            symbol_mapper: SymbolMapper | None = None,
        ) -> None:
        ```
    *   **Modify logic:**
        *   Remove the internal instantiation of `self.pt_config`.
        *   Assign the passed `pt_config` to `self.pt_config`.
        *   `self.pt_config = pt_config`
        *   The `_initialize_from_config()` method should now directly use `self.pt_config.initial_balances`, etc. (if those fields are added to `PortfolioTrackerConfig`). For `data_freshness_seconds`, it's typically used directly by methods like `is_stale` rather than in `_initialize_from_config`.

5.  **Update `PortfolioTracker._initialize_from_config()`:**
    *   **File:** `cyberdelta/core/portfolio_tracker.py`
    *   **Logic:** If `initial_balances` and `initial_positions` are moved to `PortfolioTrackerConfig`, this method should read them from `self.pt_config` instead of its current hardcoded/default logic.
    *   Example for balances (if `initial_balances` is added to `PortfolioTrackerConfig`):
        ```python
        # Inside _initialize_from_config
        for exchange_id, balances in self.pt_config.initial_balances.items():
            for asset, quantity_str in balances.items():
                # ... (rest of the logic for parsing and setting balances)
        ```

6.  **Update Instantiation Sites:**
    *   Locate where `PortfolioTracker` is instantiated (likely in your main application setup or tests).
    *   Ensure it's now passed `app_settings.portfolio_tracker` for the `pt_config` argument.
        ```python
        # Example
        from cyberdelta.config import get_app_settings
        app_settings = get_app_settings()
        # ...
        portfolio_tracker = PortfolioTracker(
            app_settings=app_settings, # Still pass app_settings if PortfolioTracker uses other parts of it
            pt_config=app_settings.portfolio_tracker,
            api_clients=...,
            symbol_mapper=...
        )
        ```

7.  **Testing Requirements:**
    *   Update unit tests for `PortfolioTracker` to mock and pass `PortfolioTrackerConfig`.
    *   Add tests to verify that settings from `PortfolioTrackerConfig` (e.g., `data_freshness_seconds`) are correctly applied and used by `PortfolioTracker`.
    *   Test that if `portfolio_tracker` section is missing or invalid in `config.yaml`, `AppSettings` validation fails appropriately.

8.  **Static Analysis and Reporting:**
    *   Run static analysis (Mypy, Pylint, Ruff) after changes and report any new warnings/errors.
    *   Ensure all changes strictly adhere to project rules.

