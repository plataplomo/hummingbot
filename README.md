# CyberDeltaEngine - Funding Rate Arbitrage Bot Implementation

This directory contains the source code, documentation, and configuration for the implementation of the CyberDeltaEngine Funding Rate Arbitrage strategy.

It is built based on the mathematical models and strategic concepts detailed in the parent `strategy_math` directory, specifically focusing on the refined rule-based approach incorporating dynamic risk management, oracle validation, utility ranking, and adaptation loops.

## Project Structure

- `docs/`: Detailed documentation covering architecture, setup, components, API integration, deployment, testing, frontend plans, and **database strategy**.
    - `current_workflow/`: Status updates and detailed plans.
    - `diagrams/`: Mermaid diagrams for architecture, workflow, etc.
    - `api_integration/`: Notes on specific exchange/bridge APIs.
    - `component_details/`: Deeper dives into core components.
    - `frontend_plan.md`: Goals and plan for the monitoring UI.
    - `database_plan.md`: Considerations for database usage.
- `cyberdelta/`: Python source code for the trading bot.
    - `apis/`: Wrappers and clients for interacting with exchange (Backpack, Hyperliquid, Paradex) and bridge APIs.
    - `core/`: Core logic components (Data Handler, Signal Generator, Risk Manager, Collateral Manager, Execution Handler, Portfolio Tracker, Adaptation Loop).
    - `utils/`: Helper functions, logging configuration, constants.
    - `config/`: Configuration loading and validation.
    - `main.py`: Main application entry point and orchestrator.
- `tests/`: Unit and integration tests.
- `config.yaml`: Strategy parameters and settings.
- `.env.example`: Template for environment variables (API keys, etc.).
- `requirements.txt`: Python dependencies.
- `README.md`: This file.

## Getting Started

1.  **Setup:** Refer to `docs/setup_guide.md` for detailed instructions on setting up the development environment, installing dependencies, and configuring API keys.
2.  **Configuration:** Copy `.env.example` to `.env` and fill in your API credentials. Adjust strategy parameters in `config.yaml` as needed.
3.  **Running:** Execute the main script (details in `docs/deployment_guide.md`).

## Technology Stack

- **Language:** Python 3.13+
- **Concurrency:** `asyncio`
- **API Interaction:** `aiohttp`, `websockets`, `ed25519`
- **Numerics:** `numpy`
- **Configuration:** `PyYAML`, `python-dotenv`
- **Logging:** Built-in `logging` module
- **Testing:** `pytest`, `pytest-asyncio`
- **Linting/Formatting:** `ruff`, `mypy`

## Current Status

[Specify current development status - e.g., Initial setup, Component prototyping, etc.]

## Contributing

[Outline contribution guidelines if applicable] 