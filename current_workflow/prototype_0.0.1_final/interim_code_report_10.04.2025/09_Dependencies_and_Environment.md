# Code Report: CyberDeltaEngine - Dependencies and Environment

## 1. Overview

This document outlines the project's dependencies, environment setup, and management approach.

## 2. Core Dependencies

The project relies on several key Python libraries:

- **`aiohttp`**: For asynchronous HTTP client/server operations (used heavily in `apis/base.py`).
- **`websockets`**: (Potentially used alongside or instead of `aiohttp`'s WebSocket client, depending on specific API client needs - Check `requirements.txt`). For handling real-time data streams.
- **`PyYAML`**: For loading configuration files (`config.yaml`, `secrets.yaml`).
- **`pytest`**: The core framework for unit and integration testing.
- **`pytest-asyncio`**: Plugin for testing `asyncio` code with `pytest`.
- **`numpy`**: Numerical computing library (likely used for calculations in strategies or risk management, e.g., Pandas dependency).
- **`pandas`**: Data manipulation and analysis library (potentially used in strategies, analysis, or data handling).
- **`python-dateutil`**: Provides extensions to the standard `datetime` module (Pandas dependency).
- **`pytz`**: World timezone definitions (Pandas dependency).
- **`tzdata`**: Timezone database (Pandas dependency).
- **`requests`**: (Potentially used for synchronous operations or specific utility scripts, though `aiohttp` is primary for core async logic).
- **`matplotlib`**: Used for visualization in backtesting and performance monitoring components.

*(Note: This list is based on observed imports and test dependencies. A definitive list requires checking `requirements.txt` or `pyproject.toml`)*

## 3. Environment Management

- **Virtual Environment**: The project strongly enforces the use of a Python virtual environment (typically named `.venv`) located in the project root.
- **Dependency Specification**: Dependencies should be listed in `requirements.txt` or managed via `pyproject.toml` (using tools like Poetry or PDM, although current evidence points towards `requirements.txt` and `pip`).
- **Installation**: Dependencies are installed within the virtual environment using `.venv/bin/pip install -r requirements.txt` (or equivalent).
- **Execution**: All Python tools and scripts (e.g., `pytest`, `mypy`, `ruff`, `python main.py`) **must** be executed using the Python interpreter within the virtual environment (e.g., `.venv/bin/python -m pytest`). Relying on shell activation (`source .venv/bin/activate`) is discouraged for automated tasks.

**Guideline Enforcement**: The `venv_execution.mdc` rule explicitly mandates using the virtual environment path for all tool execution.

## 4. `requirements.txt` (Example Structure)

```
# Core Application Dependencies
aiohttp>=3.8.0,<4.0.0
websockets>=10.0,<12.0
PyYAML>=6.0,<7.0
numpy>=1.21.0,<2.3.0
pandas>=1.4.0,<2.3.0
python-dateutil>=2.8.0,<3.0.0
pytz>=2022.1
tzdata>=2022.1

# Testing Dependencies
pytest>=7.0.0,<8.0.0
pytest-asyncio>=0.21.0,<0.22.0
matplotlib>=3.5.0,<4.0.0 # For visualization tests

# Linting/Formatting (Development)
ruff>=0.1.0,<0.2.0
mypy>=1.0.0,<2.0.0
```
*(Note: Actual versions should be pinned based on compatibility testing.)*

## 5. Potential Issues / Considerations

- **Dependency Conflicts**: Need to ensure pinned versions in `requirements.txt` are compatible.
- **Environment Setup**: Clear instructions are needed for developers to set up the virtual environment and install dependencies correctly.
- **`pytest` Compatibility**: Recent issues were observed with `pytest` and `pytest-asyncio` compatibility, requiring specific versions (`pytest==7.4.0`, `pytest-asyncio==0.21.1`) to be installed. This highlights the need for pinned dependencies.
- **System Dependencies**: Check if any libraries have underlying system dependencies (e.g., C libraries for compilation) that need to be documented.

Maintaining a clean, isolated, and reproducible environment via the virtual environment and pinned dependencies is crucial for consistent development and deployment. 