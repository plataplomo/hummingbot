# cyberdelta/utils/ — Per-Folder Analysis

---

## state_manager.py
**Purpose:**
Provides robust state persistence and recovery for the engine, including atomic state saving, integrity validation, backup rotation, and corruption recovery. Ensures reliable restoration of engine state after failures or restarts.

```mermaid
flowchart TD
    A[Init StateManager] --> B[Load/Save State]
    B --> C[Verify Integrity]
    C --> D[Create/Rotate Backups]
    D --> E[Recover from Backup]
```

```mermaid
sequenceDiagram
    participant StateMgr as StateManager
    participant File as State File/Backup
    participant Engine as Engine
    Engine->>StateMgr: Save/load state
    StateMgr->>File: Read/write/backup
    StateMgr-->>Engine: Return state/result
```

**Summary:**
- Inputs: State dictionaries, config.
- Outputs: Persisted state files, backups, recovery actions.
- Dependencies: Config, OS/filesystem, JSON.
- Critical Path: Ensures engine can recover from crashes or corruption.

---

## constants.py
**Purpose:**
Defines system-wide constants, enums, and configuration defaults for the engine, including exchange IDs, event types, timeframes, and rate limits. Centralizes values for maintainability and consistency.

```mermaid
flowchart TD
    A[Define Constants/Enums] --> B[Expose for Use]
    B --> C[Reference in Engine/Modules]
```

```mermaid
sequenceDiagram
    participant Const as Constants
    participant Module as Engine/Module
    Module->>Const: Import/use constants
    Const-->>Module: Provide values
```

**Summary:**
- Inputs: None (static definitions).
- Outputs: Constants and enums for use throughout the codebase.
- Dependencies: None.
- Critical Path: Ensures consistent configuration and event handling.

---

## parsing.py
**Purpose:**
Provides robust, reusable utilities for parsing datetimes (ensuring UTC-awareness) and decimals (ensuring precision and safety). Used throughout the engine for model and data validation.

```mermaid
flowchart TD
    A[Parse Input] --> B[Convert to Datetime/Decimal]
    B --> C[Return Parsed Value or Error]
```

```mermaid
sequenceDiagram
    participant Parser as ParsingUtils
    participant Caller as Model/Module
    Caller->>Parser: Parse value
    Parser-->>Caller: Return datetime/decimal or error
```

**Summary:**
- Inputs: Raw values (str, int, float, etc.).
- Outputs: Parsed datetime/Decimal or error.
- Dependencies: datetime, decimal.
- Critical Path: Ensures correctness and safety in all time/amount handling.

---

## config.py
**Purpose:**
Manages application configuration, supporting loading from YAML files, environment variables, and direct dictionaries. Provides dot-notation access, merging, and saving of config data.

```mermaid
flowchart TD
    A[Init Config] --> B[Load from YAML/Env]
    B --> C[Access/Set Values]
    C --> D[Merge/Save Config]
```

```mermaid
sequenceDiagram
    participant Config as Config
    participant Engine as Engine/Module
    Engine->>Config: Get/set/merge config
    Config-->>Engine: Provide config values
```

**Summary:**
- Inputs: YAML files, env vars, dicts.
- Outputs: Config values, merged/saved configs.
- Dependencies: YAML, OS, logging.
- Critical Path: Central to all configuration management and environment control.

---

## logging_config.py
**Purpose:**
Configures and manages logging for the engine, supporting console/file handlers, log levels, and module-specific overrides. Provides context managers for log capture and testing.

```mermaid
flowchart TD
    A[Setup Logging] --> B[Configure Handlers]
    B --> C[Set Levels/Format]
    C --> D[Capture/Export Logs]
```

```mermaid
sequenceDiagram
    participant Logger as LoggingConfig
    participant Engine as Engine/Module
    Engine->>Logger: Setup logging/get logger
    Logger-->>Engine: Provide logger/capture logs
```

**Summary:**
- Inputs: Config, log level, file/module settings.
- Outputs: Configured loggers, captured logs.
- Dependencies: logging, OS, sys.
- Critical Path: Ensures observability, debugging, and auditability.

---

## serialization.py
**Purpose:**
Provides custom JSON serialization/deserialization utilities, handling Decimals, datetimes, and numpy types for safe, precise data interchange and persistence.

```mermaid
flowchart TD
    A[Prepare Data] --> B[Custom Encode]
    B --> C[Dump/Load JSON]
```

```mermaid
sequenceDiagram
    participant Ser as Serialization
    participant Engine as Engine/Module
    Engine->>Ser: Dump/load JSON
    Ser-->>Engine: Return JSON/data
```

**Summary:**
- Inputs: Data to serialize/deserialize.
- Outputs: JSON strings, loaded data.
- Dependencies: json, datetime, decimal, numpy.
- Critical Path: Ensures safe, precise data interchange and persistence.

---

## __init__.py
**Purpose:**
Marks the directory as a Python package. No runtime logic or exports.

```mermaid
flowchart TD
    A[Empty Init File] --> B[Package Structure]
```

```mermaid
sequenceDiagram
    participant Init as __init__.py
    participant User as Importer
    User->>Init: Import package
    Init-->>User: Provide package structure
```

**Summary:**
- Inputs: None (empty init).
- Outputs: Package structure.
- Dependencies: None.
- Critical Path: Not runtime critical, but important for package structure. 