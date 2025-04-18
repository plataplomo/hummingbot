# cyberdelta/apis/ — Per-Folder Analysis

---

## __init__.py
**Purpose:**
Initializes the apis package and exposes key API model classes for external use.

```mermaid
flowchart TD
    A[Import Key Symbols] --> B[Define __all__]
    B --> C[Expose API]
```

```mermaid
sequenceDiagram
    participant Init as __init__.py
    participant User as Importer
    User->>Init: Import symbol
    Init-->>User: Provide class/function
```

**Summary:**
- Inputs: None (package init).
- Outputs: Exposed API symbols.
- Dependencies: Internal API model modules.
- Critical Path: Not runtime critical, but important for package structure.

---

## base.py
**Purpose:**
Defines the abstract base class for all exchange API clients, including connection management, authentication, request/response handling, rate limiting, and error management. All concrete API clients inherit from this base class.

```mermaid
flowchart TD
    A[Init BaseAPIClient] --> B[Configure Connection]
    B --> C[Authenticate]
    C --> D[Send Request]
    D --> E[Handle Response]
    E --> F[Handle Errors]
    F --> G[Close Connection]
```

```mermaid
sequenceDiagram
    participant BaseAPI as BaseAPIClient
    participant Exchange as Exchange API
    participant Caller as Engine/Handler
    Caller->>BaseAPI: Send request
    BaseAPI->>Exchange: Make API call
    Exchange-->>BaseAPI: Return response
    BaseAPI-->>Caller: Return data/result
    BaseAPI->>BaseAPI: Handle errors/authentication
    Caller->>BaseAPI: Close connection
```

**Summary:**
- Inputs: API requests, authentication/configuration data.
- Outputs: API responses, error reports, connection management.
- Dependencies: Exchange APIs, callers for requests.
- Critical Path: Reliable and secure API communication is essential for all trading and data operations.

---

## hyperliquid.py
**Purpose:**
Implements the Hyperliquid exchange API client, supporting authentication (EIP-712), REST/WebSocket communication, order management, and data retrieval. Handles all trading, data, and account operations for Hyperliquid.

```mermaid
flowchart TD
    A[Init Client] --> B[Authenticate]
    B --> C[Connect REST/WS]
    C --> D[Send/Receive Requests]
    D --> E[Parse Responses]
    E --> F[Manage Orders/Positions]
    F --> G[Handle Errors]
```

```mermaid
sequenceDiagram
    participant HLAPI as HyperliquidAPI
    participant HL as Hyperliquid Exchange
    participant Engine as Engine/Handler
    Engine->>HLAPI: Place order/request data
    HLAPI->>HL: REST/WS call
    HL-->>HLAPI: Response/data
    HLAPI-->>Engine: Return result
    HLAPI->>HLAPI: Handle errors, manage state
```

**Summary:**
- Inputs: API requests/orders, authentication/configuration data.
- Outputs: Data, order status, error reports.
- Dependencies: Hyperliquid exchange APIs, engine/handlers for requests.
- Critical Path: Reliable and secure Hyperliquid connectivity is essential for trading and data operations.

---

## backpack.py
**Purpose:**
Implements the Backpack exchange API client, supporting authentication (HMAC), REST/WebSocket communication, order management, and data retrieval. Handles all trading, data, and account operations for Backpack.

```mermaid
flowchart TD
    A[Init Client] --> B[Authenticate]
    B --> C[Connect REST/WS]
    C --> D[Send/Receive Requests]
    D --> E[Parse Responses]
    E --> F[Manage Orders/Positions]
    F --> G[Handle Errors]
```

```mermaid
sequenceDiagram
    participant BPAPI as BackpackAPI
    participant BP as Backpack Exchange
    participant Engine as Engine/Handler
    Engine->>BPAPI: Place order/request data
    BPAPI->>BP: REST/WS call
    BP-->>BPAPI: Response/data
    BPAPI-->>Engine: Return result
    BPAPI->>BPAPI: Handle errors, manage state
```

**Summary:**
- Inputs: API requests/orders, authentication/configuration data.
- Outputs: Data, order status, error reports.
- Dependencies: Backpack exchange APIs, engine/handlers for requests.
- Critical Path: Reliable and secure Backpack connectivity is essential for trading and data operations.

---

## backpack_old.py
**Purpose:**
Implements a legacy or deprecated version of the BackpackAPI client for backward compatibility, migration, or reference. Similar logic to backpack.py but may use older protocols or data structures.

```mermaid
flowchart TD
    A[Init BackpackOldAPI] --> B[Authenticate]
    B --> C[Connect REST/WS]
    C --> D[Send/Receive Requests]
    D --> E[Parse Responses]
    E --> F[Manage Orders/Positions]
    F --> G[Handle Errors]
```

```mermaid
sequenceDiagram
    participant BPAPI as BackpackOldAPI
    participant BP as Backpack Exchange
    participant Engine as Engine/Handler
    Engine->>BPAPI: Place order/request data
    BPAPI->>BP: REST/WS call
    BP-->>BPAPI: Response/data
    BPAPI-->>Engine: Return result
    BPAPI->>BPAPI: Handle errors, manage state
```

**Summary:**
- Inputs: API requests/orders, authentication/configuration data.
- Outputs: Data, order status, error reports.
- Dependencies: Backpack exchange APIs, engine/handlers for requests.
- Critical Path: Reliable and secure Backpack connectivity is essential for trading and data operations.

---

## rate_limiter.py
**Purpose:**
Implements a token bucket rate limiter for API clients, enforcing rate limits on outgoing requests to exchanges. Tracks request counts, enforces cooldowns, and provides async/sync interfaces for use by API clients.

```mermaid
flowchart TD
    A[Init RateLimiter] --> B[Track Requests]
    B --> C[Check Rate Limit]
    C --> D[Enforce Cooldown/Delay]
    D --> E[Allow or Block Request]
    E --> F[Handle Errors/Edge Cases]
```

```mermaid
sequenceDiagram
    participant RL as RateLimiter
    participant API as API Client
    API->>RL: Request permission to send
    RL->>RL: Track/check rate
    RL-->>API: Allow/block request
    RL->>RL: Enforce cooldown/delay
```

**Summary:**
- Inputs: Request permission calls from API clients.
- Outputs: Allow/block signals, enforced delays.
- Dependencies: API clients for integration, internal state for tracking.
- Critical Path: Proper rate limiting is essential to avoid API bans and ensure reliable operation.

---

## errors.py
**Purpose:**
Currently empty. Placeholder for future custom error classes or constants for use by API clients. Centralizes error definitions for consistent exception handling and reporting.
