```mermaid
graph TD
    %% Main node styles (high readability)
    classDef main fill:#f5deb3,stroke:#000,stroke-width:2px,color:black
    classDef core fill:#d4f1f9,stroke:#000,stroke-width:1px,color:black
    classDef data fill:#e0f0d0,stroke:#000,stroke-width:1px,color:black
    classDef api fill:#ffe0e0,stroke:#000,stroke-width:1px,color:black
    classDef support fill:#e6e6fa,stroke:#000,stroke-width:1px,stroke-dasharray: 5 5,color:black
    classDef future fill:#e0e0e0,stroke:#000,stroke-width:1px,stroke-dasharray: 5 5,color:black

    %% External Actor
    User[Trader]:::main

    %% Main Application Orchestration
    Main[main.py]:::main --> Config[Configuration]
    Main --> Logger[Logging]
    Main --> MainLoop[Main Loop]

    %% Portfolio State (Defined early)
    PortfolioTracker[Portfolio Tracker]:::core

    %% Core Components - Data Layer
    subgraph DataLayer[Data Layer]
        direction TB
        DataHandler[Data Handler]:::core
        WebSocketManager[WebSocket Manager]:::core
        DataCache[Data Cache]:::core
        DataNormalizer[Data Normalizer]:::core
    end

    %% Core Components - Strategy Layer
    subgraph StrategyLayer[Strategy Layer]
        direction TB
        SignalGenerator["Signal Generator (inc FRS Logic)"]:::core
        %% Removed period in label just in case
        OpportunityRanker[Opportunity Ranker]:::core
        RiskManager[Risk Manager]:::core
    end

    %% Core Components - Execution Layer
    subgraph ExecutionLayer[Execution Layer]
        direction TB
        ExecutionHandler[Execution Handler]:::core
        OrderMonitor[Order Monitor]:::core
        CollateralManager[Collateral Manager]:::core
    end

    %% External Connections - Exchange API Clients
    subgraph ExchangeAPIClients[Exchange API Clients]
        direction TB
        ExchangeAPIBase[Exchange API Base]:::api
        subgraph HyperliquidGroup[Hyperliquid]
            HyperliquidAPI[Hyperliquid API Client]:::api
            HyperliquidREST[REST Client]:::api
            HyperliquidWS[WebSocket Client]:::api
            HyperliquidAuth[EIP-712 Auth]:::api
        end
        subgraph BackpackGroup[Backpack]
            BackpackAPI[Backpack API Client]:::api
            BackpackREST[REST Client]:::api
            BackpackWS[WebSocket Client]:::api
            BackpackAuth[ED25519 Auth]:::api
        end
        subgraph ParadexGroup[Paradex]
            ParadexAPI[Paradex API Client]:::api
            ParadexREST[REST Client]:::api
            ParadexWS[WebSocket Client]:::api
            ParadexAuth["Starknet Auth?"]:::api
        end
    end

    %% External Connections - External Services (Simplified view)
    subgraph ExternalServices[External Services]
        direction TB
        ETHWallet[ETH Wallet]:::api
        KeyManager[Key Manager]:::api
    end

    %% External Connections - Support Systems (Simplified view)
    subgraph SupportSystems[Support Systems]
        direction TB
        ConfigManager[Config Manager]:::support
        LoggingSystem[Logging System]:::support
    end

    %% === Layout Hints using Invisible Links ===
    %% Try to force PortfolioTracker below Main but above the core layers
    Main ~~~ PortfolioTracker
    PortfolioTracker ~~~ DataHandler
    PortfolioTracker ~~~ SignalGenerator
    PortfolioTracker ~~~ ExecutionHandler
    %% You might also try linking the layers horizontally if needed
    %% DataHandler ~~~ SignalGenerator ~~~ ExecutionHandler

    %% === Define REAL Connections AFTER layout hints ===

    %% Internal Layer Connections
    DataHandler --> WebSocketManager
    DataHandler --> DataCache
    WebSocketManager --> DataCache
    DataHandler --> DataNormalizer
    DataNormalizer --> DataCache

    SignalGenerator --> OpportunityRanker
    OpportunityRanker --> RiskManager

    ExecutionHandler --> OrderMonitor
    ExecutionHandler --> CollateralManager

    %% Exchange API Client Relationships
    ExchangeAPIBase --> HyperliquidAPI
    ExchangeAPIBase --> BackpackAPI
    ExchangeAPIBase --> ParadexAPI
    HyperliquidAPI --> HyperliquidREST & HyperliquidWS & HyperliquidAuth
    BackpackAPI --> BackpackREST & BackpackWS & BackpackAuth
    ParadexAPI --> ParadexREST & ParadexWS & ParadexAuth

    %% WebSocket Handling
    WebSocketManager --> HyperliquidWS
    WebSocketManager --> BackpackWS
    WebSocketManager --> ParadexWS

    %% Funding Rate Strategy Flow (Overlay) - FIXED LABELS
    User -- "1 Init Bot" --> Main
    DataHandler -- "2a Fetch BP Data" --> BackpackAPI
    DataHandler -- "2b Fetch HL Data" --> HyperliquidAPI
    DataHandler -- "2c Fetch PX Data" --> ParadexAPI
    DataHandler -- "3 Feed Validated Data" --> SignalGenerator
    SignalGenerator -- "4 Calc Utility, Rank Opportunities" --> OpportunityRanker
    OpportunityRanker -- "5 Send Ranked Opps" --> RiskManager
    RiskManager -- "6a Get Current State" --> PortfolioTracker
    RiskManager -- "6b Check Constraints/VaR" --> RiskManager
    RiskManager -- "6c Determine Sizing (w_target)" --> RiskManager
    RiskManager -- "7 Trigger Collateral Check" --> CollateralManager
    CollateralManager -- "8a Query BP Collateral" --> BackpackAPI
    CollateralManager -- "8b Query HL Collateral" --> HyperliquidAPI
    CollateralManager -- "8c Query PX Collateral" --> ParadexAPI
    RiskManager -- "9 Send Sized/Viable Opp" --> ExecutionHandler
    ExecutionHandler -- "10a Execute Leg 1 (BP)" --> BackpackAPI
    ExecutionHandler -- "10b Execute Leg 2 (HL)" --> HyperliquidAPI
    ExecutionHandler -- "11a Monitor Fills" --> OrderMonitor
    OrderMonitor -- "11b Update Portfolio" --> PortfolioTracker
    PortfolioTracker -- "12 Report State/PnL" --> Main
    Main -- "13 Display Metrics" --> User

    %% Continuous Adaptation Loop (Implicit - Data flows back)
    PortfolioTracker -- "(Feedback Loop)" --> RiskManager
    PortfolioTracker -- "(Feedback Loop)" --> SignalGenerator
    DataHandler -- "(Feedback Loop)" --> SignalGenerator

    %% Connect Support Systems
    Main --> ConfigManager
    Main --> LoggingSystem
    CollateralManager --> ETHWallet
    BackpackAPI --> KeyManager

    %% Main component connections
    Main --> DataHandler
    Main --> SignalGenerator
    Main --> ExecutionHandler
    Main --> PortfolioTracker
