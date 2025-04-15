
# Code Relationships

## Core Components Overview
```mermaid
graph TD
    subgraph ApplicationEntry["Application Entry (main.py)"]
        Main[main.py]
    end

    subgraph UserSystem ["User/System"]
        ConfigFile[Config File] --> Main
        SecretsFile[Secrets File] --> Main
        InputData[Market Data]
        Output[Signals_Orders_Logs]
        Engine --> Output
    end

    subgraph CoreEngine
        Engine
        DataHandler
        StrategyManager
        SignalGenerator
        SignalQueue
        RiskManager
        ExecutionHandler
        PortfolioTracker
        BalanceMonitor
        StateManager
        Strategy(Strategy ABC)
        FundingRateArbitrageStrategy
        ArbitrageOpportunity[Arbitrage Opportunity]
        SizedOpportunity[Sized Opportunity]
        Balance[Balance]
        Position[Position]
        Order[Order]
        MarketData[Market Data]
        FundingRate[Funding Rate]
    end

    subgraph ExternalSystems
        APIs(Exchange APIs)
        ValidationSystems[Validation Systems]
        MonitoringSystems[Monitoring Systems]
    end

    StateFile[engine_state.json]

    Main -- Creates_Configures --> StateManager
    Main -- Creates_Configures --> Config((load_config))
    Main -- Creates_Configures --> PortfolioTracker
    Main -- Creates_Configures --> ValidationSystems
    Main -- Creates_Configures --> ExecutionHandler
    Main -- Creates_Configures --> RiskManager
    Main -- Creates_Configures --> SignalQueue
    Main -- Creates_Configures --> Engine
    Main -- Creates_Configures --> DataHandler
    Main -- Creates_Uses --> APIs
    Main -- Creates_Configures --> FundingRateArbitrageStrategy

    Engine -- Gets_Data_Updates --> DataHandler
    Engine -- Manages --> StrategyManager
    Engine -- Sends_Signals --> SignalQueue
    Engine -- Gets_State --> PortfolioTracker
    Engine -- Gets_State --> BalanceMonitor

    StrategyManager -- Manages --> Strategy
    FundingRateArbitrageStrategy --> Strategy

    DataHandler -- Notifies_Observer --> Engine
    DataHandler -- Subscribes_Fetches --> APIs
    DataHandler -- Provides_Data --> SignalGenerator
    InputData --> DataHandler

    SignalGenerator -- Uses_Data --> DataHandler
    SignalGenerator -- Generates --> ArbitrageOpportunity
    SignalGenerator -- Enqueues_Signal --> SignalQueue

    SignalQueue -- Sends_Signal --> RiskManager

    RiskManager -- Sizes --> ArbitrageOpportunity
    RiskManager -- Creates --> SizedOpportunity
    RiskManager -- Gets_State --> PortfolioTracker
    RiskManager -- Sends_Order --> ExecutionHandler
    RiskManager --> ValidationSystems

    ExecutionHandler -- Executes --> SizedOpportunity
    ExecutionHandler -- Updates_State --> PortfolioTracker
    ExecutionHandler -- Places_Orders --> APIs
    ExecutionHandler --> ValidationSystems

    PortfolioTracker -- Tracks --> Balance
    PortfolioTracker -- Tracks --> Position
    PortfolioTracker -- Tracks --> Order
    PortfolioTracker -- Fetches_Data --> APIs
    PortfolioTracker --> ValidationSystems
    PortfolioTracker -- Load_Save_State --> StateManager

    BalanceMonitor --> PortfolioTracker

    StateManager -- Handles --> StateFile

    Engine -- Reports_to --> MonitoringSystems

    FundingRateArbitrageStrategy -- Uses --> MarketData
    FundingRateArbitrageStrategy -- Uses --> FundingRate
```

## API Class Hierarchy
```mermaid
classDiagram
    direction LR

    class Ticker
    class OrderBook
    class Balance
    class Position
    class Order
    class APIError
    class APIErrorCode

    class ExchangeAPI {
        <<Abstract>>
        +str exchange_name
        +connect() None
        +close() None
        +get_ticker(str) Ticker
        +get_order_book(str) OrderBook
        +get_balances() dict<str, Balance>
        +get_positions(str) list<Position>
        +place_order(...) Order
        +cancel_order(str) dict
        +get_open_orders(str) list<Order>
        #_request(str, str, ...)
        #_map_error_response(...) APIError
        #_authenticate(...)*
        #_sign_request(...)*
    }
    class BackpackAPI {
        +exchange_name = "Backpack"
        #_sign_request(...)
        #_map_error_response(...)
    }
    class HyperliquidAPI {
        +exchange_name = "Hyperliquid"
        #_authenticate(...)
        #_map_error_response(...)
    }

    ExchangeAPI <|-- BackpackAPI
    ExchangeAPI <|-- HyperliquidAPI
    ExchangeAPI ..> APIError : Creates_Uses
    APIError ..> APIErrorCode : Uses
    ExchangeAPI ..> Ticker : Returns
    ExchangeAPI ..> OrderBook : Returns
    ExchangeAPI ..> Balance : Returns
    ExchangeAPI ..> Position : Returns
    ExchangeAPI ..> Order : Returns_Uses
```

## Strategy Management
```mermaid
graph TD
    subgraph Core
        StrategyManager
        Engine
        SignalQueue
        Strategy(Strategy ABC)
        FundingRateArbitrageStrategy
    end
    subgraph Data
         MarketData[Market Data]
         FundingRate[Funding Rate]
         Ticker[Ticker Data]
    end
    subgraph Signals
        TradeSignal[Trade Signal]
        ArbitrageOpportunity[Arbitrage Opportunity]
    end

    StrategyManager --> Strategy
    Strategy --> TradeSignal
    StrategyManager -- Manages --> FundingRateArbitrageStrategy
    FundingRateArbitrageStrategy --> Strategy
    FundingRateArbitrageStrategy -- Uses --> MarketData
    FundingRateArbitrageStrategy -- Uses --> FundingRate
    FundingRateArbitrageStrategy -- Uses --> Ticker
    FundingRateArbitrageStrategy -- Creates --> ArbitrageOpportunity
    FundingRateArbitrageStrategy -- Creates --> TradeSignal
    Engine -- Routes_MarketData --> StrategyManager
    StrategyManager -- Sends_Signals --> SignalQueue
```

## Opportunity Execution Data Flow (Simplified Sequence)
```mermaid
sequenceDiagram
    participant SG as SignalGenerator
    participant SQ as SignalQueue
    participant RM as RiskManager
    participant EH as ExecutionHandler
    participant PT as PortfolioTracker
    participant API as ExchangeAPI
    participant CB as CircuitBreakerSystem

    SG ->> SQ: Add Signal (from Opportunity)
    SQ ->> RM: Get Next Signal
    activate RM
    RM ->> CB: Check Breakers
    activate CB
    CB -->> RM: Status OK
    deactivate CB
    RM ->> RM: Size Opportunity
    RM ->> PT: Get Balances/Positions
    activate PT
    PT -->> RM: Portfolio State
    deactivate PT
    RM ->> EH: Execute Sized Opportunity
    deactivate RM

    activate EH
    EH ->> CB: Check Breakers (Pre-Exec)
    activate CB
    CB -->> EH: Status OK
    deactivate CB
    EH ->> API: Place Order (Leg 1)
    activate API
    API -->> EH: Order Ack/Fill
    deactivate API
    EH ->> API: Place Order (Leg 2)
    activate API
    API -->> EH: Order Ack/Fill
    deactivate API
    EH ->> EH: Monitor Fills / Verify Execution
    EH ->> PT: Update Portfolio (Trades/Orders)
    activate PT
    PT -->> EH: Update Ack
    deactivate PT
    EH ->> CB: Record Success/Failure
    activate CB
    deactivate CB
    deactivate EH
```

## Configuration Management
```mermaid
graph TD
    subgraph Input
        ConfigFile[config.yaml]
        SecretsFile[secrets.yaml]
        EnvVars[(Environment Variables)]
    end
    subgraph Processing
        Main[main.py]
        UtilConfig(utils.config.load_config)
        SecretsManager(config.SecretsManager)
    end
    subgraph OutputData
       ConfigObject((Config Data))
       SecretsObject((Secrets Data))
    end
    subgraph Consumers
        CoreComponent[Core Components]
    end

    Main -- Uses --> UtilConfig
    UtilConfig -- Creates --> ConfigObject
    UtilConfig -- Reads --> ConfigFile
    UtilConfig -- Reads --> EnvVars

    Main -- Uses --> SecretsManager
    SecretsManager -- Loads --> SecretsFile
    SecretsManager -- Reads --> EnvVars
    SecretsManager --> SecretsObject

    Main -- Gets_Data --> ConfigObject
    Main -- Gets_Data --> SecretsObject
    Main -- Provides_Config_Secrets --> CoreComponent
```

## Validation Systems
```mermaid
graph TD
    subgraph EntryConfig
        Main[main.py]
    end
    subgraph Validation
        CBS[CircuitBreakerSystem]
        PRS[PositionReconciliationSystem]
        FRV[FundingRateValidator]
        MTFP[MultiTierFundingProvider]
    end
    subgraph Core
       RM[RiskManager]
       EH[ExecutionHandler]
       PT[PortfolioTracker]
       SG[SignalGenerator]
       Engine
    end
    subgraph External
       ExchangeAPI(Exchange API)
    end

    Main -- Creates_Configures --> CBS
    RM -- Check_Breakers --> CBS
    EH -- Check_Record_Breakers --> CBS
    RM -- Get_Validation_Factor --> FRV
    Engine -- Periodically_Runs --> PRS
    PRS -- Reads_Local_State --> PT
    PRS -- Reads_Exchange_State --> ExchangeAPI
    SG -- Get_Funding_Rate --> MTFP
    MTFP -- Records_Prediction_Payment --> FRV
    MTFP -- Fetch_Rates --> ExchangeAPI
```

