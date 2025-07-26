"""CyberDelta Unified Symbol System.

This package provides a comprehensive symbol management system with:
- Type-safe Pydantic models for symbol representation
- Thread-safe symbol registry with caching
- Exchange-specific symbol transformation
- Unified validation architecture
- Performance-optimized lookups

Version: 2.0
"""

from cyberdelta.core.symbols.exceptions import (
    SymbolCacheError,
    SymbolError,
    SymbolNotFoundError,
    SymbolRegistryError,
    SymbolValidationError,
)
from cyberdelta.core.symbols.helpers import (
    SymbolDomainHelpers,
    get_domain_helpers,
)
from cyberdelta.core.symbols.models import (
    BaseSymbol,
    ExchangeSymbol,
    InternalSymbol,
    SymbolFormat,
    SymbolType,
    UnifiedSymbol,
    create_exchange_symbol,
    create_internal_symbol,
)
from cyberdelta.core.symbols.operation_results import (
    SymbolArbitrageCompatibility,
    SymbolBatchTransformResult,
)
from cyberdelta.core.symbols.protocols import (
    SymbolStoreProtocol,
    SymbolTransformerProtocol,
)
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.core.symbols.store import SymbolStore
from cyberdelta.core.symbols.transformers import (
    SYMBOL_TRANSFORMERS,
    BackpackSymbolTransformer,
    BinanceSymbolTransformer,
    HyperliquidSymbolTransformer,
)
from cyberdelta.core.symbols.validators import (
    ArbitrageValidator,
    CrossExchangeValidator,
    DomainObjectValidator,
    SymbolValidator,
    get_validation_errors,
    validate_arbitrage_pair,
    validate_domain_object,
)


__all__ = [
    "SYMBOL_TRANSFORMERS",
    "ArbitrageValidator",
    "BackpackSymbolTransformer",
    # Models
    "BaseSymbol",
    "BinanceSymbolTransformer",
    "CrossExchangeValidator",
    "DomainObjectValidator",
    "ExchangeSymbol",
    # Transformers
    "HyperliquidSymbolTransformer",
    "InternalSymbol",
    "SymbolArbitrageCompatibility",
    # Operation Results
    "SymbolBatchTransformResult",
    "SymbolCacheError",
    # Domain Helpers
    "SymbolDomainHelpers",
    # Exceptions
    "SymbolError",
    "SymbolFormat",
    "SymbolNotFoundError",
    "SymbolRegistryError",
    # New DDD Architecture
    "SymbolService",
    "SymbolStore",
    "SymbolStoreProtocol",
    "SymbolTransformerProtocol",
    "SymbolType",
    "SymbolValidationError",
    # Validators
    "SymbolValidator",
    "UnifiedSymbol",
    "create_exchange_symbol",
    "create_internal_symbol",
    "get_domain_helpers",
    "get_validation_errors",
    "validate_arbitrage_pair",
    "validate_domain_object",
]

__version__ = "2.0.0"
