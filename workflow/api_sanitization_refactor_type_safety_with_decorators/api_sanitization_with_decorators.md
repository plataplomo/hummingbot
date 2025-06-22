# API Sanitization with Decorators - CyberDeltaEngine
## Security-Enhanced Decorator Framework for Type-Safe API Transformations

**Document Type**: Security + Type Safety Enhancement Strategy  
**Date**: 2025-06-22  
**Classification**: TECHNICAL IMPLEMENTATION GUIDE  
**Status**: RECOMMENDED APPROACH

---

## Executive Summary

The API sanitization security framework can be brilliantly enhanced with decorators that combine the type safety benefits from `ParsedJsonResponse_with_decorators.md` with the security hardening requirements from the API sanitization analysis. This approach transforms manual security validation into automatic, enforceable patterns that cannot be bypassed.

**Key Innovation**: Security-focused decorators that enforce Pydantic validation while providing the same developer experience benefits as the type safety decorators.

---

## Integration Overview

### **Combining Two Powerful Approaches**

1. **Type Safety Decorators** (from ParsedJsonResponse analysis)
   - Eliminate manual type checking boilerplate
   - Provide compile-time type guarantees
   - Enable automatic validation of HTTP responses

2. **Security Sanitization** (from API sanitization analysis)
   - Prevent Pydantic validation bypass vulnerabilities
   - Enforce secure transformation patterns
   - Add comprehensive audit trails and monitoring

3. **Combined Approach** = **Ultimate API Security + Developer Experience**

---

## Security-Enhanced Decorator Framework

### **1. Security-Focused Core Decorators**

```python
# cyberdelta/apis/decorators/security_decorators.py
from functools import wraps
from typing import TypeVar, Type, Dict, Any, Optional
from pydantic import BaseModel, ValidationError
import logging
import hashlib
from datetime import datetime, UTC

T = TypeVar('T', bound=BaseModel)

class TransformationError(Exception):
    """Critical security error in data transformation"""
    pass

def secure_transform(
    target_model: Type[T],
    context: str | None = None,
    enable_monitoring: bool = True,
    enable_audit: bool = False,
    source_exchange: str | None = None
):
    """
    Security-first decorator that enforces Pydantic validation and logging.
    Replaces the manual secure_transform utility with automatic decoration.
    
    Args:
        target_model: Pydantic model class for validation
        context: Description for security logging
        enable_monitoring: Enable security event logging
        enable_audit: Enable cryptographic audit trail
        source_exchange: Exchange name for security context
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Call original mapper logic to build data dict
            transformation_data = await func(self, *args, **kwargs)
            
            # Build context for security logging
            method_context = context or f"{func.__name__}_{target_model.__name__}"
            exchange_context = source_exchange or getattr(self, '_exchange_name', 'unknown')
            
            # Security logging
            if enable_monitoring:
                logger.info(f"SECURITY: Secure transformation attempt: {method_context} from {exchange_context}")
            
            try:
                # ENFORCE model_validate() - prevents bypass vulnerability
                validated_model = target_model.model_validate(transformation_data)
                
                # Success logging
                if enable_monitoring:
                    logger.debug(f"SECURITY: Validation successful: {method_context}")
                
                # Optional audit trail
                if enable_audit:
                    _create_audit_record(transformation_data, validated_model, method_context)
                
                return validated_model
                
            except ValidationError as e:
                # SECURITY ALERT - potential attack attempt
                logger.error(f"SECURITY ALERT: Validation failed in {method_context} from {exchange_context}: {e}")
                raise TransformationError(f"Security validation failed for {target_model.__name__}: {e}")
                
        return wrapper
    return decorator

def _create_audit_record(source_data: Dict[str, Any], target_model: BaseModel, context: str):
    """Create cryptographic audit record for security monitoring"""
    source_hash = hashlib.sha256(str(source_data).encode()).hexdigest()
    target_hash = hashlib.sha256(target_model.model_dump_json().encode()).hexdigest()
    
    audit_record = {
        "timestamp": datetime.now(UTC).isoformat(),
        "context": context,
        "source_hash": source_hash,
        "target_hash": target_hash,
        "validation_result": "success"
    }
    
    logger.info(f"AUDIT: {audit_record}")
```

### **2. Business Logic Security Decorators**

```python
def business_logic_validated(
    constraints: Dict[str, Dict[str, Any]] | None = None,
    financial_fields: list[str] | None = None
):
    """
    Decorator that adds business logic constraints to prevent financial exploitation.
    
    Args:
        constraints: Field-specific validation rules
        financial_fields: Fields that must be non-negative
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Get transformation data
            data = await func(self, *args, **kwargs)
            
            # Apply business logic validation
            if financial_fields:
                for field in financial_fields:
                    if field in data:
                        try:
                            value = float(str(data[field]))
                            if value < 0:
                                raise ValueError(f"Financial field {field} cannot be negative: {value}")
                        except (ValueError, TypeError) as e:
                            if "cannot be negative" not in str(e):
                                raise ValueError(f"Financial field {field} must be numeric: {data[field]}")
                            raise
            
            # Apply custom constraints
            if constraints:
                for field, rules in constraints.items():
                    if field in data:
                        value = data[field]
                        
                        if "min" in rules:
                            if float(str(value)) < rules["min"]:
                                raise ValueError(f"Field {field} below minimum {rules['min']}: {value}")
                        
                        if "max" in rules:
                            if float(str(value)) > rules["max"]:
                                raise ValueError(f"Field {field} exceeds maximum {rules['max']}: {value}")
            
            return data
            
        return wrapper
    return decorator

def security_monitored(
    alert_on_negative: bool = True,
    alert_on_oversized: bool = True,
    anomaly_detection: bool = False,
    max_field_count: int = 100
):
    """
    Decorator for real-time security monitoring and anomaly detection.
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            data = await func(self, *args, **kwargs)
            
            # Anomaly detection
            anomalies = []
            
            if alert_on_negative:
                for field, value in data.items():
                    if isinstance(value, (str, int, float)):
                        try:
                            if float(str(value)) < 0 and field in ["price", "quantity", "balance", "total", "available"]:
                                anomalies.append(f"negative_value_in_{field}")
                        except (ValueError, TypeError):
                            pass
            
            if alert_on_oversized:
                if len(data) > max_field_count:
                    anomalies.append(f"oversized_structure_{len(data)}_fields")
                
                for field, value in data.items():
                    if isinstance(value, str) and len(value) > 1000:
                        anomalies.append(f"oversized_string_in_{field}")
            
            # Log anomalies
            if anomalies:
                logger.warning(f"SECURITY ANOMALY: {anomalies} in {func.__name__}")
            
            return data
            
        return wrapper
    return decorator
```

### **3. Combined Security + Type Safety Decorators**

```python
def secure_mapped_response(
    raw_model: Type[BaseModel],
    target_model: Type[BaseModel], 
    mapper_method: str,
    enable_security_monitoring: bool = True,
    enable_audit_trail: bool = False,
    business_constraints: Dict[str, Any] | None = None
):
    """
    Ultimate decorator combining HTTP type safety + security validation + mapping.
    
    This decorator handles the complete flow:
    1. HTTP request with typed response validation
    2. Security monitoring and business logic validation  
    3. Secure transformation to target model
    4. Automatic mapper method invocation
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Step 1: Execute HTTP request
            raw_response_content, status_code, headers = await func(self, *args, **kwargs)
            
            # Step 2: Validate HTTP response as raw model
            if raw_response_content is None:
                return None
                
            if not isinstance(raw_response_content, dict):
                raise APIError(
                    message=f"Expected dict for {raw_model.__name__}, got {type(raw_response_content).__name__}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                )
            
            try:
                validated_raw = raw_model.model_validate(raw_response_content)
            except ValidationError as e:
                raise APIError(
                    message=f"Raw model validation failed for {raw_model.__name__}: {e}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code
                ) from e
            
            # Step 3: Get mapper and apply security validation
            mapper = getattr(self, '_market_data_mapper', None)
            if not mapper or not hasattr(mapper, mapper_method):
                raise AttributeError(f"Mapper method {mapper_method} not found")
            
            # Step 4: Execute mapper with security decorators
            mapper_func = getattr(mapper, mapper_method)
            
            # Apply security validation if enabled
            if enable_security_monitoring:
                @security_monitored(alert_on_negative=True, alert_on_oversized=True)
                @business_logic_validated(constraints=business_constraints)
                async def secure_mapper_call():
                    return await mapper_func(validated_raw)
                
                return await secure_mapper_call()
            else:
                return await mapper_func(validated_raw)
                
        return wrapper
    return decorator
```

---

## Enhanced Mapper Implementation Patterns

### **2. Enhanced Mapper Pattern**

```python
# Example: cyberdelta/apis/backpack/mappers/bp_account_data_mapper.py
from cyberdelta.apis.decorators.security_decorators import secure_transform, business_logic_validated, security_monitored

class BackpackAccountDataMapper:
    """Security-hardened mapper using decorators"""
    
    # BEFORE (vulnerable):
    # def transform_raw_balance_to_internal(...) -> SpotBalance:
    #     return SpotBalance(asset=asset, total_quantity=total, ...)  # VALIDATION BYPASS!
    
    # AFTER (secure with decorators):
    @security_monitored(alert_on_negative=True, alert_on_oversized=True)
    @business_logic_validated(
        financial_fields=["total_quantity", "available_quantity"],
        constraints={
            "total_quantity": {"min": 0, "max": 1000000000},
            "available_quantity": {"min": 0, "max": 1000000000}
        }
    )
    @secure_transform(
        target_model=SpotBalance,
        context="balance_transformation",
        enable_monitoring=True,
        enable_audit=True,
        source_exchange="backpack"
    )
    async def transform_raw_balance_to_internal(
        self,
        asset_symbol: str,
        raw: BackpackRawBalance,
    ) -> Dict[str, Any]:  # Returns data dict for validation
        """
        Transform raw balance - security enforced by decorator stack.
        
        Decorator stack provides:
        1. Security monitoring for anomalies
        2. Business logic validation (non-negative financial values)
        3. Secure transformation with Pydantic validation
        4. Audit trail logging
        """
        
        # Keep existing parsing logic (this part was already secure)
        parsed_available = parse_decimal_value(raw.available, allow_none=False, field_name="available")
        parsed_locked = parse_decimal_value(raw.locked, allow_none=False, field_name="locked")
        parsed_staked = parse_decimal_value(raw.staked, allow_none=False, field_name="staked") if raw.staked else Decimal('0')
        parsed_total = parsed_available + parsed_locked + parsed_staked
        
        # Create BackpackSpotBalanceDetails if needed
        details = None
        if raw.staked is not None:
            details = BackpackSpotBalanceDetails(
                locked_balance=parsed_locked,
                staked_balance=parsed_staked
            )
        
        # Return data dict - decorator stack will:
        # 1. Monitor for security anomalies
        # 2. Validate business logic constraints  
        # 3. Call SpotBalance.model_validate() securely
        # 4. Log audit trail
        return {
            "asset": asset_symbol.upper(),
            "exchange": ExchangeName.BACKPACK.value,
            "total_quantity": str(parsed_total),
            "available_quantity": str(parsed_available),
            "timestamp": datetime.now(UTC).isoformat(),
            "bp_details": details.model_dump() if details else None,
        }
        # All security validation happens automatically via decorator stack
```

### **3. Service Layer Integration**

```python
# Example: cyberdelta/apis/backpack/services/bp_market_data_service.py
class BackpackMarketDataService:
    """Service layer with combined type safety + security"""
    
    # Style 1: Maximum security with full decorator stack
    @secure_mapped_response(
        raw_model=BackpackRawTicker,
        target_model=Ticker,
        mapper_method='transform_raw_ticker_to_internal',
        enable_security_monitoring=True,
        enable_audit_trail=True,
        business_constraints={
            "last_price": {"min": 0},
            "volume": {"min": 0}
        }
    )
    async def get_ticker_secure(self, symbol: str) -> Ticker | None:
        """
        Ultimate security: HTTP validation + business logic + secure mapping + audit trail.
        
        Single decorator handles:
        1. HTTP request execution
        2. BackpackRawTicker validation
        3. Security monitoring (negative values, anomalies)
        4. Business logic validation (positive prices/volumes)
        5. Secure mapper transformation
        6. Audit trail logging
        7. Return validated Ticker object
        """
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/ticker",
            params={"symbol": symbol},
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )
        # Everything else handled by decorator automatically!
    
    # Style 2: Existing method enhanced with security
    @business_logic_validated(financial_fields=["last_price", "volume"])
    @dict_response(BackpackRawTicker, allow_none=True)
    async def get_ticker_raw_secure(self, symbol: str) -> BackpackRawTicker | None:
        """Existing pattern enhanced with business logic validation"""
        return await self._http_client_requester(
            method="GET",
            endpoint="/api/v1/ticker", 
            params={"symbol": symbol},
            is_signed=False
        )
    
    # Style 3: Full control with manual security
    async def get_ticker_manual_security(self, symbol: str) -> Ticker | None:
        """Manual approach when fine-grained control needed"""
        
        # Get raw data with type safety
        raw_ticker = await self.get_ticker_raw_secure(symbol)
        if not raw_ticker:
            return None
        
        # Apply security-enhanced mapper
        return await self._market_data_mapper.transform_raw_ticker_to_internal(raw_ticker)
```

---

## Advanced Security Features

### **4. Decorator Composition for Maximum Security**

```python
# Ultimate security decorator stack
@retry_on_failure(max_attempts=3)
@rate_limited(calls_per_minute=60)
@security_monitored(
    alert_on_negative=True,
    alert_on_oversized=True,
    anomaly_detection=True
)
@business_logic_validated(
    financial_fields=["price", "quantity", "total_value"],
    constraints={
        "price": {"min": 0.001, "max": 1000000},
        "quantity": {"min": 0, "max": 1000000}
    }
)
@secure_transform(
    target_model=Order,
    enable_monitoring=True,
    enable_audit=True
)
@cache_result(ttl_seconds=30)
async def transform_order_ultra_secure(self, raw_order: BackpackRawOrder) -> Dict[str, Any]:
    """
    Ultra-secure order transformation with full decorator stack:
    - Retry logic for resilience
    - Rate limiting for DoS protection  
    - Security monitoring for anomalies
    - Business logic validation for financial constraints
    - Secure transformation with audit trail
    - Result caching for performance
    """
    # Implementation returns data dict
    # All security, validation, monitoring handled by decorators
```

### **5. Audit Trail Integration**

```python
@audit_trail(
    signature_key="transformation_audit_key",
    include_source_hash=True,
    include_target_hash=True,
    cryptographic_signing=True
)
@secure_transform(target_model=SpotBalance, enable_audit=True)
async def transform_balance_with_audit(self, raw: BackpackRawBalance) -> Dict[str, Any]:
    """Transformation with cryptographic audit trail"""
    # Decorator automatically creates signed audit record:
    # {
    #   "timestamp": "2025-06-22T10:00:00Z",
    #   "source_hash": "abc123...",
    #   "target_hash": "def456...", 
    #   "transformation_id": "unique_id",
    #   "signature": "hmac_signature",
    #   "validation_result": "success"
    # }
```

---

## Implementation Benefits

### **1. Security Benefits**
- **100% Validation Coverage**: Impossible to bypass security - decorators enforce it
- **Automatic Security Monitoring**: Real-time anomaly detection built-in
- **Cryptographic Audit Trails**: Complete transformation history with HMAC signatures
- **Business Logic Enforcement**: Financial constraints automatically applied
- **Attack Prevention**: Negative values, oversized data, type confusion all blocked

### **2. Developer Experience Benefits**
- **Zero Boilerplate**: Security becomes automatic, not manual
- **Composable Security**: Layer multiple security features via decorator stacking
- **Type Safety + Security**: Combined approach addresses both concerns simultaneously
- **IDE Support**: Full autocomplete and type checking maintained
- **Gradual Migration**: Can be applied incrementally to existing mappers

### **3. Code Quality Benefits**
- **Single Responsibility**: Each decorator handles one security concern
- **Testability**: Each decorator can be tested independently
- **Maintainability**: Security logic centralized in reusable decorators
- **Consistency**: Uniform security patterns across all mappers
- **Documentation**: Security requirements self-documenting via decorator names

---

## Migration Strategy

### **Phase 1: Core Security Decorators (Week 1)**
1. Implement `@secure_transform` decorator
2. Create `@business_logic_validated` decorator
3. Add `@security_monitored` decorator
4. Test with pilot mapper methods

### **Phase 2: Service Layer Integration (Week 2)**
1. Implement `@secure_mapped_response` decorator
2. Enhance existing service methods with security decorators
3. Create comprehensive security test suite
4. Deploy to staging environment

### **Phase 3: Full Migration (Week 3-4)**
1. Apply security decorators to all mapper methods
2. Migrate all service methods to enhanced patterns
3. Enable comprehensive audit trails
4. Deploy to production with monitoring

### **Phase 4: Advanced Features (Month 2)**
1. Add cryptographic audit trail decorators
2. Implement AI-powered anomaly detection
3. Create real-time security dashboards
4. Establish automated incident response

---

## Success Metrics

### **Security Metrics**
- **Validation Coverage**: 100% of transformations use secure validation
- **Attack Prevention**: Zero successful validation bypass attempts
- **Anomaly Detection**: <1 minute from detection to alert
- **Audit Coverage**: 100% of financial transformations audited

### **Performance Metrics**
- **Decorator Overhead**: <2ms per transformation
- **Memory Usage**: No increase in baseline consumption
- **Throughput**: No degradation in transaction processing
- **Error Rate**: <0.1% false positive security alerts

---

## Conclusion

The decorator-based API sanitization approach represents the ultimate evolution of the CyberDeltaEngine security architecture. By combining type safety decorators with security-focused validation, this solution provides:

1. **Unbreakable Security**: Validation bypass becomes impossible
2. **Zero Boilerplate**: Security and type safety become automatic
3. **Maximum Developer Experience**: Single-line methods with comprehensive protection
4. **Regulatory Compliance**: Built-in audit trails and monitoring
5. **Financial Protection**: Business logic constraints prevent exploitation

This approach transforms the manual `secure_transform()` utility pattern into automatic, enforceable security that integrates seamlessly with the type safety enhancements. Each mapper method becomes a single line of business logic with comprehensive security handled transparently through the decorator stack.

**Result**: **Type-Safe + Security-Hardened + Zero-Boilerplate + Audit-Ready** API layer that provides maximum protection with minimal complexity.

---

## Implementation Todo List

### **High Priority Security Tasks**
1. **Core Security Framework** - Implement `@secure_transform`, `@business_logic_validated`, `@security_monitored` decorators
2. **Service Integration** - Create `@secure_mapped_response` decorator for end-to-end security
3. **Pilot Migration** - Apply security decorators to BackpackAccountDataMapper methods
4. **Security Testing** - Create comprehensive security test suite for all decorator patterns
5. **Static Analysis** - Ensure all security decorator code passes ruff/mypy validation

### **Medium Priority Tasks**
6. **Audit Trail Implementation** - Add `@audit_trail` decorator with cryptographic signing
7. **Anomaly Detection** - Enhance `@security_monitored` with AI-powered pattern detection
8. **Full Mapper Migration** - Apply security decorators to all mapper classes
9. **Performance Testing** - Validate decorator overhead stays under 2ms per transformation

### **Low Priority Tasks**
10. **Advanced Monitoring** - Create real-time security dashboards and automated incident response

This decorator-based security approach truly represents the best possible solution for securing the CyberDeltaEngine API layer while maintaining exceptional developer experience!