"""
Tests for the Enhanced Position Sizing components.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from cyberdelta.core.position_sizing import EnhancedPositionSizer
from cyberdelta.core.models import OrderSide
from cyberdelta.core.signal_generator import ArbitrageOpportunity


class TestEnhancedPositionSizer:
    """Test suite for the EnhancedPositionSizer class."""
    
    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.get.side_effect = lambda key, default=None: {
            'position_sizing.base_win_probability': 0.55,
            'position_sizing.base_kelly_fraction': 0.3,
            'position_sizing.confidence_adjustment_factor': 0.2,
            'position_sizing.high_confidence_threshold': 0.8,
            'position_sizing.max_kelly_fraction': 0.5,
            'position_sizing.max_position_size': 5000.0,
            'position_sizing.min_position_size': 100.0,
            'position_sizing.default_payoff_ratio': 1.5,
            'position_sizing.min_payoff_ratio': 1.1,
            'position_sizing.max_payoff_ratio': 5.0,
            'position_sizing.volatility_scaling_power': 1.5,
            'position_sizing.max_low_volatility_increase': 1.2,
            'position_sizing.minor_drawdown_threshold': 5.0,
            'position_sizing.moderate_drawdown_threshold': 15.0,
            'position_sizing.min_drawdown_factor': 0.1,
            'position_sizing.max_total_exposure': 0.8,
            'position_sizing.max_symbol_exposure': 0.2,
            'position_sizing.max_exchange_exposure': 0.5,
            'position_sizing.min_trade_size': 50.0,
            'position_sizing.correlation_threshold': 0.7,
            'position_sizing.min_correlation_factor': 0.5,
            'position_sizing.initial_position_size': 1000.0,
            'position_sizing.debug_position_sizing': False
        }.get(key, default)
        return config
    
    @pytest.fixture
    def portfolio_tracker(self):
        """Create a mock portfolio tracker for testing."""
        tracker = MagicMock()
        
        # Setup default return values
        tracker.get_total_portfolio_value.return_value = 100000.0
        tracker.get_current_drawdown.return_value = 0.0
        tracker.get_total_exposure.return_value = 30000.0
        tracker.get_symbol_exposure.return_value = 5000.0
        tracker.get_exchange_exposure.return_value = 20000.0
        tracker.get_active_symbols.return_value = ["BTC", "ETH"]
        
        return tracker
    
    @pytest.fixture
    def data_handler(self):
        """Create a mock data handler for testing."""
        handler = MagicMock()
        
        # Setup default return values
        handler.get_recent_volatility.return_value = 0.02  # 2% recent volatility
        handler.get_historical_volatility.return_value = 0.015  # 1.5% historical volatility
        
        # Setup correlation matrix
        correlation_matrix = {
            ("BTC", "ETH"): 0.6,
            ("BTC", "SOL"): 0.8,
            ("ETH", "SOL"): 0.7
        }
        handler.get_correlation_matrix.return_value = correlation_matrix
        
        return handler
    
    @pytest.fixture
    def funding_rate_validator(self):
        """Create a mock funding rate validator for testing."""
        validator = MagicMock()
        
        # Setup default return values
        validator.get_similar_trades.return_value = [
            {"profit": 100.0},
            {"profit": -20.0},
            {"profit": 80.0},
            {"profit": 50.0},
            {"profit": -10.0}
        ]
        
        validator.get_metrics.return_value = {
            "rmse": 0.001,
            "mae": 0.0008,
            "bias": 0.0002
        }
        
        return validator
    
    @pytest.fixture
    def exchange_adapters(self):
        """Create mock exchange adapters for testing."""
        adapters = {
            "hyperliquid": MagicMock(),
            "backpack": MagicMock()
        }
        
        # Setup default return values
        for exchange, adapter in adapters.items():
            adapter.get_min_order_size.return_value = 10.0
            adapter.get_max_order_size.return_value = 100000.0
            adapter.get_market_liquidity_factor.return_value = 0.9
        
        return adapters
    
    @pytest.fixture
    def position_sizer(self, config, portfolio_tracker, data_handler, funding_rate_validator, exchange_adapters):
        """Create an EnhancedPositionSizer instance for testing."""
        return EnhancedPositionSizer(
            config=config,
            portfolio_tracker=portfolio_tracker,
            data_handler=data_handler,
            funding_rate_validator=funding_rate_validator,
            exchange_adapters=exchange_adapters
        )
    
    @pytest.fixture
    def sample_signal(self):
        """Create a sample trade signal for testing."""
        signal = MagicMock()
        signal.exchange = "hyperliquid"
        signal.symbol = "BTC"
        signal.confidence = 0.85
        return signal
    
    def test_estimate_win_probability(self, position_sizer, sample_signal):
        """Test estimating win probability based on historical data."""
        # Normal case with sufficient historical data
        win_prob = position_sizer._estimate_win_probability(
            exchange="hyperliquid",
            symbol="BTC",
            funding_rate=0.01,
            confidence=0.85
        )
        
        # We expect 3/5 = 0.6 win rate from the mock data
        # After confidence adjustment: 0.6 * 0.85 = 0.51
        # After Bayesian shrinkage with weight min(1.0, 5/20) = 0.25:
        # Expected: 0.51 * 0.25 + 0.55 * 0.75 = 0.54
        assert 0.5 < win_prob < 0.6
        
        # Test with insufficient historical data
        position_sizer.funding_rate_validator.get_similar_trades.return_value = []
        win_prob_no_data = position_sizer._estimate_win_probability(
            exchange="hyperliquid",
            symbol="BTC",
            funding_rate=0.01,
            confidence=0.85
        )
        
        # Expected: 0.55 * 0.85 = 0.4675
        assert 0.45 < win_prob_no_data < 0.5
    
    def test_calculate_payoff_ratio(self, position_sizer):
        """Test calculating payoff ratio for Kelly criterion."""
        # Normal case
        payoff_ratio = position_sizer._calculate_payoff_ratio(
            expected_profit=100.0,
            position_size=1000.0,
            max_loss=50.0
        )
        
        # Expected: (100.0 / 1000.0) / (50.0 / 1000.0) = 0.1 / 0.05 = 2.0
        assert payoff_ratio == 2.0
        
        # Test with zero max_loss (should return default)
        payoff_ratio_zero_loss = position_sizer._calculate_payoff_ratio(
            expected_profit=100.0,
            position_size=1000.0,
            max_loss=0.0
        )
        
        assert payoff_ratio_zero_loss == position_sizer.config.get("position_sizing.default_payoff_ratio")
        
        # Test with ratio outside limits (too high)
        payoff_ratio_high = position_sizer._calculate_payoff_ratio(
            expected_profit=1000.0,
            position_size=1000.0,
            max_loss=10.0
        )
        
        assert payoff_ratio_high <= position_sizer.config.get("position_sizing.max_payoff_ratio")
        
        # Test with ratio outside limits (too low)
        payoff_ratio_low = position_sizer._calculate_payoff_ratio(
            expected_profit=10.0,
            position_size=1000.0,
            max_loss=100.0
        )
        
        assert payoff_ratio_low >= position_sizer.config.get("position_sizing.min_payoff_ratio")
    
    def test_calculate_kelly_fraction(self, position_sizer):
        """Test calculating Kelly fraction with adjustments."""
        # Normal case
        kelly_fraction = position_sizer._calculate_kelly_fraction(
            win_probability=0.6,
            payoff_ratio=2.0,
            confidence=0.85
        )
        
        # Classic Kelly: (0.6 * 2 - 0.4) / 2 = 0.4
        # Fractional Kelly with confidence: 0.4 * (0.3 + 0.85 * 0.2) = 0.4 * 0.47 = 0.188
        # Should be within reasonable range
        assert 0.15 < kelly_fraction < 0.25
        
        # Test with low confidence (should use half-Kelly maximum)
        kelly_fraction_low_conf = position_sizer._calculate_kelly_fraction(
            win_probability=0.6,
            payoff_ratio=2.0,
            confidence=0.5
        )
        
        # Half-Kelly would be 0.4 * 0.5 = 0.2
        # Should be capped by this
        assert kelly_fraction_low_conf <= 0.2
        
        # Test with losing edge (negative Kelly)
        kelly_fraction_losing = position_sizer._calculate_kelly_fraction(
            win_probability=0.3,
            payoff_ratio=1.5,
            confidence=0.85
        )
        
        # Should return 0.0 (no negative sizing)
        assert kelly_fraction_losing == 0.0
    
    def test_apply_volatility_adjustment(self, position_sizer):
        """Test volatility-based position size adjustment."""
        # Higher recent volatility case
        position_sizer.data_handler.get_recent_volatility.return_value = 0.03  # 3%
        position_sizer.data_handler.get_historical_volatility.return_value = 0.02  # 2%
        
        adjusted_size = position_sizer._apply_volatility_adjustment(
            base_size=1000.0,
            symbol="BTC",
            exchange="hyperliquid"
        )
        
        # Volatility ratio: 0.03 / 0.02 = 1.5
        # Adjustment: 1.0 / (1.5 ^ 1.5) = 1.0 / 1.84 = 0.54
        # Expected size: 1000.0 * 0.54 = 540.0
        assert 500.0 < adjusted_size < 600.0
        
        # Lower recent volatility case
        position_sizer.data_handler.get_recent_volatility.return_value = 0.01  # 1%
        position_sizer.data_handler.get_historical_volatility.return_value = 0.02  # 2%
        
        adjusted_size = position_sizer._apply_volatility_adjustment(
            base_size=1000.0,
            symbol="BTC",
            exchange="hyperliquid"
        )
        
        # Volatility ratio: 0.01 / 0.02 = 0.5
        # Adjustment: min(1.2, (1.0 / 0.5) ^ (1.5 * 0.5)) = min(1.2, 1.41) = 1.2
        # Expected size: 1000.0 * 1.2 = 1200.0
        assert 1100.0 < adjusted_size < 1300.0
        
        # Missing volatility data case
        position_sizer.data_handler.get_recent_volatility.return_value = None
        
        adjusted_size = position_sizer._apply_volatility_adjustment(
            base_size=1000.0,
            symbol="BTC",
            exchange="hyperliquid"
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
    
    def test_apply_drawdown_protection(self, position_sizer):
        """Test drawdown-based position size adjustment."""
        # No drawdown case
        position_sizer.portfolio_tracker.get_current_drawdown.return_value = 0.0
        
        adjusted_size = position_sizer._apply_drawdown_protection(
            position_size=1000.0
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
        
        # Minor drawdown case
        position_sizer.portfolio_tracker.get_current_drawdown.return_value = -3.0  # 3% drawdown
        
        adjusted_size = position_sizer._apply_drawdown_protection(
            position_size=1000.0
        )
        
        # Factor: 1.0 - (3.0 / 5.0) * 0.2 = 1.0 - 0.12 = 0.88
        # Expected size: 1000.0 * 0.88 = 880.0
        assert 850.0 < adjusted_size < 900.0
        
        # Moderate drawdown case
        position_sizer.portfolio_tracker.get_current_drawdown.return_value = -10.0  # 10% drawdown
        
        adjusted_size = position_sizer._apply_drawdown_protection(
            position_size=1000.0
        )
        
        # Factor: 0.8 - (10.0 - 5.0) / (15.0 - 5.0) * 0.3 = 0.8 - 0.15 = 0.65
        # Expected size: 1000.0 * 0.65 = 650.0
        assert 600.0 < adjusted_size < 700.0
        
        # Severe drawdown case
        position_sizer.portfolio_tracker.get_current_drawdown.return_value = -20.0  # 20% drawdown
        
        adjusted_size = position_sizer._apply_drawdown_protection(
            position_size=1000.0
        )
        
        # Factor: 0.5 - (20.0 - 15.0) / (100.0 - 15.0) * 0.5 = 0.5 - 0.03 = 0.47
        # Expected size: 1000.0 * 0.47 = 470.0
        assert 450.0 < adjusted_size < 500.0
        
        # Extreme drawdown case (should use minimum factor)
        position_sizer.portfolio_tracker.get_current_drawdown.return_value = -50.0  # 50% drawdown
        
        adjusted_size = position_sizer._apply_drawdown_protection(
            position_size=1000.0
        )
        
        # Should use minimum factor: 1000.0 * 0.1 = 100.0
        assert adjusted_size == 100.0
    
    def test_apply_portfolio_limits(self, position_sizer):
        """Test portfolio-level exposure limits."""
        # Within limits case
        adjusted_size = position_sizer._apply_portfolio_limits(
            position_size=1000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
        
        # Total exposure limit case
        position_sizer.portfolio_tracker.get_total_exposure.return_value = 75000.0  # 75% exposure
        
        adjusted_size = position_sizer._apply_portfolio_limits(
            position_size=10000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Total available: (0.8 - 0.75) * 100000 = 5000.0
        # Should be limited to 5000.0
        assert adjusted_size == 5000.0
        
        # Symbol exposure limit case
        position_sizer.portfolio_tracker.get_total_exposure.return_value = 30000.0  # Reset
        position_sizer.portfolio_tracker.get_symbol_exposure.return_value = 18000.0  # 18% symbol exposure
        
        adjusted_size = position_sizer._apply_portfolio_limits(
            position_size=10000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Symbol available: (0.2 - 0.18) * 100000 = 2000.0
        # Should be limited to 2000.0
        assert adjusted_size == 2000.0
        
        # Exchange exposure limit case
        position_sizer.portfolio_tracker.get_symbol_exposure.return_value = 5000.0  # Reset
        position_sizer.portfolio_tracker.get_exchange_exposure.return_value = 48000.0  # 48% exchange exposure
        
        adjusted_size = position_sizer._apply_portfolio_limits(
            position_size=10000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Exchange available: (0.5 - 0.48) * 100000 = 2000.0
        # Should be limited to 2000.0
        assert adjusted_size == 2000.0
        
        # Zero portfolio value case
        position_sizer.portfolio_tracker.get_total_portfolio_value.return_value = 0.0
        
        adjusted_size = position_sizer._apply_portfolio_limits(
            position_size=1000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Should return minimum trade size
        assert adjusted_size == position_sizer.config.get("position_sizing.min_trade_size")
    
    def test_apply_correlation_limits(self, position_sizer):
        """Test correlation-based position size adjustment."""
        # No active positions case
        position_sizer.portfolio_tracker.get_active_symbols.return_value = []
        
        adjusted_size = position_sizer._apply_correlation_limits(
            position_size=1000.0,
            symbol="BTC"
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
        
        # Same symbol already in portfolio case
        position_sizer.portfolio_tracker.get_active_symbols.return_value = ["BTC", "ETH"]
        
        adjusted_size = position_sizer._apply_correlation_limits(
            position_size=1000.0,
            symbol="BTC"
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
        
        # Normal correlation case (below threshold)
        position_sizer.portfolio_tracker.get_active_symbols.return_value = ["ETH"]
        
        adjusted_size = position_sizer._apply_correlation_limits(
            position_size=1000.0,
            symbol="BTC"
        )
        
        # Correlation (BTC, ETH) = 0.6 < 0.7 threshold
        # Should return original size
        assert adjusted_size == 1000.0
        
        # High correlation case
        position_sizer.portfolio_tracker.get_active_symbols.return_value = ["SOL"]
        
        adjusted_size = position_sizer._apply_correlation_limits(
            position_size=1000.0,
            symbol="BTC"
        )
        
        # Correlation (BTC, SOL) = 0.8 > 0.7 threshold
        # Factor: 1.0 - (0.8 - 0.7) / (1.0 - 0.7) * (1.0 - 0.5) = 1.0 - 0.33 * 0.5 = 0.835
        # Expected size: 1000.0 * 0.835 = 835.0
        assert 800.0 < adjusted_size < 850.0
        
        # Missing correlation data case
        position_sizer.data_handler.get_correlation_matrix.return_value = None
        
        adjusted_size = position_sizer._apply_correlation_limits(
            position_size=1000.0,
            symbol="BTC"
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
    
    def test_apply_exchange_specific_adjustments(self, position_sizer):
        """Test exchange-specific position size adjustments."""
        # Normal case
        adjusted_size = position_sizer._apply_exchange_specific_adjustments(
            position_size=1000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Liquidity factor: 0.9
        # Expected size: 1000.0 * 0.9 = 900.0
        assert adjusted_size == 900.0
        
        # Below minimum order size case
        adjusted_size = position_sizer._apply_exchange_specific_adjustments(
            position_size=5.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Should be increased to minimum: 10.0
        assert adjusted_size == 10.0
        
        # Above maximum order size case
        adjusted_size = position_sizer._apply_exchange_specific_adjustments(
            position_size=200000.0,
            exchange="hyperliquid",
            symbol="BTC"
        )
        
        # Should be decreased to maximum: 100000.0
        assert adjusted_size == 90000.0  # After liquidity factor
        
        # Missing exchange adapter case
        adjusted_size = position_sizer._apply_exchange_specific_adjustments(
            position_size=1000.0,
            exchange="unknown",
            symbol="BTC"
        )
        
        # Should return original size
        assert adjusted_size == 1000.0
    
    def test_calculate_position_size(self, position_sizer, sample_signal):
        """Test the end-to-end position sizing calculation."""
        # Normal case
        position_size = position_sizer.calculate_position_size(
            signal=sample_signal,
            funding_rate=0.01,
            expected_profit=100.0,
            max_loss=50.0,
            confidence=0.85
        )
        
        # Should return a valid position size
        assert position_size > 0
        
        # Test with debugging enabled
        position_sizer.config.get.side_effect = lambda key, default=None: {
            'position_sizing.debug_position_sizing': True
        }.get(key, default) or {
            'position_sizing.base_win_probability': 0.55,
            'position_sizing.base_kelly_fraction': 0.3,
            'position_sizing.confidence_adjustment_factor': 0.2,
            'position_sizing.high_confidence_threshold': 0.8,
            'position_sizing.max_kelly_fraction': 0.5,
            'position_sizing.max_position_size': 5000.0,
            'position_sizing.min_position_size': 100.0,
            'position_sizing.default_payoff_ratio': 1.5,
            'position_sizing.min_payoff_ratio': 1.1,
            'position_sizing.max_payoff_ratio': 5.0,
            'position_sizing.volatility_scaling_power': 1.5,
            'position_sizing.max_low_volatility_increase': 1.2,
            'position_sizing.minor_drawdown_threshold': 5.0,
            'position_sizing.moderate_drawdown_threshold': 15.0,
            'position_sizing.min_drawdown_factor': 0.1,
            'position_sizing.max_total_exposure': 0.8,
            'position_sizing.max_symbol_exposure': 0.2,
            'position_sizing.max_exchange_exposure': 0.5,
            'position_sizing.min_trade_size': 50.0,
            'position_sizing.correlation_threshold': 0.7,
            'position_sizing.min_correlation_factor': 0.5,
            'position_sizing.initial_position_size': 1000.0
        }.get(key, default)
        
        position_sizer._log_position_sizing_details = MagicMock()
        
        position_size = position_sizer.calculate_position_size(
            signal=sample_signal,
            funding_rate=0.01,
            expected_profit=100.0,
            max_loss=50.0,
            confidence=0.85
        )
        
        # Should call log function
        assert position_sizer._log_position_sizing_details.called


class TestEnhancedPositionSizerIntegration:
    """Integration tests for the EnhancedPositionSizer with real components."""
    
    @pytest.mark.skip(reason="Integration test requiring actual component implementations")
    def test_real_components_integration(self):
        """Test with real component implementations instead of mocks."""
        # This test would use actual implementations of portfolio tracker, data handler, etc.
        # It's skipped by default since it requires the actual components
        pass 