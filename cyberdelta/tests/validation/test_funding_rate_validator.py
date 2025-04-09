"""
Tests for the FundingRateValidator class.
"""

import pytest
import sqlite3
import time
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import os
import tempfile

from cyberdelta.validation.funding_rate_validator import FundingRateValidator


class TestFundingRateValidator:
    """Test suite for the FundingRateValidator class."""
    
    @pytest.fixture
    def validator(self):
        """Create a validator with in-memory SQLite database for testing."""
        # Create a mock config that returns in-memory SQLite database
        config = MagicMock()
        config.get.return_value = ":memory:"
        
        return FundingRateValidator(config)
    
    @pytest.fixture
    def validator_with_file(self):
        """Create a validator with temporary file-based SQLite database."""
        # Create a temporary directory for the test database
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_validation.db")
        
        # Create a mock config
        config = MagicMock()
        config.get.return_value = db_path
        
        validator = FundingRateValidator(config)
        
        yield validator
        
        # Clean up
        if os.path.exists(db_path):
            os.remove(db_path)
        os.rmdir(temp_dir)
    
    def test_init_db(self, validator):
        """Test database initialization."""
        # Get a connection to verify tables exist
        conn = validator._get_db_connection()
        cursor = conn.cursor()
        
        # Check funding_predictions table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='funding_predictions'")
        assert cursor.fetchone() is not None
        
        # Check funding_payments table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='funding_payments'")
        assert cursor.fetchone() is not None
        
        conn.close()
    
    def test_record_prediction(self, validator):
        """Test recording a funding rate prediction."""
        # Record a prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0001, "api", 0.9)
        
        # Verify it was stored
        conn = validator._get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM funding_predictions WHERE exchange = ? AND symbol = ?", 
                      ("hyperliquid", "BTC"))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result[2] == "hyperliquid"  # exchange
        assert result[3] == "BTC"  # symbol
        assert result[4] == 0.0001  # predicted_rate
        assert result[5] == "api"  # prediction_method
        assert result[6] == 0.9  # confidence
    
    def test_record_payment(self, validator):
        """Test recording an actual funding payment."""
        # Record a payment
        validator.record_payment("hyperliquid", "BTC", 0.0001, 0.5, 10.0)
        
        # Verify it was stored
        conn = validator._get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM funding_payments WHERE exchange = ? AND symbol = ?", 
                      ("hyperliquid", "BTC"))
        result = cursor.fetchone()
        conn.close()
        
        assert result is not None
        assert result[2] == "hyperliquid"  # exchange
        assert result[3] == "BTC"  # symbol
        assert result[4] == 0.0001  # actual_rate
        assert result[5] == 0.5  # payment_amount
        assert result[6] == 10.0  # position_size
    
    def test_calculate_metrics_no_data(self, validator):
        """Test calculating metrics with no data."""
        metrics = validator.calculate_metrics("hyperliquid", "BTC")
        
        assert metrics["rmse"] is None
        assert metrics["mae"] is None
        assert metrics["bias"] is None
        assert metrics["prediction_count"] == 0
        assert metrics["payment_count"] == 0
    
    def test_calculate_metrics_with_data(self, validator):
        """Test calculating metrics with sample data."""
        # Add test data
        # First prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        time.sleep(0.01)  # Ensure different timestamps
        
        # First payment (after first prediction)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        time.sleep(0.01)
        
        # Second prediction
        validator.record_prediction("hyperliquid", "BTC", 0.0008, "api", 0.9)
        time.sleep(0.01)
        
        # Second payment (after second prediction)
        validator.record_payment("hyperliquid", "BTC", 0.0009, 0.9, 100.0)
        
        # Calculate metrics
        metrics = validator.calculate_metrics("hyperliquid", "BTC")
        
        # Verify metrics
        assert metrics["rmse"] is not None
        assert metrics["mae"] is not None
        assert metrics["bias"] is not None
        assert metrics["prediction_count"] == 2
        assert metrics["payment_count"] == 2
        assert metrics["matched_count"] == 2
        
        # Expected metrics:
        # First pair: error = 0.0010 - 0.0012 = -0.0002
        # Second pair: error = 0.0008 - 0.0009 = -0.0001
        # RMSE = sqrt(mean([-0.0002, -0.0001]^2)) = sqrt(mean([0.00000004, 0.00000001])) = sqrt(0.000000025) = 0.00015811
        # MAE = mean(abs([-0.0002, -0.0001])) = mean([0.0002, 0.0001]) = 0.00015
        # Bias = mean([-0.0002, -0.0001]) = -0.00015
        
        # Allow for small floating-point differences
        assert abs(metrics["rmse"] - 0.00015811) < 0.0001
        assert abs(metrics["mae"] - 0.00015) < 0.0001
        assert abs(metrics["bias"] - (-0.00015)) < 0.0001
    
    def test_get_validation_report(self, validator):
        """Test generating a validation report."""
        # Add data for two exchange-symbol pairs
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        
        validator.record_prediction("backpack", "ETH", 0.0005, "api", 0.9)
        validator.record_payment("backpack", "ETH", 0.0004, 0.4, 50.0)
        
        # Generate report
        report = validator.get_validation_report()
        
        # Verify report structure
        assert "hyperliquid" in report
        assert "backpack" in report
        assert "BTC" in report["hyperliquid"]
        assert "ETH" in report["backpack"]
        
        # Verify report contents
        assert report["hyperliquid"]["BTC"]["prediction_count"] == 1
        assert report["hyperliquid"]["BTC"]["payment_count"] == 1
        assert report["backpack"]["ETH"]["prediction_count"] == 1
        assert report["backpack"]["ETH"]["payment_count"] == 1
    
    def test_get_recent_predictions(self, validator):
        """Test retrieving recent predictions."""
        # Add predictions
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        validator.record_prediction("hyperliquid", "ETH", 0.0005, "api", 0.8)
        validator.record_prediction("backpack", "BTC", 0.0008, "model", 0.7)
        
        # Get all predictions
        predictions = validator.get_recent_predictions()
        assert len(predictions) == 3
        
        # Get predictions for a specific exchange
        predictions = validator.get_recent_predictions(exchange="hyperliquid")
        assert len(predictions) == 2
        assert all(p["exchange"] == "hyperliquid" for p in predictions)
        
        # Get predictions for a specific symbol
        predictions = validator.get_recent_predictions(symbol="BTC")
        assert len(predictions) == 2
        assert all(p["symbol"] == "BTC" for p in predictions)
        
        # Get predictions for a specific exchange and symbol
        predictions = validator.get_recent_predictions(exchange="hyperliquid", symbol="BTC")
        assert len(predictions) == 1
        assert predictions[0]["exchange"] == "hyperliquid"
        assert predictions[0]["symbol"] == "BTC"
    
    def test_get_recent_payments(self, validator):
        """Test retrieving recent payments."""
        # Add payments
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        validator.record_payment("hyperliquid", "ETH", 0.0006, 0.6, 80.0)
        validator.record_payment("backpack", "BTC", 0.0009, 0.9, 90.0)
        
        # Get all payments
        payments = validator.get_recent_payments()
        assert len(payments) == 3
        
        # Get payments for a specific exchange
        payments = validator.get_recent_payments(exchange="hyperliquid")
        assert len(payments) == 2
        assert all(p["exchange"] == "hyperliquid" for p in payments)
        
        # Get payments for a specific symbol
        payments = validator.get_recent_payments(symbol="BTC")
        assert len(payments) == 2
        assert all(p["symbol"] == "BTC" for p in payments)
        
        # Get payments for a specific exchange and symbol
        payments = validator.get_recent_payments(exchange="hyperliquid", symbol="BTC")
        assert len(payments) == 1
        assert payments[0]["exchange"] == "hyperliquid"
        assert payments[0]["symbol"] == "BTC"
    
    def test_get_prediction_history(self, validator):
        """Test retrieving prediction history."""
        # Add data
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        time.sleep(0.01)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        time.sleep(0.01)
        validator.record_prediction("hyperliquid", "BTC", 0.0008, "api", 0.9)
        time.sleep(0.01)
        validator.record_payment("hyperliquid", "BTC", 0.0009, 0.9, 100.0)
        
        # Get history
        history = validator.get_prediction_history("hyperliquid", "BTC")
        
        # Verify structure
        assert "predictions" in history
        assert "actuals" in history
        assert "timestamps" in history["predictions"]
        assert "rates" in history["predictions"]
        assert "timestamps" in history["actuals"]
        assert "rates" in history["actuals"]
        
        # Verify data
        assert len(history["predictions"]["timestamps"]) == 2
        assert len(history["predictions"]["rates"]) == 2
        assert len(history["actuals"]["timestamps"]) == 2
        assert len(history["actuals"]["rates"]) == 2
        
        # Verify values
        assert history["predictions"]["rates"] == [0.0010, 0.0008]
        assert history["actuals"]["rates"] == [0.0012, 0.0009]
    
    def test_file_db_persistence(self, validator_with_file):
        """Test that data is persisted in file-based database."""
        validator = validator_with_file
        
        # Add data
        validator.record_prediction("hyperliquid", "BTC", 0.0010, "api", 0.9)
        validator.record_payment("hyperliquid", "BTC", 0.0012, 1.2, 100.0)
        
        # Verify data exists
        predictions = validator.get_recent_predictions()
        payments = validator.get_recent_payments()
        
        assert len(predictions) == 1
        assert len(payments) == 1 