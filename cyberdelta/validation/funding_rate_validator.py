"""
Funding Rate Validation System

This module implements the validation system for funding rate predictions
against actual payments received/paid.
"""

import logging
import sqlite3
import time
import os
import math
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

class FundingRateValidator:
    """
    Validates funding rate data and predictions against actual payments.
    Tracks accuracy and provides metrics for improving predictions.
    """
    
    def __init__(self, config):
        """
        Initialize the FundingRateValidator.
        
        Args:
            config: Application configuration object
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Database for tracking predictions and actuals
        self.db_path = config.get("validation.db_path", "data/validation.db")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize the database
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database for storing validation data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funding_predictions (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            exchange TEXT,
            symbol TEXT,
            predicted_rate REAL,
            prediction_method TEXT,
            confidence REAL
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS funding_payments (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            exchange TEXT,
            symbol TEXT,
            actual_rate REAL,
            payment_amount REAL,
            position_size REAL
        )
        ''')
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Initialized funding rate validation database at {self.db_path}")
    
    def _get_db_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        return sqlite3.connect(self.db_path)
    
    def record_prediction(self, exchange: str, symbol: str, 
                          predicted_rate: float, 
                          method: str = "api",
                          confidence: float = 1.0):
        """
        Record a funding rate prediction.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            predicted_rate: Predicted funding rate
            method: Method used for prediction (api, model, etc.)
            confidence: Confidence level in the prediction (0-1)
        """
        timestamp = int(time.time() * 1000)
        
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO funding_predictions (timestamp, exchange, symbol, predicted_rate, prediction_method, confidence) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (timestamp, exchange, symbol, predicted_rate, method, confidence)
        )
        
        conn.commit()
        conn.close()
        
        self.logger.debug(f"Recorded funding rate prediction: {exchange}/{symbol}, rate={predicted_rate:.6f}, method={method}")
    
    def record_payment(self, exchange: str, symbol: str, 
                       actual_rate: float, payment_amount: float,
                       position_size: float):
        """
        Record an actual funding payment.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            actual_rate: Actual funding rate that was applied
            payment_amount: Amount of funding paid/received
            position_size: Position size at time of payment
        """
        timestamp = int(time.time() * 1000)
        
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT INTO funding_payments (timestamp, exchange, symbol, actual_rate, payment_amount, position_size) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (timestamp, exchange, symbol, actual_rate, payment_amount, position_size)
        )
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Recorded funding payment: {exchange}/{symbol}, rate={actual_rate:.6f}, amount={payment_amount:.8f}")
    
    def calculate_metrics(self, exchange: str, symbol: str, days: int = 7) -> Dict[str, float]:
        """
        Calculate prediction accuracy metrics.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            days: Number of days to include in calculation
            
        Returns:
            Dictionary with accuracy metrics (RMSE, MAE, bias, etc.)
        """
        # Calculate time threshold (milliseconds)
        threshold = int((time.time() - (days * 86400)) * 1000)
        
        conn = self._get_db_connection()
        
        # Get predictions and payments within time range
        predictions_df = pd.read_sql_query(
            "SELECT timestamp, predicted_rate, prediction_method, confidence FROM funding_predictions "
            "WHERE exchange = ? AND symbol = ? AND timestamp >= ? "
            "ORDER BY timestamp",
            conn, params=(exchange, symbol, threshold)
        )
        
        payments_df = pd.read_sql_query(
            "SELECT timestamp, actual_rate FROM funding_payments "
            "WHERE exchange = ? AND symbol = ? AND timestamp >= ? "
            "ORDER BY timestamp",
            conn, params=(exchange, symbol, threshold)
        )
        
        conn.close()
        
        # If we don't have enough data, return empty metrics
        if len(predictions_df) == 0 or len(payments_df) == 0:
            self.logger.warning(f"Insufficient data to calculate metrics for {exchange}/{symbol}")
            return {
                "rmse": None,
                "mae": None,
                "bias": None,
                "prediction_count": len(predictions_df),
                "payment_count": len(payments_df)
            }
        
        # Merge predictions with closest actual payments
        # For each payment, find the most recent prediction before the payment
        merged_data = []
        for _, payment_row in payments_df.iterrows():
            payment_time = payment_row["timestamp"]
            actual_rate = payment_row["actual_rate"]
            
            # Find the most recent prediction before this payment
            relevant_predictions = predictions_df[predictions_df["timestamp"] < payment_time]
            if len(relevant_predictions) > 0:
                # Get the most recent prediction
                latest_prediction = relevant_predictions.iloc[-1]
                predicted_rate = latest_prediction["predicted_rate"]
                method = latest_prediction["prediction_method"]
                confidence = latest_prediction["confidence"]
                
                merged_data.append({
                    "timestamp": payment_time,
                    "predicted_rate": predicted_rate,
                    "actual_rate": actual_rate,
                    "method": method,
                    "confidence": confidence,
                    "error": predicted_rate - actual_rate
                })
        
        # If we couldn't match any predictions with payments
        if len(merged_data) == 0:
            self.logger.warning(f"No matching prediction-payment pairs for {exchange}/{symbol}")
            return {
                "rmse": None,
                "mae": None,
                "bias": None,
                "prediction_count": len(predictions_df),
                "payment_count": len(payments_df)
            }
        
        # Convert to DataFrame for calculations
        df = pd.DataFrame(merged_data)
        
        # Calculate metrics
        rmse = math.sqrt(np.mean(df["error"] ** 2))
        mae = np.mean(np.abs(df["error"]))
        bias = np.mean(df["error"])
        
        metrics = {
            "rmse": rmse,
            "mae": mae,
            "bias": bias,
            "prediction_count": len(predictions_df),
            "payment_count": len(payments_df),
            "matched_count": len(merged_data)
        }
        
        self.logger.info(f"Calculated metrics for {exchange}/{symbol}: RMSE={rmse:.6f}, MAE={mae:.6f}, Bias={bias:.6f}")
        return metrics
    
    def get_validation_report(self, days: int = 7) -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        Generate a comprehensive validation report.
        
        Args:
            days: Number of days to include in the report
            
        Returns:
            Dictionary with validation metrics by exchange and symbol
        """
        conn = self._get_db_connection()
        
        # Get unique exchange-symbol pairs
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT exchange, symbol FROM funding_payments "
            "UNION "
            "SELECT DISTINCT exchange, symbol FROM funding_predictions"
        )
        pairs = cursor.fetchall()
        
        conn.close()
        
        # Generate report for each pair
        report = {}
        for exchange, symbol in pairs:
            if exchange not in report:
                report[exchange] = {}
                
            metrics = self.calculate_metrics(exchange, symbol, days)
            report[exchange][symbol] = metrics
        
        return report
    
    def get_recent_predictions(self, exchange: str = None, symbol: str = None, 
                               limit: int = 100) -> List[Dict]:
        """
        Get recent funding rate predictions.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            limit: Maximum number of records to return
            
        Returns:
            List of prediction records
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT timestamp, exchange, symbol, predicted_rate, prediction_method, confidence FROM funding_predictions "
        params = []
        
        if exchange or symbol:
            query += "WHERE "
            conditions = []
            
            if exchange:
                conditions.append("exchange = ?")
                params.append(exchange)
                
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
                
            query += " AND ".join(conditions)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        
        predictions = []
        for row in cursor.fetchall():
            timestamp, exchange, symbol, rate, method, confidence = row
            predictions.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp / 1000),
                "exchange": exchange,
                "symbol": symbol,
                "predicted_rate": rate,
                "method": method,
                "confidence": confidence
            })
        
        conn.close()
        return predictions
    
    def get_recent_payments(self, exchange: str = None, symbol: str = None, 
                            limit: int = 100) -> List[Dict]:
        """
        Get recent funding payments.
        
        Args:
            exchange: Optional exchange filter
            symbol: Optional symbol filter
            limit: Maximum number of records to return
            
        Returns:
            List of payment records
        """
        conn = self._get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT timestamp, exchange, symbol, actual_rate, payment_amount, position_size FROM funding_payments "
        params = []
        
        if exchange or symbol:
            query += "WHERE "
            conditions = []
            
            if exchange:
                conditions.append("exchange = ?")
                params.append(exchange)
                
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol)
                
            query += " AND ".join(conditions)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        
        payments = []
        for row in cursor.fetchall():
            timestamp, exchange, symbol, rate, amount, size = row
            payments.append({
                "timestamp": timestamp,
                "datetime": datetime.fromtimestamp(timestamp / 1000),
                "exchange": exchange,
                "symbol": symbol,
                "actual_rate": rate,
                "payment_amount": amount,
                "position_size": size
            })
        
        conn.close()
        return payments
    
    def get_prediction_history(self, exchange: str, symbol: str, 
                               days: int = 30) -> Dict[str, List]:
        """
        Get prediction history for a specific exchange and symbol.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            days: Number of days of history to retrieve
            
        Returns:
            Dictionary with timestamp, predicted, and actual rate lists
        """
        # Calculate time threshold (milliseconds)
        threshold = int((time.time() - (days * 86400)) * 1000)
        
        conn = self._get_db_connection()
        
        # Get predictions
        predictions_df = pd.read_sql_query(
            "SELECT timestamp, predicted_rate FROM funding_predictions "
            "WHERE exchange = ? AND symbol = ? AND timestamp >= ? "
            "ORDER BY timestamp",
            conn, params=(exchange, symbol, threshold)
        )
        
        # Get payments
        payments_df = pd.read_sql_query(
            "SELECT timestamp, actual_rate FROM funding_payments "
            "WHERE exchange = ? AND symbol = ? AND timestamp >= ? "
            "ORDER BY timestamp",
            conn, params=(exchange, symbol, threshold)
        )
        
        conn.close()
        
        # Convert timestamps to datetime for easier visualization
        if not predictions_df.empty:
            predictions_df["datetime"] = pd.to_datetime(predictions_df["timestamp"], unit="ms")
        
        if not payments_df.empty:
            payments_df["datetime"] = pd.to_datetime(payments_df["timestamp"], unit="ms")
        
        return {
            "predictions": {
                "timestamps": predictions_df["timestamp"].tolist() if not predictions_df.empty else [],
                "datetimes": predictions_df["datetime"].tolist() if not predictions_df.empty else [],
                "rates": predictions_df["predicted_rate"].tolist() if not predictions_df.empty else []
            },
            "actuals": {
                "timestamps": payments_df["timestamp"].tolist() if not payments_df.empty else [],
                "datetimes": payments_df["datetime"].tolist() if not payments_df.empty else [],
                "rates": payments_df["actual_rate"].tolist() if not payments_df.empty else []
            }
        } 