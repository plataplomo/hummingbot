"""
Utilities for generating synthetic market data for testing purposes.
"""

import logging
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def generate_synthetic_data(
    days: int = 60,
    symbols: list[str] | None = None,
    data_type: str = "funding_rate",
    freq: str = "1h",
    seed: int | None = None,
) -> pd.DataFrame:
    """
    Generate synthetic market data for backtesting.

    Args:
        days: Number of days of data to generate.
        symbols: List of symbols (e.g., exchange pairs) to generate data for.
        data_type: Type of data ('funding_rate', 'price', 'combined').
        freq: Frequency of data points (e.g., '1H', '15Min').
        seed: Random seed for reproducibility.

    Returns:
        DataFrame containing the synthetic data.
    """
    if symbols is None:
        symbols = ["ExchangeA/Coin1/USDT", "ExchangeB/Coin1/USDT"]

    rng = np.random.default_rng(seed)
    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=days)
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)

    data_frames = []

    for symbol in symbols:
        num_points = len(date_range)
        df_symbol = pd.DataFrame(index=date_range)

        if data_type in ["funding_rate", "combined"]:
            # Simulate funding rates: Random walk around a small mean
            mean_rate = rng.uniform(-0.0001, 0.0001)
            volatility = rng.uniform(0.00005, 0.0002)
            rates = rng.normal(mean_rate, volatility, num_points).cumsum() * 0.1 + mean_rate
            df_symbol[("funding_rate", symbol)] = rates

        if data_type in ["price", "combined"]:
            # Simulate prices: Geometric Brownian Motion
            start_price = rng.uniform(1000, 50000)
            drift = rng.uniform(-0.001, 0.001)
            volatility = rng.uniform(0.01, 0.05)
            log_returns = rng.normal(drift, volatility, num_points)
            prices = start_price * np.exp(log_returns.cumsum())
            df_symbol[("mid_price", symbol)] = prices
            # Simulate bid/ask spread
            spread = rng.uniform(0.0005, 0.002) * prices  # Spread as fraction of price
            df_symbol[("bid_price", symbol)] = prices - spread / 2
            df_symbol[("ask_price", symbol)] = prices + spread / 2

        # Ensure columns are Decimal
        # for col in df_symbol.columns:
        #     try:
        #         df_symbol[col] = df_symbol[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)
        #     except Exception as e:
        #         logger.error(f"Error converting column {col} to Decimal: {e}")
        #         # Handle error appropriately, maybe skip column or raise

        data_frames.append(df_symbol)

    if not data_frames:
        logger.warning("No data generated.")
        return pd.DataFrame(index=date_range)

    # Combine dataframes for different symbols
    combined_df = pd.concat(data_frames, axis=1)

    # Sort columns by symbol then metric for consistent structure
    combined_df.columns = pd.MultiIndex.from_tuples(combined_df.columns)
    combined_df = combined_df.sort_index(axis=1, level=[1, 0])

    logger.info(
        f"Generated synthetic {data_type} data for {symbols} with shape {combined_df.shape}"
    )
    return combined_df
