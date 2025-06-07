"""Utilities for generating synthetic market data for testing purposes."""

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def generate_synthetic_data(
    days: int = 60,
    symbols: list[str] | None = None,
    data_type: str = "funding_rate",
    freq: str = "1h",
    seed: int | None = None,
) -> pd.DataFrame:
    """Generate synthetic market data for backtesting.

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

    data_frames: list[pd.DataFrame] = []

    for symbol in symbols:
        df_symbol = _generate_symbol_data(symbol, date_range, data_type, rng)
        data_frames.append(df_symbol)

    if not data_frames:
        logger.warning("No data generated.")
        return pd.DataFrame(index=date_range)

    # Combine dataframes for different symbols
    combined_df = pd.concat(data_frames, axis=1)
    combined_df = _process_dataframe_columns(combined_df)

    logger.info(
        f"Generated synthetic {data_type} data for {symbols} with shape {combined_df.shape}",
    )
    return combined_df


def _generate_symbol_data(
    symbol: str, date_range: pd.DatetimeIndex, data_type: str, rng: np.random.Generator
) -> pd.DataFrame:
    """Generate data for a single symbol."""
    num_points = len(date_range)
    df_symbol = pd.DataFrame(index=date_range)

    if data_type in ["funding_rate", "combined"]:
        _add_funding_rate_data(df_symbol, symbol, num_points, rng)

    if data_type in ["price", "combined"]:
        _add_price_data(df_symbol, symbol, num_points, rng)

    elif data_type == "ohlcv":
        _add_ohlcv_data(df_symbol, symbol, num_points, rng)

    return df_symbol


def _add_funding_rate_data(
    df: pd.DataFrame, symbol: str, num_points: int, rng: np.random.Generator
) -> None:
    """Add funding rate data to the dataframe."""
    # Simulate funding rates: Random walk around a small mean
    mean_rate = rng.uniform(-0.0001, 0.0001)
    volatility = rng.uniform(0.00005, 0.0002)
    rates = rng.normal(mean_rate, volatility, num_points).cumsum() * 0.1 + mean_rate
    df[("funding_rate", symbol)] = rates


def _add_price_data(
    df: pd.DataFrame, symbol: str, num_points: int, rng: np.random.Generator
) -> None:
    """Add price data to the dataframe."""
    # Simulate prices: Geometric Brownian Motion
    start_price = rng.uniform(1000, 50000)
    drift = rng.uniform(-0.001, 0.001)
    volatility = rng.uniform(0.01, 0.05)
    log_returns = rng.normal(drift, volatility, num_points)
    prices = start_price * np.exp(log_returns.cumsum())
    df[("mid_price", symbol)] = prices
    # Simulate bid/ask spread
    spread = rng.uniform(0.0005, 0.002) * prices  # Spread as fraction of price
    df[("bid_price", symbol)] = prices - spread / 2
    df[("ask_price", symbol)] = prices + spread / 2


def _add_ohlcv_data(
    df: pd.DataFrame, symbol: str, num_points: int, rng: np.random.Generator
) -> None:
    """Add OHLCV data to the dataframe."""
    # Simulate OHLCV data based on a mid_price simulation
    start_price = rng.uniform(1000, 50000)
    drift = rng.uniform(-0.001, 0.001)
    volatility = rng.uniform(0.01, 0.05)
    log_returns = rng.normal(drift, volatility, num_points)
    base_prices = start_price * np.exp(log_returns.cumsum())

    # Derive OHLC from base_prices with some noise
    price_variation = volatility * base_prices * 0.1  # Smaller variation for OHLC
    df[("open", symbol)] = base_prices - rng.normal(0, price_variation / 2, num_points)
    df[("close", symbol)] = base_prices + rng.normal(0, price_variation / 2, num_points)

    _calculate_high_low_prices(df, symbol, price_variation, rng)
    _add_volume_data(df, symbol, num_points, rng)


def _calculate_high_low_prices(
    df: pd.DataFrame, symbol: str, price_variation: np.ndarray[Any, Any], rng: np.random.Generator
) -> None:
    """Calculate high and low prices ensuring proper OHLC relationships."""
    df[("high", symbol)] = np.maximum(
        df[("open", symbol)],
        df[("close", symbol)],
    ) + rng.exponential(price_variation, len(price_variation))
    df[("low", symbol)] = np.minimum(
        df[("open", symbol)],
        df[("close", symbol)],
    ) - rng.exponential(price_variation, len(price_variation))

    # Ensure low <= open/close <= high
    df[("low", symbol)] = np.minimum(
        df[("low", symbol)],
        np.minimum(df[("open", symbol)], df[("close", symbol)]),
    )
    df[("high", symbol)] = np.maximum(
        df[("high", symbol)],
        np.maximum(df[("open", symbol)], df[("close", symbol)]),
    )


def _add_volume_data(
    df: pd.DataFrame, symbol: str, num_points: int, rng: np.random.Generator
) -> None:
    """Add volume data to the dataframe."""
    mean_volume = rng.uniform(10, 1000)
    df[("volume", symbol)] = rng.poisson(mean_volume, num_points)


def _process_dataframe_columns(combined_df: pd.DataFrame) -> pd.DataFrame:
    """Process and sort dataframe columns."""
    # Sort columns by symbol then metric for consistent structure
    if not combined_df.columns.empty:
        try:
            # Check if columns are already tuples, if not convert them
            if not isinstance(combined_df.columns, pd.MultiIndex):
                # Convert column names to tuples if they aren't already
                # DEFENSIVE CHECK: Check if columns are iterable but not strings.
                # Mypy=[unreachable] Ruff=[]
                if hasattr(combined_df.columns[0], "__iter__") and not isinstance(
                    combined_df.columns[0],
                    str,
                ):
                    combined_df.columns = pd.MultiIndex.from_tuples(combined_df.columns)
                # Note: Removed unreachable else clause as type checker can't reach it
            combined_df = combined_df.sort_index(axis=1, level=[1, 0])
        except (TypeError, ValueError) as e:
            logger.error(f"Error processing DataFrame columns: {e}. Columns: {combined_df.columns}")
            # Decide on fallback: return as is, or raise, or return with empty MultiIndex
            # For now, let it pass to see if a later stage handles it or fails revealing more.
            # Note: Continuing with original columns structure
    # else: combined_df has no columns, leave as is (empty Index for columns)

    return combined_df
