import asyncio
import logging
from typing import Dict, Any, Optional

# Assuming structure: strategy_math/coding_strategy/src/
from ..apis.base import ExchangeAPI, MessageHandler
from ..core.models import Ticker, OrderBook # Import relevant models
from ..config.settings import settings

logger = logging.getLogger(__name__)

class DataHandler:
    """Handles incoming data streams from exchanges."""

    def __init__(self, api_clients: Dict[str, ExchangeAPI]):
        self.api_clients = api_clients
        self.tickers: Dict[str, Ticker] = {} # Store latest ticker data per symbol@exchange
        self.order_books: Dict[str, OrderBook] = {} # Store latest order book data
        # Add other data stores as needed (trades, funding rates via WS, etc.)
        self._stop_event = asyncio.Event()

    async def _handle_ticker_message(self, exchange: str, data: Dict[str, Any]):
        """Placeholder handler for ticker WebSocket messages."""
        # TODO: Parse the exchange-specific data format into our Ticker model
        # Example parsing (highly dependent on actual data format):
        try:
            symbol = data.get('symbol') # Adjust key based on actual data
            if not symbol:
                logger.warning(f"[{exchange}] Ticker message missing symbol: {data}")
                return

            ticker = Ticker(
                symbol=symbol,
                bid_price=float(data.get('bid')), # Adjust keys
                ask_price=float(data.get('ask')), # Adjust keys
                last_price=float(data.get('last')) # Adjust keys
            )
            key = f"{symbol}@{exchange}"
            self.tickers[key] = ticker
            # logger.debug(f"Updated ticker for {key}: {ticker}")
        except Exception as e:
            logger.error(f"[{exchange}] Error processing ticker data: {data} - Error: {e}", exc_info=True)

    async def _handle_orderbook_message(self, exchange: str, data: Dict[str, Any]):
        """Placeholder handler for order book WebSocket messages."""
        # TODO: Parse the exchange-specific data format into our OrderBook model
        logger.debug(f"[{exchange}] Received order book update (parsing not implemented): {str(data)[:200]}")
        # Parse bids/asks, update self.order_books[key]
        pass

    async def subscribe_to_streams(self):
        """Subscribes to necessary data streams for configured symbols and exchanges."""
        if not settings:
            logger.error("Cannot subscribe to streams, settings not loaded.")
            return

        logger.info("Subscribing to data streams...")
        for exchange_name, client in self.api_clients.items():
            if not client.is_connected:
                logger.warning(f"Cannot subscribe to {exchange_name} streams, client not connected.")
                continue

            for symbol in settings.symbols:
                # --- Subscribe to Tickers --- #
                # Determine the exchange-specific topic name for tickers
                # This is an EXAMPLE, replace with actual topic format
                ticker_topic = f"ticker_{symbol}" # e.g., "ticker_BTC-PERP" or "instrument.ticker.{symbol}"
                try:
                    # Pass the appropriate handler
                    # Need partial or lambda to include exchange context
                    await client.subscribe(ticker_topic, lambda msg, ex=exchange_name: self._handle_ticker_message(ex, msg))
                    logger.info(f"[{exchange_name}] Subscribing to ticker for {symbol} (Topic: {ticker_topic})")
                except Exception as e:
                    logger.error(f"[{exchange_name}] Failed to subscribe to ticker {symbol}: {e}")

                # --- Subscribe to Order Books (Optional/Example) --- #
                # orderbook_topic = f"orderbook_{symbol}" # Replace with actual format
                # try:
                #    await client.subscribe(orderbook_topic, lambda msg, ex=exchange_name: self._handle_orderbook_message(ex, msg))
                #    logger.info(f"[{exchange_name}] Subscribing to order book for {symbol} (Topic: {orderbook_topic})")
                # except Exception as e:
                #    logger.error(f"[{exchange_name}] Failed to subscribe to order book {symbol}: {e}")

                # Add subscriptions for trades, funding rates via WS, etc. as needed

                await asyncio.sleep(0.2) # Avoid overwhelming the API with rapid subscriptions

        logger.info("Finished subscribing to initial data streams.")

    async def run(self):
        """Runs the data handler, primarily by keeping connections alive and subscribed."""
        logger.info("Data Handler starting run loop.")
        await self.subscribe_to_streams()
        # The actual message handling happens in the API client's _ws_listener loop
        # This run method might just wait for a stop signal or perform periodic checks
        try:
            await self._stop_event.wait() # Keep running until stopped
        except asyncio.CancelledError:
            logger.info("Data Handler run loop cancelled.")
        finally:
            logger.info("Data Handler run loop finished.")

    def stop(self):
        """Signals the data handler to stop."""
        logger.info("Data Handler received stop signal.")
        self._stop_event.set()

    # Methods to access data (can be expanded)
    def get_ticker(self, symbol: str, exchange: str) -> Optional[Ticker]:
        return self.tickers.get(f"{symbol}@{exchange}")

    def get_order_book(self, symbol: str, exchange: str) -> Optional[OrderBook]:
        return self.order_books.get(f"{symbol}@{exchange}") 