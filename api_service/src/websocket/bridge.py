import asyncio
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
from aiokafka import AIOKafkaConsumer
import pymysql
from . import ws_manager as _ws_mod
ws_manager = _ws_mod
from typing import TypedDict, Callable

class TradeMessage(TypedDict):
    """Parsed trade message from market_data_trade topic."""
    type: str
    symbol: str
    last_price: float
    change: float
    ratio_change: float
    volume: int
    value: float
    time: str

class QuoteMessage(TypedDict):
    """Parsed quote message from market_data_quote topic."""
    type: str
    symbol: str
    bids: list[dict[str, float | int]]
    asks: list[dict[str, float | int]]
    time: str

class IndexMessage(TypedDict):
    """Parsed index message from index_data topic."""
    type: str
    index_id: str
    index_value: float
    change: float
    ratio_change: float
    advances: int
    declines: int
    time: str

WebSocketMessage = TradeMessage | QuoteMessage | IndexMessage
from ..services.stock_cache import stock_cache
from ..config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# Kafka topics to consume
TOPICS = [
    "market_data_trade",
    "market_data_quote",
    "index_data",
    "candlestick_updates",  # event-driven candlestick updates from consumer
]

# Parse streaming_db_url for pymysql candlestick poller (no longer used)
# _streaming_url = urlparse(settings.streaming_db_url.replace("mysql+pymysql://", "http://"))
# _STREAMING_HOST = _streaming_url.hostname or "mysql"
# _STREAMING_PORT = _streaming_url.port or 3306
# _STREAMING_DB = _streaming_url.path.lstrip("/")


def _safe_float(val) -> float:
    try:
        return float(val) if val is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(val) -> int:
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


def _parse_trade(content_str: str) -> TradeMessage | None:
    """Parse a trade message from market_data_trade topic."""
    try:
        data = json.loads(content_str)
        symbol = data.get("Symbol", "")
        if not symbol:
            return None
        return {
            "type": "price_update",
            "symbol": symbol,
            "last_price": _safe_float(data.get("LastPrice")),
            # SSI stores Change as integer × 1000 (e.g. 60 = 0.06 VND change)
            "change": _safe_float(data.get("Change")) / 1000,
            # RatioChange is DECIMAL(10,4) — already a percentage value (e.g. 0.85 = 0.85%)
            "ratio_change": _safe_float(data.get("RatioChange")),
            "volume": _safe_int(data.get("TotalVol")),
            "value": _safe_float(data.get("TotalVal")),
            "time": data.get("Time", ""),
        }
    except Exception as e:
        logger.warning("Failed to parse trade message: %s", e)
        return None


def _parse_quote(content_str: str) -> QuoteMessage | None:
    """Parse a quote message from market_data_quote topic."""
    try:
        data = json.loads(content_str)
        symbol = data.get("Symbol", "")
        if not symbol:
            return None
        bids = []
        asks = []
        for i in range(1, 11):
            bp = data.get(f"BidPrice{i}")
            bv = data.get(f"BidVol{i}")
            ap = data.get(f"AskPrice{i}")
            av = data.get(f"AskVol{i}")
            if bp is not None and bv is not None:
                bids.append({"price": _safe_float(bp), "volume": _safe_int(bv)})
            if ap is not None and av is not None:
                asks.append({"price": _safe_float(ap), "volume": _safe_int(av)})
        return {
            "type": "orderbook_update",
            "symbol": symbol,
            "bids": bids[:10],
            "asks": asks[:10],
            "time": data.get("Time", ""),
        }
    except Exception as e:
        logger.warning("Failed to parse quote message: %s", e)
        return None


def _parse_index(content_str: str) -> IndexMessage | None:
    """Parse an index message from index_data topic."""
    try:
        data = json.loads(content_str)
        index_id = data.get("IndexId", "")
        if not index_id:
            return None
        return {
            "type": "index_update",
            "index_id": index_id,
            "index_value": _safe_float(data.get("IndexValue")),
            # Change: integer × 1000 (e.g. 151 = 0.151 points)
            "change": _safe_float(data.get("Change")) / 1000,
            # RatioChange: DECIMAL(10,4) — already a percentage (e.g. 1.5 = 1.5%)
            "ratio_change": _safe_float(data.get("RatioChange")),
            "advances": _safe_int(data.get("Advances")),
            "declines": _safe_int(data.get("Declines")),
            "time": data.get("Time", ""),
        }
    except Exception as e:
        logger.warning("Failed to parse index message: %s", e)
        return None


PARSERS: dict[str, Callable[[str], WebSocketMessage | None]] = {
    "market_data_trade": _parse_trade,
    "market_data_quote": _parse_quote,
    "index_data": _parse_index,
}



async def kafka_bridge_loop() -> None:
    """
    Runs as a background asyncio task (started in FastAPI lifespan).
    Consumes all topics and forwards messages to the WS manager.
    Includes automatic retry with exponential backoff on failures.
    """
    consecutive_errors = 0
    max_backoff_seconds = 30

    while True:
        try:
            consumer = AIOKafkaConsumer(
                *TOPICS,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id="api_websocket_bridge",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )

            try:
                await consumer.start()
                logger.info("Kafka bridge started, consuming topics: %s", TOPICS)
                consecutive_errors = 0  # reset on successful start
            except Exception as e:
                logger.error("Failed to start Kafka consumer: %s", e)
                backoff = min(2 ** consecutive_errors, max_backoff_seconds)
                await asyncio.sleep(backoff)
                consecutive_errors += 1
                continue

            try:
                async for msg in consumer:
                    topic = msg.topic
                    try:
                        raw = msg.value
                        ws_msg: WebSocketMessage | None = None

                        if topic == "candlestick_updates":
                            # Candlestick messages are sent directly without Content wrapper
                            if isinstance(raw, dict):
                                ws_msg = raw
                        elif isinstance(raw, dict) and "Content" in raw:
                            content_str = raw.get("Content", "")
                            if content_str:
                                parser = PARSERS.get(topic)
                                if parser:
                                    ws_msg = parser(content_str)

                        if not ws_msg:
                            continue

                        msg_type = ws_msg.get("type", "")
                        symbol = ws_msg.get("symbol", "")

                        if msg_type == "candlestick_update" and symbol:
                            await ws_manager.broadcast_to_symbol(symbol, ws_msg)
                        elif msg_type == "price_update" and symbol:
                            # Invalidate the /stocks cache so next poll picks up the new tick immediately.
                            await stock_cache.invalidate_all()
                            # Only broadcast to clients watching this specific symbol.
                            await ws_manager.broadcast_to_symbol(symbol, ws_msg)
                        elif msg_type == "orderbook_update" and symbol:
                            await ws_manager.broadcast_to_symbol(symbol, ws_msg)
                        elif msg_type == "index_update":
                            await ws_manager.broadcast_all(ws_msg)

                    except Exception as e:
                        logger.warning("Error processing Kafka message: %s", e)

            except Exception as e:
                logger.error("Kafka consumer loop error: %s", e)
                raise
            finally:
                await consumer.stop()
                logger.info("Kafka bridge stopped")

        except Exception as e:
            logger.error("Bridge crashed, restarting in 5s: %s", e)
            consecutive_errors += 1
            backoff = min(5, max_backoff_seconds)  # Fixed 5s for now, could be exponential
            await asyncio.sleep(backoff)
