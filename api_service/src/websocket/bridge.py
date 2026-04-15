import asyncio
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
from aiokafka import AIOKafkaConsumer
import pymysql
from . import ws_manager as _ws_mod
ws_manager = _ws_mod
from ..services.stock_cache import stock_cache
from ..config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

# NOTE: candlestick_1m is a MySQL table (written by CandlestickConsumer),
# NOT a Kafka topic. Candlestick updates are served by the MySQL poll loop
# below, not by Kafka.
TOPICS = [
    "market_data_trade",
    "market_data_quote",
    "index_data",
]

CANDLESTICK_POLL_INTERVAL_SEC = 10  # how often to poll MySQL for new 1m bars

# Parse streaming_db_url for pymysql candlestick poller
_streaming_url = urlparse(settings.streaming_db_url.replace("mysql+pymysql://", "http://"))
_STREAMING_HOST = _streaming_url.hostname or "mysql"
_STREAMING_PORT = _streaming_url.port or 3306


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


def _parse_trade(content_str: str) -> dict | None:
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


def _parse_quote(content_str: str) -> dict | None:
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


def _parse_index(content_str: str) -> dict | None:
    """Parse an index message from index_data topic."""
    try:
        data = json.loads(content_str)
        index_id = data.get("IndexId", "")
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


PARSERS: dict[str, callable] = {
    "market_data_trade": _parse_trade,
    "market_data_quote": _parse_quote,
    "index_data": _parse_index,
}


# ── MySQL candlestick polling ─────────────────────────────────────────────────

def _connect_streaming_db():
    """Create a raw pymysql connection to the data DB (for candlestick polling)."""
    return pymysql.connect(
        host=_STREAMING_HOST,
        port=_STREAMING_PORT,
        user=settings.db_user,
        password=settings.db_password,
        database="data",
        charset="utf8mb4",
        autocommit=True,
    )


async def _poll_candlesticks() -> None:
    """
    Background coroutine that polls data.candlestick_1m every CANDLESTICK_POLL_INTERVAL_SEC
    for newly-closed 1m bars and broadcasts them as WebSocket candlestick_update messages.

    Tracks the last-seen (symbol, time_start) per symbol so it only emits each bar once.
    """
    last_seen: dict[str, datetime] = {}

    while True:
        try:
            conn = _connect_streaming_db()
            cur = conn.cursor()

            # Fetch all bars closed in the last 2 poll intervals (catches any that
            # arrived between polls; deduplicated by last_seen check below).
            cutoff = datetime.now() - timedelta(seconds=CANDLESTICK_POLL_INTERVAL_SEC * 2)
            cur.execute(
                """
                SELECT symbol, time_start, open, high, low, close, volume
                FROM data.candlestick_1m
                WHERE time_start > %s
                ORDER BY symbol, time_start
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()

            for (symbol, time_start, open_, high, low, close, volume) in rows:
                last = last_seen.get(symbol)
                if last is not None and time_start <= last:
                    # Already emitted this bar (or a newer one).
                    continue
                last_seen[symbol] = time_start

                try:
                    await ws_manager.broadcast_to_symbol(symbol, {
                        "type": "candlestick_update",
                        "symbol": symbol,
                        "interval": "1m",
                        "timestamp": int(time_start.timestamp() * 1000),
                        "open": float(open_) if open_ is not None else 0.0,
                        "high": float(high) if high is not None else 0.0,
                        "low": float(low) if low is not None else 0.0,
                        "close": float(close) if close is not None else 0.0,
                        "volume": int(volume) if volume is not None else 0,
                    })
                except Exception as e:
                    logger.warning("WS broadcast error for %s: %s", symbol, e)

        except Exception as e:
            logger.warning("Candlestick poll error: %s", e)

        await asyncio.sleep(CANDLESTICK_POLL_INTERVAL_SEC)


async def kafka_bridge_loop() -> None:
    """
    Runs as a background asyncio task (started in FastAPI lifespan).
    Consumes all topics and forwards messages to the WS manager.
    """
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
    except Exception as e:
        logger.error("Failed to start Kafka consumer: %s", e)
        return

    # Run MySQL candlestick poller concurrently alongside Kafka consumer.
    asyncio.create_task(_poll_candlesticks())
    logger.info("Candlestick MySQL poller started (interval=%ds)", CANDLESTICK_POLL_INTERVAL_SEC)

    try:
        async for msg in consumer:
            topic = msg.topic
            try:
                raw = msg.value
                ws_msg: dict | None = None

                if isinstance(raw, dict) and "Content" in raw:
                    content_str = raw.get("Content", "")
                    if content_str:
                        parser = PARSERS.get(topic)
                        if parser:
                            ws_msg = parser(content_str)

                if not ws_msg:
                    continue

                msg_type = ws_msg.get("type", "")
                symbol = ws_msg.get("symbol", "")

                if msg_type == "price_update" and symbol:
                    # Invalidate the /stocks cache so next poll picks up the new tick immediately.
                    await stock_cache.invalidate_all()
                    await ws_manager.broadcast_to_symbol(symbol, ws_msg)
                    await ws_manager.broadcast_all(ws_msg)
                elif msg_type == "orderbook_update" and symbol:
                    await ws_manager.broadcast_to_symbol(symbol, ws_msg)
                elif msg_type == "index_update":
                    await ws_manager.broadcast_all(ws_msg)

            except Exception as e:
                logger.warning("Error processing Kafka message: %s", e)
    finally:
        await consumer.stop()
        logger.info("Kafka bridge stopped")
