"""
consumer_unified.py
==================
Runs all 6 Kafka consumers in parallel threads inside a single container:
  5 topic consumers (market_data_trade/quote, index_data, foreign_room_data,
  securities_status) + CandlestickConsumer (1m/1d OHLCV pre-computation).
  + TradeMatchArchiveThread (daily trade match archive writer).
Each thread writes to the streaming MySQL DB.
"""

import os
import sys
import json
import time
import signal
import logging
import threading
from datetime import datetime, timedelta, time as dtime, timezone
from logging.handlers import RotatingFileHandler
from kafka import KafkaConsumer
import pymysql
from http.server import HTTPServer, BaseHTTPRequestHandler

from candlestick import CandlestickConsumer

# ── Vietnam market hours (UTC+7) ───────────────────────────────────────────────
MARKET_OPEN  = dtime(9, 15)   # 9:15 AM UTC+7 — archive display starts
MARKET_CLOSE = dtime(15, 30)  # 3:30 PM UTC+7 — archive switches on
RESET_HOUR   = 9              # 9:00 AM UTC+7 — next-day archive clear

def _now() -> datetime:
    """Return current Vietnam-time datetime."""
    return datetime.now(timezone.utc) + timedelta(hours=7)

def _vn_time_to_str(dt: datetime) -> str:
    """Format a Vietnam-time datetime as HH:MM:SS string."""
    vn = _now()  # local, offset-aware enough
    return dt.strftime("%H:%M:%S")

def _date_offset(dt: datetime, days: int) -> datetime:
    return dt + timedelta(days=days)

def _is_trading_hours(now_vn: datetime) -> bool:
    t = now_vn.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE

def _is_reset_time(now_vn: datetime) -> bool:
    """True when current time is exactly 9:00 AM — triggers daily reset."""
    return now_vn.hour == RESET_HOUR and now_vn.minute == 0 and now_vn.second < 30


# ── Topics → INSERT SQL ───────────────────────────────────────────────────────

INSERT_INDEX = """
    INSERT INTO data.index_data (
        index_id, index_value, prior_index_value,
        trading_date, time, total_trade, total_qtty,
        total_value, index_name, advances, nochanges,
        declines, ceilings, floors, `change`, ratio_change,
        total_qtty_pt, total_value_pt, exchange, all_qtty,
        all_value, index_type, trading_session, market_id,
        rtype, total_qtty_od, total_value_od
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

INSERT_QUOTE = """
    INSERT INTO data.data_quote (
        trading_date, time, exchange, symbol_id, rtype, trading_session,
        ask_price1,ask_vol1,ask_price2,ask_vol2,ask_price3,ask_vol3,
        ask_price4,ask_vol4,ask_price5,ask_vol5,ask_price6,ask_vol6,
        ask_price7,ask_vol7,ask_price8,ask_vol8,ask_price9,ask_vol9,
        ask_price10,ask_vol10,
        bid_price1,bid_vol1,bid_price2,bid_vol2,bid_price3,bid_vol3,
        bid_price4,bid_vol4,bid_price5,bid_vol5,bid_price6,bid_vol6,
        bid_price7,bid_vol7,bid_price8,bid_vol8,bid_price9,bid_vol9,
        bid_price10,bid_vol10
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

INSERT_TRADE = """
    INSERT INTO data.data_trade (
        rtype, trading_date, time, isin, symbol,
        ceiling, `floor`, ref_price, avg_price, prior_val,
        last_price, last_vol, total_val, total_vol,
        market_id, exchange, trading_session, trading_status,
        `change`, ratio_change, est_matched_price, highest, lowest, side
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

INSERT_FOREIGN = """
    INSERT INTO data.foreign_room (
        rtype, trading_date, time, isin, symbol,
        total_room, current_room, buy_vol, sell_vol,
        buy_val, sell_val, market_id, exchange
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

INSERT_STATUS = """
    INSERT INTO data.securities_status (
        rtype, market_id, trading_date, time, symbol_id,
        trading_session, trading_status, exchange, trading_ol_session
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

INSERT_TRADE_MATCH = """
    INSERT INTO data.trade_match_archive
        (trading_date, `time`, symbol, price, volume, side, price_change)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        price = VALUES(price),
        volume = VALUES(volume)
"""

TOPIC_CONFIG = {
    "market_data_trade": {
        "insert_sql": INSERT_TRADE,
        "batch_size": 50_000,
        "parser": "_parse_trade",
    },
    "market_data_quote": {
        "insert_sql": INSERT_QUOTE,
        "batch_size": 50_000,
        "parser": "_parse_quote",
    },
    "index_data": {
        "insert_sql": INSERT_INDEX,
        "batch_size": 50_000,
        "parser": "_parse_index",
    },
    "foreign_room_data": {
        "insert_sql": INSERT_FOREIGN,
        "batch_size": 50_000,
        "parser": "_parse_foreign",
    },
    "securities_status": {
        "insert_sql": INSERT_STATUS,
        "batch_size": 50_000,
        "parser": "_parse_status",
    },
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def setup_logger(name: str) -> logging.Logger:
    log_dir = os.getenv("LOG_DIR", "/streamflow/logs")
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def connect_db():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", 3306)),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "stream_flow"),
        database="data",
        charset="utf8mb4",
        autocommit=False,
    )


def _parse_date(raw):
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y").date().isoformat()
    except Exception:
        return None


# ── Parsers ─────────────────────────────────────────────────────────────────

def _parse_trade(content_str: str) -> tuple | None:
    data = json.loads(content_str)

    # Only pass buy-aggressor trades (Side='BU') — skip SD (seller-initiated)
    # to avoid double-counting each matched order. SSI uses BU/SD, not M/B.
    side = str(data.get("Side") or "").strip().upper()
    if side and side != "BU":
        return None

    data["TradingDate"] = _parse_date(data.get("TradingDate"))
    return (
        data.get("RType"), data.get("TradingDate"), data.get("Time"), data.get("Isin"),
        data.get("Symbol"), data.get("Ceiling"), data.get("Floor"), data.get("RefPrice"),
        data.get("AvgPrice"), data.get("PriorVal"), data.get("LastPrice"), data.get("LastVol"),
        data.get("TotalVal"), data.get("TotalVol"), data.get("MarketId"), data.get("Exchange"),
        data.get("TradingSession"), data.get("TradingStatus"), data.get("Change"),
        data.get("RatioChange"), data.get("EstMatchedPrice"), data.get("Highest"),
        data.get("Lowest"), data.get("Side"),
    )


def _parse_quote(content_str: str) -> tuple | None:
    data = json.loads(content_str)
    data["TradingDate"] = _parse_date(data.get("TradingDate"))
    return (
        data.get("TradingDate"), data.get("Time"), data.get("Exchange"), data.get("Symbol"),
        data.get("RType"), data.get("TradingSession"),
        data.get("AskPrice1"), data.get("AskVol1"),
        data.get("AskPrice2"), data.get("AskVol2"),
        data.get("AskPrice3"), data.get("AskVol3"),
        data.get("AskPrice4"), data.get("AskVol4"),
        data.get("AskPrice5"), data.get("AskVol5"),
        data.get("AskPrice6"), data.get("AskVol6"),
        data.get("AskPrice7"), data.get("AskVol7"),
        data.get("AskPrice8"), data.get("AskVol8"),
        data.get("AskPrice9"), data.get("AskVol9"),
        data.get("AskPrice10"), data.get("AskVol10"),
        data.get("BidPrice1"), data.get("BidVol1"),
        data.get("BidPrice2"), data.get("BidVol2"),
        data.get("BidPrice3"), data.get("BidVol3"),
        data.get("BidPrice4"), data.get("BidVol4"),
        data.get("BidPrice5"), data.get("BidVol5"),
        data.get("BidPrice6"), data.get("BidVol6"),
        data.get("BidPrice7"), data.get("BidVol7"),
        data.get("BidPrice8"), data.get("BidVol8"),
        data.get("BidPrice9"), data.get("BidVol9"),
        data.get("BidPrice10"), data.get("BidVol10"),
    )


def _parse_index(content_str: str) -> tuple | None:
    data = json.loads(content_str)
    data["TradingDate"] = _parse_date(data.get("TradingDate"))
    return (
        data.get("IndexId"), data.get("IndexValue"), data.get("PriorIndexValue"),
        data.get("TradingDate"), data.get("Time"), data.get("TotalTrade"), data.get("TotalQtty"),
        data.get("TotalValue"), data.get("IndexName"), data.get("Advances"), data.get("NoChanges"),
        data.get("Declines"), data.get("Ceilings"), data.get("Floors"), data.get("Change"),
        data.get("RatioChange"), data.get("TotalQttyPt"), data.get("TotalValuePt"), data.get("Exchange"),
        data.get("AllQty"), data.get("AllValue"), data.get("IndexType"), data.get("TradingSession"),
        data.get("MarketId"), data.get("RType"), data.get("TotalQttyOd"), data.get("TotalValueOd"),
    )


def _parse_foreign(content_str: str) -> tuple | None:
    data = json.loads(content_str)
    data["TradingDate"] = _parse_date(data.get("TradingDate"))
    return (
        data.get("RType"), data.get("TradingDate"), data.get("Time"), data.get("Isin"),
        data.get("Symbol"), data.get("TotalRoom"), data.get("CurrentRoom"),
        data.get("BuyVol"), data.get("SellVol"), data.get("BuyVal"), data.get("SellVal"),
        data.get("MarketId"), data.get("Exchange"),
    )


def _parse_status(content_str: str) -> tuple | None:
    data = json.loads(content_str)
    data["TradingDate"] = _parse_date(data.get("TradingDate"))
    return (
        data.get("RType"), data.get("MarketId"), data.get("TradingDate"), data.get("Time"),
        data.get("Symbol"), data.get("TradingSession"), data.get("TradingStatus"),
        data.get("Exchange"), data.get("TradingOlSession"),
    )


PARSERS = {
    "market_data_trade": _parse_trade,
    "market_data_quote": _parse_quote,
    "index_data": _parse_index,
    "foreign_room_data": _parse_foreign,
    "securities_status": _parse_status,
}


# ── Health Check HTTP Server ───────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    """Simple health check endpoint that returns thread status."""

    def do_GET(self):
        if self.path == "/health":
            # Check all consumer threads
            all_threads = threading.enumerate()
            consumer_threads = [t for t in all_threads if t.name.startswith("Consumer-") or t.name in ("CandlestickConsumer", "TradeMatchArchive")]
            healthy = all(t.is_alive() for t in consumer_threads)

            status = 200 if healthy else 503
            payload = {
                "status": "ok" if healthy else "unhealthy",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                "threads": [
                    {"name": t.name, "alive": t.is_alive()}
                    for t in consumer_threads
                ]
            }

            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload, indent=2).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress logging for health checks
        pass


def start_health_server(port: int = 8080):
    """Start health check HTTP server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="HealthServer")
    thread.start()
    return server


# ── Consumer Thread ───────────────────────────────────────────────────────────

class ConsumerThread(threading.Thread):
    """Runs a single Kafka consumer → MySQL in its own thread."""

    def __init__(self, topic: str, insert_sql: str, batch_size: int):
        super().__init__(daemon=True, name=f"Consumer-{topic}")
        self.topic = topic
        self.insert_sql = insert_sql
        self.batch_size = batch_size
        self._stop_event = threading.Event()
        self._conn = None
        self._cursor = None
        self._consumer = None

    def _connect_db(self):
        self._conn = connect_db()
        self._cursor = self._conn.cursor()

    def _connect_kafka(self):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        group_map = {
            "market_data_trade":    "consumer-trade-to-mysql",
            "market_data_quote":    "consumer-quote-to-mysql",
            "index_data":           "consumer-index-to-mysql",
            "foreign_room_data":    "consumer-foreign-to-mysql",
            "securities_status":    "consumer-status-to-mysql",
        }
        self._consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_map.get(self.topic, f"consumer-{self.topic}-to-mysql"),
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    def _reconnect_db(self):
        try:
            if self._cursor:
                self._cursor.close()
            if self._conn:
                self._conn.close()
        except Exception:
            pass
        self._connect_db()

    def run(self):
        logger = setup_logger(f"consumer-{self.topic}")
        logger.info("Starting consumer thread for topic: %s", self.topic)

        self._connect_db()
        self._connect_kafka()

        batch = []
        parse_fn = PARSERS.get(self.topic)

        while not self._stop_event.is_set():
            try:
                raw_msgs = self._consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                if not raw_msgs:
                    if batch:
                        self._flush(batch, logger)
                        batch.clear()
                    continue

                for tp, messages in raw_msgs.items():
                    for msg in messages:
                        try:
                            content_str = msg.value["Content"]
                            record = parse_fn(content_str) if parse_fn else None
                            if record:
                                batch.append(record)
                        except Exception as e:
                            logger.error("Parse error: %s", e)

                if len(batch) >= self.batch_size or batch:
                    self._flush(batch, logger)
                    batch.clear()

            except Exception as e:
                logger.error("Poll error: %s", e)
                time.sleep(2)
                try:
                    self._reconnect_db()
                except Exception:
                    pass

        # Final flush
        if batch:
            self._flush(batch, logger)
        self._consumer.close()
        self._cursor.close()
        self._conn.close()
        logger.info("Consumer thread for %s stopped.", self.topic)

    def _flush(self, batch: list, logger: logging.Logger):
        try:
            self._cursor.executemany(self.insert_sql, batch)
            self._conn.commit()
            self._consumer.commit()
            logger.info("Inserted %d records into %s", len(batch), self.topic)
        except Exception as e:
            logger.error("Flush error on %s: %s", self.topic, e)
            self._conn.rollback()

    def stop(self):
        self._stop_event.set()


# ── Trade Match Archive Thread ───────────────────────────────────────────────
# Listens on market_data_trade, strips raw messages to symbol/price/volume/side
# and writes one row per matched trade to data.trade_match_archive.
#
# Automatic lifecycle:
#   • Active (9:15 AM – 3:30 PM UTC+7): batches and writes every tick.
#   • 3:30 PM trigger: flushes remaining batch, archive becomes queryable.
#   • 9:00 AM next day: DELETE today's rows before new session begins.
#   • Outside market hours: idles (skips messages, flushes only on shutdown).

class TradeMatchArchiveThread(threading.Thread):
    """
    Dedicated thread that writes every matched-trade tick to data.trade_match_archive.

    Deduplication: INSERT ... ON DUPLICATE KEY UPDATE on (trading_date, `time`, symbol)
    ensures that in case of consumer restart / re-processing, each tick appears exactly once.

    Reset: DELETE WHERE trading_date = today at 9:00 AM so the archive is always
    a single clean session (9:15 AM – 3:30 PM).
    """

    BATCH_SIZE       = 200          # rows before a forced flush
    FLUSH_INTERVAL_S = 5            # seconds between periodic flushes
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    def __init__(self):
        super().__init__(daemon=True, name="TradeMatchArchive")
        self._stop       = threading.Event()
        self._conn       = None
        self._cursor     = None
        self._consumer   = None
        self._batch: list = []
        self._last_flush = time.time()
        self._today_date = ""       # today's archive trading date (YYYY-MM-DD)
        self._closing    = False    # True once 3:30 PM flush has been done today

    def _connect_db(self):
        self._conn   = connect_db()
        self._cursor = self._conn.cursor()

    def _connect_kafka(self):
        self._consumer = KafkaConsumer(
            "market_data_trade",
            bootstrap_servers=self.BOOTSTRAP_SERVERS,
            group_id="consumer-trade-match-archive",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

    def _reconnect_db(self):
        try:
            self._cursor.close()
            self._conn.close()
        except Exception:
            pass
        self._connect_db()

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _refresh_today(self, now_vn: datetime) -> None:
        """Update _today_date; trigger daily reset if it's 9:00 AM."""
        new_date = now_vn.strftime("%Y-%m-%d")

        if new_date != self._today_date:
            if self._today_date:
                # We crossed midnight — clear stale closing flag
                self._closing = False
                self.logger.info(
                    "Date changed from %s to %s — closing flag reset.",
                    self._today_date, new_date,
                )
            self._today_date = new_date

        # Daily reset: 9:00 AM — delete today's rows so the new session is clean
        if _is_reset_time(now_vn) and not getattr(self, "_reset_done", False):
            self._do_daily_reset()
            self._reset_done = True

        # If we stepped past 9:00 AM and somehow missed unsetting the flag
        if now_vn.hour > RESET_HOUR:
            self._reset_done = False

    def _do_daily_reset(self) -> None:
        """DELETE all rows for today's date (from the *previous* session)."""
        try:
            yesterday = (datetime.now(timezone.utc) + timedelta(hours=7) - timedelta(days=1)).strftime("%Y-%m-%d")
            self._cursor.execute(
                "DELETE FROM data.trade_match_archive WHERE trading_date = %s",
                (yesterday,),
            )
            self._conn.commit()
            self.logger.info("Daily reset: deleted trade_match_archive rows for %s", yesterday)
        except Exception as e:
            self.logger.error("Daily reset DELETE failed: %s", e)
            self._conn.rollback()

    def _process_msg(self, msg) -> None:
        """Extract symbol/price/vol/side from a trade message and enqueue it."""
        try:
            content_str = msg.value.get("Content", "")
            if not content_str:
                return
            data = json.loads(content_str)
        except Exception:
            return

        trading_date = _parse_date(data.get("TradingDate"))
        if not trading_date:
            return

        side_raw = str(data.get("Side") or "").strip().upper()
        if side_raw not in ("BU", "SD"):
            return

        try:
            price = float(data.get("LastPrice") or 0)
            vol   = int(data.get("LastVol") or 0)
        except (TypeError, ValueError):
            return

        if price <= 0 or vol <= 0:
            return

        self._batch.append((
            trading_date,                    # trading_date
            data.get("Time") or "00:00:00", # time
            data.get("Symbol") or "",        # symbol
            price,                           # price
            vol,                             # volume
            "buy" if side_raw == "BU" else "sell",  # side
            float(data.get("Change") or 0) or None,  # price_change
        ))

    def _flush(self) -> None:
        if not self._batch:
            return
        try:
            self._cursor.executemany(INSERT_TRADE_MATCH, self._batch)
            self._conn.commit()
            self._consumer.commit()
            self.logger.info(
                "Archived %d trade matches to trade_match_archive (date=%s)",
                len(self._batch), self._today_date,
            )
        except Exception as e:
            self.logger.error("Archive flush error: %s", e)
            self._conn.rollback()
        finally:
            self._batch.clear()
            self._last_flush = time.time()

    # ── Main loop ────────────────────────────────────────────────────────────

    def run(self) -> None:
        self.logger = setup_logger("trade-match-archive")
        self.logger.info("TradeMatchArchive thread starting...")

        self._connect_db()
        self._connect_kafka()

        while not self._stop.is_set():
            try:
                now_vn = _now()
                self._refresh_today(now_vn)

                # ── Idle outside market hours ────────────────────────────────
                if not _is_trading_hours(now_vn):
                    # Still flush periodically to avoid holding a large batch
                    if self._batch and (time.time() - self._last_flush) >= self.FLUSH_INTERVAL_S:
                        self._flush()
                    time.sleep(5)
                    continue

                # ── At 3:30 PM: trigger closing flush ───────────────────────
                t = now_vn.time()
                if (t.hour == 15 and t.minute == 30 and not self._closing):
                    self.logger.info("Market close (3:30 PM) — final flush of trade_match_archive.")
                    self._flush()
                    self._closing = True

                # ── Poll Kafka ───────────────────────────────────────────────
                raw_msgs = self._consumer.poll(timeout_ms=2000, max_records=self.BATCH_SIZE)
                for tp, messages in raw_msgs.items():
                    for msg in messages:
                        self._process_msg(msg)

                # ── Flush on batch size ────────────────────────────────────
                if len(self._batch) >= self.BATCH_SIZE:
                    self._flush()
                # ── Flush on timer ─────────────────────────────────────────
                elif self._batch and (time.time() - self._last_flush) >= self.FLUSH_INTERVAL_S:
                    self._flush()

            except Exception as e:
                self.logger.error("Archive loop error: %s", e)
                time.sleep(5)
                try:
                    self._reconnect_db()
                except Exception:
                    pass

        # ── Shutdown: final flush ──────────────────────────────────────────
        self.logger.info("TradeMatchArchive shutting down — final flush.")
        self._flush()
        self._consumer.close()
        self._cursor.close()
        self._conn.close()
        self.logger.info("TradeMatchArchive stopped.")

    def stop(self) -> None:
        self._stop.set()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    threads: list[threading.Thread] = []

    def shutdown(signum, frame):
        print("\nShutdown signal received. Stopping all consumer threads...")
        for t in threads:
            if hasattr(t, "stop"):
                t.stop()
        for t in threads:
            t.join(timeout=10)
        print("All consumer threads stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── Start health check server ─────────────────────────────────────────────
    health_port = int(os.getenv("HEALTH_PORT", "8080"))
    health_server = start_health_server(health_port)
    print(f"Health check server started on port {health_port}")

    # ── CandlestickConsumer ────────────────────────────────────────────────
    candlestick = CandlestickConsumer()
    candlestick.start()
    threads.append(candlestick)
    time.sleep(0.5)

    # ── Trade Match Archive ───────────────────────────────────────────────
    archive = TradeMatchArchiveThread()
    archive.start()
    threads.append(archive)
    time.sleep(0.5)

    # ── 5 topic consumers ────────────────────────────────────────────────
    for topic, cfg in TOPIC_CONFIG.items():
        t = ConsumerThread(topic, cfg["insert_sql"], cfg["batch_size"])
        t.start()
        threads.append(t)
        time.sleep(0.5)

    total = len(threads)
    print(f"All {total} consumer threads started successfully!")
    print(f"Topics: {', '.join(TOPIC_CONFIG.keys())} + candlestick + trade-match-archive")
    print("Press Ctrl+C to stop all consumers.\n")

    try:
        while True:
            time.sleep(10)
            # Check thread health - exit if any thread died unexpectedly
            for t in threads:
                if not t.is_alive():
                    print(f"ERROR: Thread {t.name} died! Shutting down.")
                    shutdown(None, None)
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
