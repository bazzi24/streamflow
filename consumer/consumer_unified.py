"""
consumer_unified.py
==================
Runs all 6 Kafka consumers in parallel threads inside a single container:
  5 topic consumers (market_data_trade/quote, index_data, foreign_room_data,
  securities_status) + CandlestickConsumer (1m/1d OHLCV pre-computation).
Each thread writes to the streaming MySQL DB.
"""

import os
import sys
import json
import time
import signal
import logging
import threading
from datetime import datetime
from logging.handlers import RotatingFileHandler
from kafka import KafkaConsumer
import pymysql

from candlestick import CandlestickConsumer

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


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    threads: list[threading.Thread] = []

    def shutdown(signum, frame):
        print("\nShutdown signal received. Stopping all consumer threads...")
        for t in threads:
            if hasattr(t, "stop"):
                t.stop()
            elif hasattr(t, "_stop"):
                t._stop.set()
        for t in threads:
            t.join(timeout=10)
        print("All consumer threads stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── CandlestickConsumer ────────────────────────────────────────────────
    candlestick = CandlestickConsumer()
    candlestick.start()
    threads.append(candlestick)
    time.sleep(0.5)

    # ── 5 topic consumers ─────────────────────────────────────────────────
    for topic, cfg in TOPIC_CONFIG.items():
        t = ConsumerThread(topic, cfg["insert_sql"], cfg["batch_size"])
        t.start()
        threads.append(t)
        time.sleep(0.5)

    total = len(threads)
    print(f"All {total} consumer threads started successfully!")
    print(f"Topics: {', '.join(TOPIC_CONFIG.keys())} + candlestick")
    print("Press Ctrl+C to stop all consumers.\n")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
