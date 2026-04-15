"""
consumer/candlestick.py
========================
CandlestickConsumer — pre-computes 1m and 1d OHLCV candles from `market_data_trade`
and upserts them into MySQL `data.candlestick_1m` / `data.candlestick_1d`.

Design
------
- In-memory rolling window per symbol (no external state store).
- Upsert with `INSERT ... ON DUPLICATE KEY UPDATE` — non-blocking, no delete/re-insert.
- 1m table is source of truth; larger timeframes derived at query time (Step 5).
- 1d candle includes foreign-room stats (nn_mua, nn_ban, room) from the latest
  foreign_room row for that symbol on the closing date.
- On startup, the consumer hydrates its in-memory state from the DB so it doesn't
  overwrite bars that were already written by a previous run.

Roll
----
- A bar is "closed" when the next tick arrives in a later 1m bucket (or at shutdown).
- A 1d bar is closed when a tick arrives with a different trading_date than the one
  stored for that symbol (or at shutdown).

Risk: Row locks from concurrent upserts — mitigated by batching every 1s or when the
in-memory buffer reaches 200 symbols.
"""

import os
import json
import time
import signal
import logging
import threading
import sys
from datetime import datetime, timedelta, date
from typing import Optional
from logging.handlers import RotatingFileHandler

from kafka import KafkaConsumer
import pymysql

# ── SQL ───────────────────────────────────────────────────────────────────────

UPSERT_1M = """
    INSERT INTO data.candlestick_1m (symbol, time_start, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open   = IF(VALUES(open)   IS NOT NULL AND VALUES(open)   <> 0, VALUES(open),   open),
        high   = IF(VALUES(high)   >  high,                         VALUES(high),         high),
        low    = IF(VALUES(low)    <  low  OR  VALUES(low)   = 0,    VALUES(low),          low),
        close  = VALUES(close),
        volume = volume + VALUES(volume)
"""

UPSERT_1D = """
    INSERT INTO data.candlestick_1d
        (symbol, trading_date, open, high, low, close, volume, nn_mua, nn_ban, room)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open   = IF(VALUES(open)   IS NOT NULL AND VALUES(open)   <> 0, VALUES(open),   open),
        high   = IF(VALUES(high)   >  high,                         VALUES(high),         high),
        low    = IF(VALUES(low)    <  low  OR  VALUES(low)   = 0,    VALUES(low),          low),
        close  = VALUES(close),
        volume = volume + VALUES(volume),
        nn_mua = IF(VALUES(nn_mua) > 0, VALUES(nn_mua), nn_mua),
        nn_ban = IF(VALUES(nn_ban) > 0, VALUES(nn_ban), nn_ban),
        room   = IF(VALUES(room)   > 0, VALUES(room),   room)
"""

# ── Logging ──────────────────────────────────────────────────────────────────

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


# ── Candle State ──────────────────────────────────────────────────────────────

class CandleState:
    """In-memory rolling OHLCV for one symbol's current 1m + 1d bars."""

    __slots__ = (
        "symbol", "cur_1m_start", "cur_1m_open", "cur_1m_high",
        "cur_1m_low", "cur_1m_close", "cur_1m_vol",
        "cur_1d_date", "cur_1d_open", "cur_1d_high",
        "cur_1d_low", "cur_1d_close", "cur_1d_vol",
    )

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.cur_1m_start: Optional[datetime] = None
        self.cur_1m_open: Optional[float] = None
        self.cur_1m_high: Optional[float] = None
        self.cur_1m_low: Optional[float] = None
        self.cur_1m_close: Optional[float] = None
        self.cur_1m_vol: int = 0

        self.cur_1d_date: Optional[date] = None
        self.cur_1d_open: Optional[float] = None
        self.cur_1d_high: Optional[float] = None
        self.cur_1d_low: Optional[float] = None
        self.cur_1d_close: Optional[float] = None
        self.cur_1d_vol: int = 0

    def update(self, price: float, vol: int, tick_dt: datetime, tick_date: date):
        """Update both 1m and 1d candles with a new tick."""
        # ── 1m bucket ────────────────────────────────────────────────────────
        bucket_start = tick_dt.replace(second=0, microsecond=0)
        if self.cur_1m_start is None or bucket_start > self.cur_1m_start:
            # New bucket — close previous bar before starting new one.
            self._emit_1m()

        if self.cur_1m_start is None:
            self.cur_1m_start = bucket_start
            self.cur_1m_open = price
            self.cur_1m_high = price
            self.cur_1m_low = price
            self.cur_1m_close = price
            self.cur_1m_vol = vol
        else:
            self.cur_1m_close = price
            self.cur_1m_high = max(self.cur_1m_high or price, price)
            self.cur_1m_low = min(self.cur_1m_low or price, price)
            self.cur_1m_vol += vol

        # ── 1d bucket ───────────────────────────────────────────────────────
        if self.cur_1d_date is None or tick_date > self.cur_1d_date:
            # New trading date — close previous 1d bar.
            self._emit_1d()
            self.cur_1d_date = tick_date
            self.cur_1d_open = price
            self.cur_1d_high = price
            self.cur_1d_low = price
            self.cur_1d_close = price
            self.cur_1d_vol = vol
        else:
            self.cur_1d_close = price
            self.cur_1d_high = max(self.cur_1d_high or price, price)
            self.cur_1d_low = min(self.cur_1d_low or price, price)
            self.cur_1d_vol += vol

    def _emit_1m(self):
        """Return the completed 1m bar as a dict, or None."""
        if self.cur_1m_start is None:
            return None
        return {
            "symbol": self.symbol,
            "time_start": self.cur_1m_start,
            "open": self.cur_1m_open,
            "high": self.cur_1m_high,
            "low": self.cur_1m_low,
            "close": self.cur_1m_close,
            "volume": self.cur_1m_vol,
        }

    def _emit_1d(self):
        """Return the completed 1d bar as a dict, or None."""
        if self.cur_1d_date is None:
            return None
        return {
            "symbol": self.symbol,
            "trading_date": self.cur_1d_date,
            "open": self.cur_1d_open,
            "high": self.cur_1d_high,
            "low": self.cur_1d_low,
            "close": self.cur_1d_close,
            "volume": self.cur_1d_vol,
        }

    def flush_all(self):
        """Yield all completed bars (1m + 1d) for final DB write at shutdown."""
        bar = self._emit_1m()
        if bar:
            yield "1m", bar
        bar = self._emit_1d()
        if bar:
            yield "1d", bar


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y").date()
    except Exception:
        return None


def _time_to_dt(time_str: str | None, trading_date: date) -> datetime | None:
    """Parse SSI time string 'HH:MM:SS' into a full datetime using trading_date."""
    if not time_str:
        return None
    try:
        h, m, s = time_str.split(":")
        return datetime(
            trading_date.year, trading_date.month, trading_date.day,
            int(h), int(m), int(s),
        )
    except Exception:
        return None


def _hydrate_from_db(conn) -> dict[str, CandleState]:
    """Load latest candle state per symbol so we resume from where we left off."""
    states: dict[str, CandleState] = {}
    cur = conn.cursor()

    # Latest 1m bar per symbol
    cur.execute("""
        SELECT symbol, time_start, open, high, low, close, volume
        FROM data.candlestick_1m
        WHERE (symbol, time_start) IN (
            SELECT symbol, MAX(time_start)
            FROM data.candlestick_1m
            GROUP BY symbol
        )
    """)
    for (symbol, time_start, open_, high, low, close, volume) in cur.fetchall():
        cs = CandleState(symbol)
        cs.cur_1m_start = time_start
        cs.cur_1m_open = float(open_) if open_ is not None else None
        cs.cur_1m_high = float(high) if high is not None else None
        cs.cur_1m_low = float(low) if low is not None else None
        cs.cur_1m_close = float(close) if close is not None else None
        cs.cur_1m_vol = int(volume) if volume else 0
        states[symbol] = cs

    # Latest 1d bar per symbol (patch cur_1d_date so next tick with new date closes it)
    cur.execute("""
        SELECT symbol, trading_date, open, high, low, close, volume
        FROM data.candlestick_1d
        WHERE (symbol, trading_date) IN (
            SELECT symbol, MAX(trading_date)
            FROM data.candlestick_1d
            GROUP BY symbol
        )
    """)
    for (symbol, trading_date, open_, high, low, close, volume) in cur.fetchall():
        if symbol not in states:
            states[symbol] = CandleState(symbol)
        cs = states[symbol]
        cs.cur_1d_date = trading_date
        cs.cur_1d_open = float(open_) if open_ is not None else None
        cs.cur_1d_high = float(high) if high is not None else None
        cs.cur_1d_low = float(low) if low is not None else None
        cs.cur_1d_close = float(close) if close is not None else None
        cs.cur_1d_vol = int(volume) if volume else 0

    cur.close()
    return states


# ── Main Consumer ─────────────────────────────────────────────────────────────

class CandlestickConsumer(threading.Thread):
    """
    Kafka consumer that reads `market_data_trade`, maintains in-memory 1m/1d
    rolling candles per symbol, and upserts completed bars to MySQL.

    Flushes:
      - When a new tick arrives in a later 1m bucket → emit previous 1m bar.
      - When a new tick has a different trading_date → emit previous 1d bar.
      - Every FLUSH_INTERVAL_SEC seconds → flush all in-flight bars.
      - On shutdown → flush everything.
    """

    BATCH_SIZE = 200          # symbols before a forced flush
    FLUSH_INTERVAL_SEC = 5    # periodic flush timer

    def __init__(self):
        super().__init__(daemon=True, name="CandlestickConsumer")
        self.logger = setup_logger("candlestick")
        self._stop = threading.Event()
        self._conn = None
        self._cursor = None
        self._consumer = None
        self._states: dict[str, CandleState] = {}
        self._pending_1m: list[tuple] = []
        self._pending_1d: list[tuple] = []
        self._last_flush = time.time()
        self._periodic_thread: threading.Thread | None = None

    def _connect_db(self):
        self._conn = connect_db()
        self._cursor = self._conn.cursor()
        self.logger.info("Connected to MySQL data DB.")

    def _connect_kafka(self):
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self._consumer = KafkaConsumer(
            "market_data_trade",
            bootstrap_servers=bootstrap_servers,
            group_id="consumer-candlestick",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self.logger.info("Connected to Kafka broker: %s", bootstrap_servers)

    def _reconnect_db(self):
        try:
            self._cursor.close()
            self._conn.close()
        except Exception:
            pass
        self._connect_db()

    # ── Processing ──────────────────────────────────────────────────────────────

    def _process_msg(self, msg) -> bool:
        """Return True if at least one bar was emitted."""
        try:
            content = msg.value["Content"]
            data = json.loads(content)
        except Exception as e:
            self.logger.error("Failed to parse message: %s", e)
            return False

        trading_date_raw = data.get("TradingDate")
        if not trading_date_raw:
            return False
        trading_date = _parse_date(trading_date_raw)
        if trading_date is None:
            return False

        time_str = data.get("Time")
        tick_dt = _time_to_dt(time_str, trading_date)
        if tick_dt is None:
            return False

        symbol = data.get("Symbol")
        if not symbol:
            return False

        try:
            price = float(data.get("LastPrice") or 0)
            vol = int(data.get("LastVol") or 0)
        except (TypeError, ValueError):
            return False

        if price <= 0 and vol <= 0:
            return False

        # Only count buy-aggressor trades (Side='BU' = Mua người mua) to avoid
        # double-counting: each matched order is reported from both the buyer's
        # and seller's perspective. Using only BU (buy-initiated / aggressor)
        # ensures each physical share trade is counted exactly once.
        # SSI uses BU (mua) and SD (bán) — NOT M/B.
        side = str(data.get("Side") or "").strip().upper()
        if side and side != "BU":
            return False

        # Get or create state
        if symbol not in self._states:
            self._states[symbol] = CandleState(symbol)

        state = self._states[symbol]

        # Detect bar closes BEFORE updating
        prev_1m = None
        prev_1d = None

        if state.cur_1m_start is not None:
            bucket = tick_dt.replace(second=0, microsecond=0)
            if bucket > state.cur_1m_start:
                prev_1m = state._emit_1m()

        if state.cur_1d_date is not None and trading_date > state.cur_1d_date:
            prev_1d = state._emit_1d()

        # Apply tick
        state.update(price, vol, tick_dt, trading_date)

        # Enqueue closed bars
        emitted = False
        if prev_1m:
            self._pending_1m.append((
                prev_1m["symbol"], prev_1m["time_start"],
                prev_1m["open"], prev_1m["high"], prev_1m["low"], prev_1m["close"], prev_1m["volume"],
            ))
            emitted = True

        if prev_1d:
            self._pending_1d.append((
                prev_1d["symbol"], prev_1d["trading_date"],
                prev_1d["open"], prev_1d["high"], prev_1d["low"], prev_1d["close"], prev_1d["volume"],
                0, 0, 0,   # nn_mua/nn_ban/room — filled by _fill_foreign_room()
            ))

        return emitted

    def _fill_foreign_room(self, conn):
        """Patch nn_mua/nn_ban/room into pending 1d records using the latest
        foreign_room row for each (symbol, trading_date)."""
        if not self._pending_1d:
            return

        cur = conn.cursor()
        for row in self._pending_1d:
            symbol = row[0]
            trading_date = row[1]
            try:
                cur.execute("""
                    SELECT buy_vol, sell_vol, current_room
                    FROM data.foreign_room
                    WHERE symbol = %s AND trading_date = %s
                    ORDER BY id DESC LIMIT 1
                """, (symbol, trading_date))
                fr = cur.fetchone()
                if fr:
                    # Update tuple in-place
                    self._pending_1d[self._pending_1d.index(row)] = (
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6],
                        int(fr[0]) if fr[0] else 0,
                        int(fr[1]) if fr[1] else 0,
                        int(fr[2]) if fr[2] else 0,
                    )
            except Exception as e:
                self.logger.warning(
                    "Could not fetch foreign_room for %s on %s: %s",
                    symbol, trading_date, e,
                )
        cur.close()

    def _flush(self):
        """Write pending 1m + 1d bars to MySQL.

        Before flushing, emit all in-progress 1m bars (bars with data but not yet
        closed by a tick in the next bucket). This ensures bars are written even
        when no new tick arrives to trigger the close (e.g., slow trading or
        after market close).
        """
        self._emit_inprogress_1m()

        if not self._pending_1m and not self._pending_1d:
            return

        try:
            if self._pending_1m:
                self._cursor.executemany(UPSERT_1M, self._pending_1m)
                self.logger.info("Upserted %d × 1m candles", len(self._pending_1m))

            if self._pending_1d:
                self._fill_foreign_room(self._conn)
                self._cursor.executemany(UPSERT_1D, self._pending_1d)
                self.logger.info("Upserted %d × 1d candles", len(self._pending_1d))

            self._conn.commit()
            self._consumer.commit()
            self._pending_1m.clear()
            self._pending_1d.clear()
            self._last_flush = time.time()

        except Exception as e:
            self.logger.error("Flush error: %s", e)
            self._conn.rollback()
            self._reconnect_db()

    def _emit_inprogress_1m(self):
        """Close and enqueue every in-progress 1m bar currently in memory.

        Called at the start of every flush so that in-progress bars (accumulated
        without being closed by a new tick) are written to MySQL even when
        trading is slow or the market is closed.
        """
        for symbol, state in list(self._states.items()):
            bar = state._emit_1m()
            if bar is None:
                continue
            self._pending_1m.append((
                bar["symbol"], bar["time_start"],
                bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"],
            ))
            # Reset in-memory state to start a fresh bar for the same minute.
            # If a real tick arrives later for the same minute, _process_msg
            # will re-open it via the ON DUPLICATE KEY UPDATE upsert.
            state.cur_1m_start = None
            state.cur_1m_open = None
            state.cur_1m_high = None
            state.cur_1m_low = None
            state.cur_1m_close = None
            state.cur_1m_vol = 0

    def _periodic_flush_loop(self):
        """Background thread: flush in-flight bars every FLUSH_INTERVAL_SEC."""
        while not self._stop.is_set():
            self._stop.wait(timeout=self.FLUSH_INTERVAL_SEC)
            if self._stop.is_set():
                break
            elapsed = time.time() - self._last_flush
            if elapsed >= self.FLUSH_INTERVAL_SEC:
                self.logger.debug(
                    "Periodic flush triggered (%.1fs elapsed, %d states, %d pending)",
                    elapsed, len(self._states), len(self._pending_1m),
                )
                self._flush()

    def run(self):
        self.logger.info("CandlestickConsumer starting...")
        self._connect_db()
        self._connect_kafka()

        # Hydrate in-memory state from DB so we don't overwrite existing bars
        self.logger.info("Hydrating candle state from DB...")
        self._states = _hydrate_from_db(self._conn)
        self.logger.info("Loaded state for %d symbols.", len(self._states))

        # Start periodic flush thread
        self._periodic_thread = threading.Thread(
            target=self._periodic_flush_loop,
            daemon=True,
            name="CandlestickPeriodicFlush",
        )
        self._periodic_thread.start()

        while not self._stop.is_set():
            try:
                raw_msgs = self._consumer.poll(timeout_ms=2000, max_records=200)
                if not raw_msgs:
                    continue

                for tp, messages in raw_msgs.items():
                    for msg in messages:
                        self._process_msg(msg)

                # Flush if batch buffer is large enough
                if len(self._pending_1m) + len(self._pending_1d) >= self.BATCH_SIZE:
                    self._flush()

            except Exception as e:
                self.logger.error("Poll error: %s", e)
                time.sleep(2)
                try:
                    self._reconnect_db()
                except Exception:
                    pass

        # ── Shutdown ─────────────────────────────────────────────────────────
        self.logger.info("Shutdown requested. Flushing all in-flight bars...")
        for symbol, state in self._states.items():
            for kind, bar in state.flush_all():
                if kind == "1m":
                    self._pending_1m.append((
                        bar["symbol"], bar["time_start"],
                        bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"],
                    ))
                else:
                    self._pending_1d.append((
                        bar["symbol"], bar["trading_date"],
                        bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"],
                        0, 0, 0,
                    ))
        self._flush()

        self._consumer.close()
        self._cursor.close()
        self._conn.close()
        self.logger.info("CandlestickConsumer stopped.")

    def stop(self):
        self._stop.set()


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    consumer = CandlestickConsumer()

    def shutdown(signum, frame):
        print("\nShutdown signal received. Stopping CandlestickConsumer...")
        consumer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


if __name__ == "__main__":
    main()