"""
producer_unified.py
==================
Runs all 5 Kafka producers in parallel threads inside a single container.
Each thread connects to its own SSI WebSocket channel and publishes to its
corresponding Kafka topic.
"""

import os
import sys
import json
import time
import signal
import logging
import threading
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from dataSSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# ── Channels → Kafka topics ──────────────────────────────────────────────────
CHANNEL_TOPIC_MAP = {
    "X-TRADE:ALL": "market_data_trade",
    "X-QUOTE:ALL": "market_data_quote",
    "MI:ALL":      "index_data",
    "R:ALL":       "foreign_room_data",
    "F:ALL":       "securities_status",
}

CHANNELS = list(CHANNEL_TOPIC_MAP.keys())

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


class ProducerThread(threading.Thread):
    """Runs a single SSI → Kafka channel in its own thread."""

    def __init__(self, channel: str, topic: str, log_dir: str):
        super().__init__(daemon=True, name=f"Producer-{channel}")
        self.channel = channel
        self.topic = topic
        self.log_dir = log_dir
        self._stop_event = threading.Event()
        self._connected = threading.Event()  # signaled when WS is open
        self._producer: KafkaProducer | None = None
        self._stream: MarketDataStream | None = None

    def _make_producer(self) -> KafkaProducer:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def _on_error(self, error):
        self.logger.error("%s", error)

    def _on_open(self):
        self.logger.info("WebSocket connected for %s", self.channel)
        self._connected.set()

    def _on_close(self):
        self.logger.warning("WebSocket closed for %s", self.channel)
        self._connected.clear()

    def _send(self, message):
        try:
            key = None
            if self.channel == "X-TRADE:ALL":
                key = message.get("Symbol", "").encode("utf-8") if isinstance(message, dict) else None
            elif self.channel == "X-QUOTE:ALL":
                key = message.get("Symbol", "").encode("utf-8") if isinstance(message, dict) else None
            elif self.channel == "R:ALL":
                key = message.get("Symbol", "").encode("utf-8") if isinstance(message, dict) else None
            elif self.channel == "MI:ALL":
                key = message.get("IndexId", "").encode("utf-8") if isinstance(message, dict) else None
            self._producer.send(self.topic, value=message, key=key)
            self._producer.flush()
            self.logger.debug("Sent to %s: %s", self.topic, str(message)[:80])
        except Exception as e:
            self.logger.error("Send error on %s: %s", self.topic, e)

    def run(self):
        self.logger = setup_logger(f"producer-{self.channel.replace(':', '_')}")
        self.logger.info("Starting producer thread for channel: %s → topic: %s",
                         self.channel, self.topic)

        self._producer = self._make_producer()
        self._stream = MarketDataStream(
            config,
            MarketDataClient(config),
            on_close=self._on_close,
            on_open=self._on_open,
        )

        self._stream.start(self._send, self._on_error, self.channel)

        # Wait for WebSocket to connect and stay connected.
        # The original producer_market_data.py blocks forever on input() or sleep().
        # Since MarketDataStream.start() is fire-and-forget, we wait here so the
        # thread doesn't exit and close the producer prematurely.
        try:
            while not self._stop_event.is_set():
                if self._connected.wait(timeout=30):
                    # Connected — keep thread alive until stop signal
                    self.logger.info("Producer thread for %s connected. Running...", self.channel)
                    while not self._stop_event.is_set():
                        time.sleep(5)
                else:
                    self.logger.warning("WebSocket for %s did not connect in 30s, retrying...", self.channel)
        except Exception as e:
            self.logger.error("Producer thread error for %s: %s", self.channel, e)
        finally:
            self.logger.info("Producer thread for %s stopping.", self.channel)
            self._stream.stop()
            if self._producer:
                self._producer.flush()
                self._producer.close()

    def stop(self):
        self._stop_event.set()
        if self._stream:
            self._stream.stop()


def main():
    log_dir = os.getenv("LOG_DIR", "/streamflow/logs")
    threads: list[ProducerThread] = []

    def shutdown(signum, frame):
        print("\nShutdown signal received. Stopping all producer threads...")
        for t in threads:
            t.stop()
        for t in threads:
            t.join(timeout=5)
        print("All producer threads stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for channel, topic in CHANNEL_TOPIC_MAP.items():
        t = ProducerThread(channel, topic, log_dir)
        t.start()
        threads.append(t)
        time.sleep(0.5)  # stagger startup slightly

    print(f"All {len(threads)} producer threads started successfully!")
    print(f"Channels: {', '.join(CHANNELS)}")
    print("Press Ctrl+C to stop all producers.\n")

    # Keep the main thread alive — wait for all threads
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()
