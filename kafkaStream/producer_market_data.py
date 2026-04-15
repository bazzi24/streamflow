import os
import json
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from dataSSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient


_CHANNEL_TO_LOG_NAME = {
    'F:ALL':       'producer-status',
    'X-TRADE:ALL': 'producer-trade',
    'X-QUOTE:ALL': 'producer-quote',
    'R:ALL':       'producer-foreign',
    'MI:ALL':      'producer-index',
    'B:ALL':       'producer-bars',
}


def _setup_logger(name: str) -> logging.Logger:
    log_dir = os.getenv("LOG_DIR", "/streamflow/logs")
    os.makedirs(log_dir, exist_ok=True)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


class MarketKafkaProducer:
    def __init__(self, config):
        self.config = config
        self.selected_channel = None
        self.logger = logging.getLogger("producer")  # replaced after channel is known
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )

    def get_error(self, error):
        self.logger.error("%s", error)

    def send_to_kafka(self, message):
        dt = self.selected_channel

        if dt == 'F:ALL':
            topic = 'securities_status'
            key = None
        elif dt == 'X-TRADE:ALL':
            topic = 'market_data_trade'
            key = message.get('Symbol', '').encode('utf-8') if isinstance(message, dict) else None
        elif dt == 'X-QUOTE:ALL':
            topic = 'market_data_quote'
            key = message.get('Symbol', '').encode('utf-8') if isinstance(message, dict) else None
        elif dt == 'R:ALL':
            topic = 'foreign_room_data'
            key = message.get('Symbol', '').encode('utf-8') if isinstance(message, dict) else None
        elif dt == 'MI:ALL':
            topic = 'index_data'
            key = message.get('IndexId', '').encode('utf-8') if isinstance(message, dict) else None
        elif dt == 'B:ALL':
            topic = 'bars'
            key = None
        else:
            self.logger.warning("Unknown channel: %s", dt)
            return

        self.producer.send(topic, value=message, key=key)
        self.logger.debug("Queued for %s: %s", topic, str(message)[:80])

    def get_market_data(self, message):
        self.send_to_kafka(message)

    def run(self):
        if len(sys.argv) > 1:
            self.selected_channel = sys.argv[1]
        else:
            self.selected_channel = input("Please select channel: ")

        log_name = _CHANNEL_TO_LOG_NAME.get(self.selected_channel, "producer-unknown")
        self.logger = _setup_logger(log_name)
        self.logger.info("Channel selected: %s", self.selected_channel)

        mm = MarketDataStream(
            self.config,
            MarketDataClient(self.config),
            on_close=lambda: self.logger.warning("WebSocket closed — reconnecting..."),
            on_open=lambda: self.logger.info("Connected to WebSocket."),
        )

        mm.start(self.get_market_data, self.get_error, self.selected_channel)

        message = None
        while message != "exit()":
            try:
                message = input(">> ")
                if message and message != "exit()":
                    mm.switch_channel(message)
            except KeyboardInterrupt:
                self.logger.info("KeyboardInterrupt received. Exiting gracefully...")
                break
            except EOFError:
                # No stdin (e.g. running inside Docker) — keep the stream alive
                while True:
                    time.sleep(60)

        self.producer.flush()
        self.producer.close()
        self.logger.info("Kafka producer closed.")


if __name__ == "__main__":
    app = MarketKafkaProducer(config)
    app.run()
