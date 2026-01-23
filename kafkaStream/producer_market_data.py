from kafka import KafkaProducer
import json
import time
from dataSSI import config
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
import sys


class MarketKafkaProducer:
    def __init__(self, config):
        self.config = config
        self.selected_channel = None
        self.producer = KafkaProducer(
            bootstrap_servers='100.125.159.41:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def get_error(self, error):
        print(f"[ERROR] {error}")

    def send_to_kafka(self, message):
        """Send data to Kafka, based on selected_channel."""
        dt = self.selected_channel  # Get the value of the selected channel

        if dt == 'F:ALL':
            topic = 'securities_status'
        elif dt == 'X-TRADE:ALL':
            topic = 'market_data_trade'
        elif dt == 'X-QUOTE:ALL':
            topic = 'market_data_quote'
        elif dt == 'R:ALL':
            topic = 'foreign_room_data'
        elif dt == 'MI:ALL':
            topic = 'index_data'
        elif dt == "B:ALL":    
            dt = "bars"
        else:
            print(f"Unknown channel: {dt}")
            return

        self.producer.send(topic, value=message)
        print(f"Sent to {topic}: {message}")

    def get_market_data(self, message):
        """Callback receives stream data and pushes it to Kafka."""
        self.send_to_kafka(message)

    def run(self):
        """RUN"""
        if len(sys.argv) > 1:
            self.selected_channel = sys.argv[1]
            print(f"[AUTO MODE] Channel selected from CLI: {self.selected_channel}")
        else:
            self.selected_channel = input("Please select channel: ")

        mm = MarketDataStream(
            self.config,
            MarketDataClient(self.config),
            on_close=lambda: print("WebSocket closed — reconnecting..."),
            on_open=lambda: print("Connected to WebSocket."),
        )

        mm.start(self.get_market_data, self.get_error, self.selected_channel)

        message = None
        while message != "exit()":
            try:
                message = input(">> ")
                if message and message != "exit()":
                    mm.switch_channel(message)
            except KeyboardInterrupt:
                print("\nKeyboardInterrupt received. Exiting gracefully...")
                break

        self.producer.flush()
        self.producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    app = MarketKafkaProducer(config)
    app.run()
