from pyflink.common import Configuration, Time, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows
import json

def parse_trade(value):
    try:
        data = json.loads(value)
        # message example: {"symbol":"BTCUSDT","price":68000.5,"volume":0.02,"timestamp":1710000000000}
        data["timestamp"] = int(data.get("timestamp", 0))
        data["price"] = float(data.get("price", 0))
        data["volume"] = float(data.get("volume", 0))
        return data
    except Exception as e:
        print("Parse error:", e)
        return None

def main():
    # ----------------- Configuration -----------------
    conf = Configuration()
    jars = [
        "file:///opt/flink/usrlib/flink-connector-kafka-3.4.0-1.20.jar",
        "file:///opt/flink/usrlib/kafka-clients-3.7.0.jar"
    ]
    conf.set_string("pipeline.jars", ",".join(jars))

    env = StreamExecutionEnvironment.get_execution_environment(configuration=conf)
    env.get_config().set_auto_watermark_interval(1000)

    # ----------------- Kafka Source -----------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("market_data_trade")
        .set_group_id("chart-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # ----------------- Kafka Sink -----------------
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("candlestick_1m")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # ----------------- Watermark Strategy -----------------
    watermark_strategy = (
        WatermarkStrategy
        .for_monotonous_timestamps()
        .with_timestamp_assigner(lambda event, ts: event["timestamp"] if event else 0)
    )

    # ----------------- Read Stream -----------------
    stream = env.from_source(source, watermark_strategy, "trade-source")

    # ----------------- Parse Trade -----------------
    parsed_stream = stream.map(parse_trade).filter(lambda x: x is not None)

    # ----------------- 1-Minute Candlestick -----------------
    def to_candlestick(trade):
        return {
            "symbol": trade["symbol"],
            "open": trade["price"],
            "high": trade["price"],
            "low": trade["price"],
            "volume": trade["volume"],
            "timestamp": trade["timestamp"]
        }

    def candlestick_reduce(a, b):
        return {
            "symbol": a["symbol"],
            "open": a["open"],
            "close": b["price"],  # close = last price
            "high": max(a["high"], b["price"]),
            "low": min(a["low"], b["price"]),
            "volume": a["volume"] + b["volume"],
            "timestamp": a["timestamp"]  
        }

    candlestick_stream = (
        parsed_stream.map(to_candlestick)
                     .key_by(lambda x: x["symbol"])
                     .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                     .reduce(candlestick_reduce)
    )

    # ----------------- Sink -----------------
    candlestick_stream.map(lambda x: json.dumps(x)).sink_to(sink)
    candlestick_stream.print()

    print("Submitting Flink job now...")
    env.execute("flink_candlestick_1m_stream")

if __name__ == "__main__":
    print("Starting flink_candlestick_1m.py")
    main()
