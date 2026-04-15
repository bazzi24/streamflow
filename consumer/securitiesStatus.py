import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from base_consumer import connect_db, setup_logger
from tqdm import tqdm

logger = setup_logger('consumer-status')

topic = 'securities_status'
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

conn = connect_db('data')
cursor = conn.cursor()

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id='securitiesStatus_to_mysql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
)

batch = []
batch_size = 50000

SQL_INSERT = """
    INSERT INTO data.securities_status (
        rtype, market_id, trading_date, time, symbol_id, trading_session,
        trading_status, exchange, trading_ol_session)
    VALUES (%s, %s, %s,
            %s, %s, %s,
            %s, %s, %s)
"""

try:
    logger.info("Consumer started, listening on topic: %s", topic)
    while True:
        raw_msgs = consumer.poll(timeout_ms=1000, max_records=batch_size)
        if not raw_msgs:
            time.sleep(1)
            continue

        progress_bar = tqdm(total=sum(len(msgs) for msgs in raw_msgs.values()), unit="msg")
        for tp, messages in raw_msgs.items():
            for msg in messages:
                try:
                    content_str = msg.value["Content"]
                    data = json.loads(content_str)

                    trading_date = None
                    if data.get("TradingDate"):
                        try:
                            trading_date = datetime.strptime(data["TradingDate"], "%d/%m/%Y").date().isoformat()
                        except Exception:
                            trading_date = None
                    data["TradingDate"] = trading_date

                    record = (
                        data.get("RType"),
                        data.get("MarketId"),
                        data.get("TradingDate"),
                        data.get("Time"),
                        data.get("Symbol"),
                        data.get("TradingSession"),
                        data.get("TradingStatus"),
                        data.get("Exchange"),
                        data.get("TradingOlSession"),
                    )
                    batch.append(record)
                    progress_bar.update(1)
                except Exception as e:
                    logger.error("Error processing message: %s", e)
                    continue

        if len(batch) >= batch_size or raw_msgs:
            cursor.executemany(SQL_INSERT, batch)
            conn.commit()
            consumer.commit()
            logger.info("Inserted %d records into securities_status", len(batch))
            batch.clear()
        progress_bar.close()

except Exception as e:
    logger.error("Fatal error: %s", e)
    conn.rollback()
finally:
    if batch:
        cursor.executemany(SQL_INSERT, batch)
        conn.commit()
        consumer.commit()
        batch.clear()
        logger.info("Final batch inserted before closing.")
    cursor.close()
    conn.close()
    consumer.close()
    logger.info("Consumer closed.")
