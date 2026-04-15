import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from base_consumer import connect_db, setup_logger
from tqdm import tqdm

logger = setup_logger('consumer-foreign')

topic = 'foreign_room_data'
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

conn = connect_db('data')
cursor = conn.cursor()

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id='dataForeignRoom_to_mysql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
)

batch = []
batch_size = 50000

SQL_INSERT = """
    INSERT INTO data.foreign_room (
        rtype, trading_date, time, isin,
        symbol, total_room, current_room,
        buy_vol, sell_vol, buy_val, sell_val,
        market_id, exchange
    )
    VALUES (
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s
    )
"""


def reconnect():
    global conn, cursor, consumer
    try:
        cursor.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    try:
        consumer.close()
    except Exception:
        pass
    conn = connect_db('data')
    cursor = conn.cursor()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id='dataForeignRoom_to_mysql',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=False,
        auto_offset_reset='earliest',
    )
    logger.info("Reconnected DB and Kafka.")


try:
    logger.info("Consumer started, listening on topic: %s", topic)
    while True:
        try:
            raw_msgs = consumer.poll(timeout_ms=1000, max_records=batch_size)
            if not raw_msgs:
                time.sleep(1)
                continue

            progress_bar = tqdm(
                total=sum(len(msgs) for msgs in raw_msgs.values()), unit="msg"
            )
            for tp, messages in raw_msgs.items():
                for msg in messages:
                    try:
                        content_str = msg.value["Content"]
                        data = json.loads(content_str)
                        trading_date = None
                        if data.get("TradingDate"):
                            try:
                                trading_date = (
                                    datetime.strptime(
                                        data.get("TradingDate"), '%d/%m/%Y'
                                    ).date().isoformat()
                                )
                            except Exception:
                                trading_date = None
                        data["TradingDate"] = trading_date

                        record = (
                            data.get('RType'),
                            data.get('TradingDate'),
                            data.get('Time'),
                            data.get('Isin'),
                            data.get('Symbol'),
                            data.get('TotalRoom'),
                            data.get('CurrentRoom'),
                            data.get('BuyVol'),
                            data.get('SellVol'),
                            data.get('BuyVal'),
                            data.get('SellVal'),
                            data.get('MarketId'),
                            data.get('Exchange'),
                        )
                        batch.append(record)
                        progress_bar.update(1)
                    except Exception as e:
                        logger.error("Error processing message: %s", e)
                        continue

            if batch:
                cursor.executemany(SQL_INSERT, batch)
                conn.commit()
                consumer.commit()
                logger.info("Inserted %d records into foreign_room", len(batch))
                batch.clear()
            progress_bar.close()

        except (OSError, IOError) as e:
            logger.error("Connection error: %s. Reconnecting...", e)
            reconnect()
            batch.clear()
            time.sleep(2)

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
