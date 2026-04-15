import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from base_consumer import connect_db, setup_logger
from tqdm import tqdm

logger = setup_logger('consumer-index')

topic = 'index_data'
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

conn = connect_db('data')
cursor = conn.cursor()

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id='index_to_mysql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
)

batch = []
batch_size = 50000

SQL_INSERT = """
    INSERT INTO data.index_data (
        index_id, index_value, prior_index_value,
        trading_date, time, total_trade, total_qtty,
        total_value, index_name, advances, nochanges,
        declines, ceilings, floors, `change`, ratio_change,
        total_qtty_pt, total_value_pt, exchange, all_qtty,
        all_value, index_type, trading_session, market_id,
        rtype, total_qtty_od, total_value_od
    )
    VALUES (%s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
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
                        data.get("IndexId"),
                        data.get("IndexValue"),
                        data.get("PriorIndexValue"),
                        data.get("TradingDate"),
                        data.get("Time"),
                        data.get("TotalTrade"),
                        data.get("TotalQtty"),
                        data.get("TotalValue"),
                        data.get("IndexName"),
                        data.get("Advances"),
                        data.get("NoChanges"),
                        data.get("Declines"),
                        data.get("Ceilings"),
                        data.get("Floors"),
                        data.get("Change"),
                        data.get("RatioChange"),
                        data.get("TotalQttyPt"),
                        data.get("TotalValuePt"),
                        data.get("Exchange"),
                        data.get("AllQty"),
                        data.get("AllValue"),
                        data.get("IndexType"),
                        data.get("TradingSession"),
                        data.get("MarketId"),
                        data.get("RType"),
                        data.get("TotalQttyOd"),
                        data.get("TotalValueOd"),
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
            logger.info("Inserted %d records into index_data", len(batch))
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
