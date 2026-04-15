import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from base_consumer import connect_db, setup_logger
from tqdm import tqdm

logger = setup_logger('consumer-trade')

topic = 'market_data_trade'
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

conn = connect_db('data')
cursor = conn.cursor()

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id='dataTrade_to_mysql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
)

batch = []
batch_size = 50000

SQL_INSERT = """
    INSERT INTO data.data_trade (
        rtype, trading_date, time, isin,
        symbol, ceiling, `floor`, ref_price,
        avg_price, prior_val, last_price,
        last_vol, total_val, total_vol,
        market_id, exchange, trading_session,
        trading_status, `change`, ratio_change,
        est_matched_price, highest, lowest, side
    )
    VALUES (%s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s)
"""


def reconnect():
    """Safely close and reopen the DB connection and Kafka consumer."""
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
        group_id='dataTrade_to_mysql',
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

                        # Only insert buy-aggressor trades (Side='BU' = Mua).
                        # Side='SD' (Ban/seller-initiated) is the same trade seen from
                        # the seller's perspective — including both doubles volume and
                        # price fields in data_trade and all downstream consumers.
                        # SSI uses BU (mua) and SD (bán) — NOT M/B.
                        side = str(data.get("Side") or "").strip().upper()
                        if side and side != "BU":
                            progress_bar.update(1)
                            continue

                        trading_date = None
                        if data.get("TradingDate"):
                            try:
                                trading_date = datetime.strptime(
                                    data["TradingDate"], "%d/%m/%Y"
                                ).date().isoformat()
                            except Exception:
                                trading_date = None
                        data["TradingDate"] = trading_date

                        record = (
                            data.get("RType"),
                            data.get("TradingDate"),
                            data.get("Time"),
                            data.get("Isin"),
                            data.get("Symbol"),
                            data.get("Ceiling"),
                            data.get("Floor"),
                            data.get("RefPrice"),
                            data.get("AvgPrice"),
                            data.get("PriorVal"),
                            data.get("LastPrice"),
                            data.get("LastVol"),
                            data.get("TotalVal"),
                            data.get("TotalVol"),
                            data.get("MarketId"),
                            data.get("Exchange"),
                            data.get("TradingSession"),
                            data.get("TradingStatus"),
                            data.get("Change"),
                            data.get("RatioChange"),
                            data.get("EstMatchedPrice"),
                            data.get("Highest"),
                            data.get("Lowest"),
                            data.get("Side"),
                        )
                        batch.append(record)
                        progress_bar.update(1)
                    except Exception as e:
                        logger.error("Error processing message: %s", e)
                        continue

            if len(batch) >= batch_size:
                cursor.executemany(SQL_INSERT, batch)
                conn.commit()
                consumer.commit()
                logger.info("Inserted %d records into data_trade", len(batch))
                batch.clear()
            progress_bar.close()

        except (OSError, IOError) as e:
            # DB or network error — reconnect and resume polling
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
        logger.info("Final batch: %d records before close", len(batch))
    cursor.close()
    conn.close()
    consumer.close()
    logger.info("Consumer closed.")
