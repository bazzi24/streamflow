import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from base_consumer import connect_db, setup_logger
from tqdm import tqdm

logger = setup_logger('consumer-quote')

topic = 'market_data_quote'
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

conn = connect_db('data')
cursor = conn.cursor()

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id='dataQuote_to_mysql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
)

batch = []
batch_size = 50000

# 43 columns in INSERT, 43 %s in VALUES
SQL_INSERT = """
INSERT INTO data.data_quote (
    trading_date, time, exchange, symbol_id, rtype, trading_session,
    ask_price1, ask_vol1, ask_price2, ask_vol2, ask_price3, ask_vol3,
    ask_price4, ask_vol4, ask_price5, ask_vol5, ask_price6, ask_vol6,
    ask_price7, ask_vol7, ask_price8, ask_vol8, ask_price9, ask_vol9,
    ask_price10, ask_vol10,
    bid_price1, bid_vol1, bid_price2, bid_vol2, bid_price3, bid_vol3,
    bid_price4, bid_vol4, bid_price5, bid_vol5, bid_price6, bid_vol6,
    bid_price7, bid_vol7, bid_price8, bid_vol8, bid_price9, bid_vol9,
    bid_price10, bid_vol10
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
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
        group_id='dataQuote_to_mysql',
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
                            data.get("TradingDate"), data.get("Time"),
                            data.get("Exchange"), data.get("Symbol"),
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
                        batch.append(record)
                        progress_bar.update(1)
                    except Exception as e:
                        logger.error("Error processing message: %s", e)
                        continue

            if batch:
                cursor.executemany(SQL_INSERT, batch)
                conn.commit()
                consumer.commit()
                logger.info("Inserted %d records into data_quote", len(batch))
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
        logger.info("Final batch: %d records before close", len(batch))
    cursor.close()
    conn.close()
    consumer.close()
    logger.info("Consumer closed.")
