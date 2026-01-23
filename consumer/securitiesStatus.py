import json
import time
import psycopg2
from kafka import KafkaConsumer, TopicPartition
from dataSSI import config
from datetime import datetime
from tqdm import tqdm

def connect_db():  
    return psycopg2.connect(
        dbname=config.DB_NAME,  
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST
    )
    
conn = connect_db()
cursor = conn.cursor()

topic = 'securities_status'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    group_id='securitiesStatus_to_psql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest'  
)   

batch = []
batch_size = 50000

SQL_INSERT = """
    INSERT INTO streaming.securities_status (
        rtype, market_id, trading_date, time, symbol_id, trading_session,
        trading_status, exchange, trading_ol_session)
    VALUES (%s, %s, %s,
            %s, %s, %s,
            %s, %s, %s)
"""

try:
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
                        except:
                            trading_date = None
                    data["TradingDate"] = trading_date
                    
                    record = (
                        data.get("RType"),
                        data.get("MarketId"),
                        data.get("TradingDat"),
                        data.get("Time"),
                        data.get("Symbol"),
                        data.get("TradingSession"),
                        data.get("TradingStatus"),
                        data.get("Exchange"),
                        data.get("TradingOlSession")
                    )
                    batch.append(record)
                    progress_bar.update(1)
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
        
        # Nếu batch đủ lớn, insert và commit
        if len(batch) >= batch_size:
            cursor.executemany(SQL_INSERT, batch)
            conn.commit()
            consumer.commit()
            batch.clear()
        elif raw_msgs:  # nếu còn message nhỏ hơn batch_size
            cursor.executemany(SQL_INSERT, batch)
            conn.commit()
            consumer.commit()
            batch.clear()
        progress_bar.close()
    print("Finished consuming messages.")
            
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    if batch:
        cursor.executemany(SQL_INSERT, batch)
        conn.commit()
        consumer.commit()
        batch.clear()
        print("Final batch inserted before closing.")
    cursor.close()
    conn.close()
    consumer.close()
    print("Consumer closed.")
