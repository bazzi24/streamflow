import json
import psycopg2
import time
from datetime import datetime
from kafka import TopicPartition, KafkaConsumer
from tqdm import tqdm
from dataSSI import config

def connect_db():
    return psycopg2.connect(
        dbname=config.DB_NAME,  
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST
    )
    
conn = connect_db()
cursor = conn.cursor()

topic = 'foreign_room_data'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    group_id='dataForeignRoom_to_psql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest'
)


batch = []
batch_size = 50000

SQL_INSERT = """
                INSERT INTO streaming.foreign_room (
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
                            trading_date = datetime.strptime(data.get("TradingDate"), '%d/%m/%Y').date().isoformat()
                        except:
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
                        data.get('Exchange')
                    )
                    batch.append(record)
                    progress_bar.update(1)
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
                
        if len(batch) >= batch_size:
            cursor.executemany(SQL_INSERT, batch)
            conn.commit()
            consumer.commit()
            batch.clear()
        elif raw_msgs:  
            cursor.executemany(SQL_INSERT, batch)
            conn.commit()
            consumer.commit()
            batch.clear()
        progress_bar.close()
        
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    if batch:
        cursor.executemany(SQL_INSERT, batch)
        conn.commit()
        consumer.commit()
        batch.clear()
    cursor.close()
    conn.close()
    consumer.close()
