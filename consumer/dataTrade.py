import json
import psycopg2
from kafka import KafkaConsumer, TopicPartition
from base_consumer import connect_kafka
from dataSSI import config
from datetime import datetime
from tqdm import tqdm
import time

def connect_db():
    return psycopg2.connect(
        dbname=config.DB_NAME,  
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST
    )
    
conn = connect_db()
cursor = conn.cursor()

topic = 'market_data_trade'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    group_id='dataTrade_to_psql',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    enable_auto_commit=False,
    auto_offset_reset='earliest'  
)

batch = []
batch_size = 50000

SQL_INSERT = """
    INSERT INTO streaming.data_trade (
        rtype, trading_date, time, isin,
        symbol, ceiling, floor, ref_price, 
        avg_price, prior_val, last_price, 
        last_vol, total_val, total_vol,
        market_id, exchange, trading_session,
        trading_status, change, ratio_change,
        est_matched_price, highest, lowest, side
    )
    VALUES (%s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s)
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
                        data.get("Side")
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
        print("Final batch inserted before closing.")
    cursor.close()
    conn.close()
    consumer.close()
    print("Consumer closed.")