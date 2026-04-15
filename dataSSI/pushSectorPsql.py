import psycopg2
import csv
import os 
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def connect_db():
    """Kết nối đến PostgreSQL"""
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),  
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST")
    )
    
conn = connect_db()
cursor = conn.cursor()

csv_file_path = 'doanh_nghiep_a_z.csv'

with open(csv_file_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        sector_name = row['Brand'].strip()
        symbol_id = row['Symbol'].strip()
        market_name = row['Market'].strip()
        #created_at = datetime.now()  # or leave it as NULL if you want it to be automatic.

        cursor.execute("""
            INSERT INTO corporation.sector (sector_name, symbol_id, market_name)
            VALUES (%s, %s, %s)
        """, (sector_name, symbol_id, market_name))

conn.commit()
cursor.close()
conn.close()