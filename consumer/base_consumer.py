import os
import json
import logging
import pymysql
from kafka import KafkaConsumer
from logging.handlers import RotatingFileHandler


def setup_logger(name: str) -> logging.Logger:
    log_dir = os.getenv("LOG_DIR", "/streamflow/logs")
    os.makedirs(log_dir, exist_ok=True)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def connect_kafka(topic, group_id):
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True,
        auto_offset_reset='earliest',  # 'latest' causes missed messages if consumer starts before producer; 'earliest' ensures no data loss
    )
    return consumer


def connect_db(database='data'):
    password = os.getenv('DB_PASSWORD')
    if password is None:
        raise ValueError("DB_PASSWORD environment variable is required")
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST', 'mysql'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('DB_USER', 'root'),
        password=password,
        database=database,
        charset='utf8mb4',
        autocommit=False,
    )
