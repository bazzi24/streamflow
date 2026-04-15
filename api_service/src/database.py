import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from .config import get_settings

settings = get_settings()

# Must install pymysql as MySQLdb for SQLAlchemy
pymysql.install_as_MySQLdb()

# DW engine (dim + fact databases)
engine = create_engine(
    settings.db_url,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=settings.debug,
)

# Streaming engine (raw live data — populated by Kafka consumers)
# init_command sets the session timezone to Asia/Ho_Chi_Minh so that naive
# datetime columns (candlestick_1m.time_start, candlestick_1d.trading_date, etc.)
# are interpreted as Vietnam local time, not UTC.
streaming_engine = create_engine(
    settings.streaming_db_url,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=settings.debug,
    connect_args={"init_command": "SET time_zone = '+07:00'"},
)

# API engine (user/watchlist tables)
api_engine = create_engine(
    settings.api_db_url,
    pool_size=5,
    max_overflow=10,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=settings.debug,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
StreamingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=streaming_engine)
APISessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=api_engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_streaming_db():
    db = StreamingSessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_api_db():
    db = APISessionLocal()
    try:
        yield db
    finally:
        db.close()
