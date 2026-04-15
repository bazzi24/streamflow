import pymysql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from ..config import get_settings

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
APISessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=api_engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
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
