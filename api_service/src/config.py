from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database — 2-database layout per PLAN_UPDATE_DB.md
    # warehouse: star-schema DW (dim + fact)
    db_url: str = "mysql+pymysql://root:stream_flow@mysql:3306/warehouse?charset=utf8mb4"
    # data: raw streaming + reference + candlestick
    streaming_db_url: str = "mysql+pymysql://root:stream_flow@mysql:3306/data?charset=utf8mb4"
    # api: user + watchlist (created at runtime by main.py lifespan)
    # NOTE: api_db_url intentionally omits the database name — the lifespan
    # creates the `api` database first, then the engine connects to it.
    api_db_url: str = "mysql+pymysql://root:stream_flow@mysql:3306?charset=utf8mb4"
    db_user: str = "root"
    db_password: str = "stream_flow"

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"

    # JWT
    secret_key: str = "streamflow-super-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24  # 24 hours

    # App
    app_name: str = "StreamFlow API"
    debug: bool = False

    class Config:
        env_file = "../.env"
        env_file_encoding = "utf-8"
        extra = "allow"


@lru_cache
def get_settings() -> Settings:
    return Settings()
