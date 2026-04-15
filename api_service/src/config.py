from pydantic_settings import BaseSettings
from pydantic import field_validator
from functools import lru_cache
import re


class Settings(BaseSettings):
    # ── Database credentials — REQUIRED, no defaults ──────────────────────────────
    # Must be set in .env.  Never use root in production.
    db_user: str
    db_password: str

    # ── JDBC URLs from env — credentials embedded here are replaced below
    # These use root in dev, and app_user in prod.
    raw_db_url: str  # RAW_DB_URL from .env
    dw_db_url: str   # DW_DB_URL from .env

    @property
    def streaming_db_url(self) -> str:
        """
        SQLAlchemy URL for data.streaming DB.
        .env has RAW_DB_URL=jdbc:mysql://mysql:3306/data?...  (no embedded credentials).
        We rebuild it with db_user/db_password from .env and strip the 'jdbc:' prefix.
        """
        return _build_sqlalchemy_url(self.raw_db_url, self.db_user, self.db_password)

    @property
    def db_url(self) -> str:
        """
        SQLAlchemy URL for warehouse DB.
        .env has DW_DB_URL=jdbc:mysql://mysql:3306/warehouse?...  (no embedded credentials).
        """
        return _build_sqlalchemy_url(self.dw_db_url, self.db_user, self.db_password)

    @property
    def api_db_url(self) -> str:
        """API DB — connects without a database name (db is created at runtime)."""
        return (
            f"mysql+pymysql://{self.db_user}:{self.db_password}"
            f"@mysql:3306?charset=utf8mb4"
        )

    # ── Kafka ──────────────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "kafka:9092"

    # ── JWT — REQUIRED, no default ─────────────────────────────────────────────
    secret_key: str

    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24  # 24 hours

    # ── App ─────────────────────────────────────────────────────────────────────
    app_name: str = "StreamFlow API"
    debug: bool = False

    # ── CORS — comma-separated origins ──────────────────────────────────────────
    # Production example: http://localhost:3000,https://app.example.com
    cors_origins: str = "http://localhost:3000"

    @field_validator("secret_key")
    @classmethod
    def reject_default_secret(cls, v: str) -> str:
        if "change" in v.lower() and "secret" in v.lower():
            raise ValueError(
                "SECURITY ERROR: SECRET_KEY must be set to a secure random value "
                "in .env — not the placeholder.  "
                'Generate one with: python -c "import secrets; print(secrets.token_hex(32))"'
            )
        return v

    @field_validator("cors_origins")
    @classmethod
    def parse_cors_origins(cls, v: str) -> str:
        return v.strip()

    class Config:
        env_file = "../.env"
        env_file_encoding = "utf-8"
        extra = "allow"


def _strip_jdbc_prefix(url: str) -> str:
    """Strip 'jdbc:mysql://' so SQLAlchemy can parse the URL."""
    return re.sub(r"^jdbc:", "", url)


def _build_sqlalchemy_url(jdbc_url: str, user: str, password: str) -> str:
    """
    Convert a JDBC URL to a SQLAlchemy URL and inject credentials.

    Input:  jdbc:mysql://[user:pass@]host[:port]/db[?query]
    Output: mysql+pymysql://user:pass@host[:port]/db[?query]

    The JDBC URLs in .env have no embedded credentials (docker service name is
    the host).  This function replaces any existing credentials AND adds new ones.
    """
    def replacer(m: re.Match) -> str:
        return f"mysql+pymysql://{user}:{password}@{m.group(1)}"

    # Matches: jdbc:mysql:// [creds@] host[:port] /path[?query]
    #   group(1): [creds@]host[:port]
    result = re.sub(
        r"jdbc:mysql://([^/]+)(/.*)",
        replacer,
        jdbc_url,
        count=1,
    )
    return result


@lru_cache
def get_settings() -> Settings:
    return Settings()
