import asyncio
import logging
import logging.handlers
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from .api.v1.router import api_router
from .websocket.manager import ws_manager
from .config import get_settings
import os

settings = get_settings()

# ── Logging Configuration ─────────────────────────────────────────────────────
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
log_dir = os.getenv("LOG_DIR", "logs")
os.makedirs(log_dir, exist_ok=True)

# Root logger configuration
root_logger = logging.getLogger()
root_logger.setLevel(log_level)

# Clear any existing handlers
root_logger.handlers.clear()

# Formatter
formatter = logging.Formatter(
    "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Console handler (always enabled)
console_handler = logging.StreamHandler()
console_handler.setLevel(log_level)
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

# File handler with rotation (production)
if settings.debug or os.getenv("ENVIRONMENT", "").lower() in ["prod", "production"]:
    file_handler = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "api_service.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

logger = logging.getLogger("streamflow_api")

# Rate limiter
limiter = Limiter(key_func=get_remote_address)
if settings.rate_limit_enabled:
    # Apply default limit to all routes unless overridden
    limiter.default_limits = [f"{settings.rate_limit_requests} per {settings.rate_limit_window_seconds} seconds"]


# ── Kafka bridge background task ──────────────────────────────────────────

bridge_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bridge_task
    from .websocket.bridge import kafka_bridge_loop

    # Run database migrations using Alembic
    try:
        from alembic.config import Config
        from alembic import command
        import os
        from sqlalchemy import create_engine, text

        # First, ensure the 'api' database exists (Alembic only creates tables)
        server_url = settings.api_db_url.rsplit("/", 1)[0]  # remove database part (api)
        engine = create_engine(server_url)
        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS api CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
            conn.commit()
        engine.dispose()

        # Now run Alembic migrations
        project_root = os.path.dirname(os.path.dirname(__file__))
        alembic_ini = os.path.join(project_root, "alembic.ini")
        alembic_cfg = Config(alembic_ini)
        # Override sqlalchemy.url from environment
        alembic_cfg.set_main_option("sqlalchemy.url", settings.api_db_url)

        logger.info("Running database migrations...")
        command.upgrade(alembic_cfg, "head")
        logger.info("Migrations completed successfully.")
    except Exception as e:
        logger.error("Migration failed: %s", e)
        raise

    logger.info("Starting Kafka bridge...")
    bridge_task = asyncio.create_task(kafka_bridge_loop())
    logger.info("API service started. WebSocket manager ready.")
    yield
    # Shutdown
    logger.info("Shutting down...")
    if bridge_task:
        bridge_task.cancel()
        try:
            await bridge_task
        except asyncio.CancelledError:
            pass
    logger.info("API service stopped.")


# ── FastAPI app ────────────────────────────────────────────────────────────

app = FastAPI(
    title=settings.app_name,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Add rate limit exception handler if enabled
if settings.rate_limit_enabled:
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS — origins from env (comma-separated)
origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting middleware (if enabled)
if settings.rate_limit_enabled:
    app.add_middleware(SlowAPIMiddleware)

# REST routes
app.include_router(api_router)

# WebSocket routes
from .api.v1 import websocket as ws_module
app.include_router(ws_module.router)


@app.get("/health")
def health():
    return {
        "status": "ok",
        "ws_connections": ws_manager.connection_count,
    }
