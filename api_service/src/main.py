import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.v1.router import api_router
from .websocket.manager import ws_manager
from .config import get_settings

settings = get_settings()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("streamflow_api")


# ── Kafka bridge background task ──────────────────────────────────────────

bridge_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bridge_task
    from .websocket.bridge import kafka_bridge_loop

    # Ensure api database + tables exist (idempotent — safe to re-run)
    from .database import api_engine
    from sqlalchemy import text
    with api_engine.connect() as conn:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS api CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
        conn.execute(text("USE api"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS `user` (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                username VARCHAR(100) NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                INDEX idx_email (email)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                position INT DEFAULT 0,
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_user_symbol (user_id, symbol),
                FOREIGN KEY (user_id) REFERENCES `user`(id) ON DELETE CASCADE,
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        conn.commit()
    logger.info("API database + tables ensured.")

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

# CORS — origins from env (comma-separated)
origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
