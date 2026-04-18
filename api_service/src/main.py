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
