from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import os
import sys

# Add project root to path for imports (go up 2 levels from this file)
# This file is at: project_root/api_service/alembic_migrations/env.py
# We need project_root in sys.path so we can import api_service.src.models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from api_service.src.models import Base

config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

def get_url():
    # Build URL from environment variables
    user = os.getenv("DB_USER", "streamflow_app")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("MYSQL_HOST", "mysql")
    port = os.getenv("MYSQL_PORT", "3306")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/api?charset=utf8mb4"

def run_migrations_offline() -> None:
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
