<div align="center">

![StreamFlow Banner](https://raw.githubusercontent.com/bazzi24/streamflow/main/frontend/public/banner_streamflow.png)

**StreamFlow — Real-Time Vietnamese Stock Market Dashboard**

![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?style=flat-square&logo=react&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Spark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?style=flat-square&logo=mysql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)
</div>

---

## Features

| | |
|---|---|
| **Bloomberg-style iBoard** | Dense dark terminal board — 27 columns, 9 segment tabs, real-time flash updates |
| **TradingView Charts** | Candlestick + volume histogram via `lightweight-charts v4` with MA overlays |
| **Market Breadth** | Live advances / declines for VNINDEX, VN30, HNXIndex, HNX30 |
| **Low-latency Pipeline** | SSI WebSocket → Kafka KRaft → MySQL streaming tables → FastAPI → WebSocket |
| **OHLCV Aggregation** | 1m/1d candlesticks computed in real-time by CandlestickConsumer |
| **Star-schema DW** | Spark ETL → `warehouse.dim.*` + `warehouse.fact.*` for historical queries |
| **JWT Authentication** | Secure watchlists, per-user watchlists persisted in `api` DB |
| **WebSocket Live Feed** | Per-symbol and market-wide push updates with 3s auto-reconnect |
| **VI / EN Language Switcher** | Full i18n via `react-i18next` — language persisted to localStorage |

---

## Quick Start

### 1. Clone & configure

```bash
git clone https://github.com/bazzi24/streamflow.git
cd streamflow
cp .env.example .env
```

Fill in `.env` with your [SSI](https://www.ssi.com.vn/) credentials and secrets:

```env
consumerID=<your_ssi_consumer_id>
consumerSecret=<your_ssi_consumer_secret>
url=https://fc-data.ssi.com.vn/
stream_url=https://fc-datahub.ssi.com.vn/

# REQUIRED — generate with: python -c "import secrets; print(secrets.token_hex(32))"
SECRET_KEY=<your-secret-key>

# Required credentials — use root for dev, create a limited app user in production
DB_USER=root
DB_PASSWORD=<your_password>

# CORS — comma-separated, no spaces
CORS_ORIGINS=http://localhost:3000
```

### 2. Install Python dependencies

```bash
uv venv
source .venv/bin/activate
uv sync
```

### 3. Download MySQL JDBC driver

```bash
mkdir -p lib
curl -L -o lib/mysql-connector-j-8.0.33.jar \
  "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
```

### 4. Launch everything

```bash
docker compose -f docker/docker-compose.yaml up -d
```

Wait for services to become healthy:

```bash
docker compose -f docker/docker-compose.yaml ps
# mysql   healthy
# kafka   running
# api     running
# frontend running
```

Open `http://localhost:3000` — the Bloomberg-style board is live.

> **First time?** Start the Kafka producers to begin ingesting market data:
> ```bash
> docker compose -f docker/docker-compose.yaml run --rm producer-trade     # X-TRADE:ALL
> docker compose -f docker/docker-compose.yaml run --rm producer-quote    # X-QUOTE:ALL
> docker compose -f docker/docker-compose.yaml run --rm producer-index    # MI:ALL
> docker compose -f docker/docker-compose.yaml run --rm producer-foreign # R:ALL
> docker compose -f docker/docker-compose.yaml run --rm producer-status  # F:ALL
> ```

---

## Architecture

![StreamFlow Architecture](https://raw.githubusercontent.com/bazzi24/streamflow/main/docs/assets/architecture.png)

---

## Tech Stack

| Layer            | Technology                                         |
|------------------|---------------------------------------------------|
| **Data source**   | SSI Securities WebSocket API                        |
| **Message broker**| Apache Kafka 3.x (KRaft, no ZooKeeper)              |
| **Raw storage**   | MySQL 8.0.39 (`data` DB)                          |
| **DW storage**    | MySQL 8.0.39 (`warehouse` DB, star schema)         |
| **ETL**          | Apache Spark 3.5 (PySpark)                         |
| **API**          | FastAPI + Python 3.12 + aiokafka                 |
| **Frontend**      | React 18 + Vite + TypeScript + Tailwind CSS       |
| **Charts**        | TradingView `lightweight-charts v4`                |
| **i18n**          | `react-i18next` (VI + EN, persisted to localStorage) |
| **Containers**    | Docker Compose                                     |

---

## Services

| Service        | Description                               | Port     |
|----------------|------------------------------------------|----------|
| `kafka`        | Kafka KRaft broker                        | 9092     |
| `kafka-ui`     | Kafka UI dashboard                        | 8080     |
| `mysql`         | MySQL 8.0.39 (`data` + `warehouse` DBs) | 3306     |
| `spark-master`  | Apache Spark master                       | 7077, 8082 |
| `spark-worker` | Spark worker (2 cores, 4 GB)             | —        |
| `api`          | FastAPI REST + WebSocket                  | 8000     |
| `frontend`     | React SPA served via Nginx                | 80→3000  |
| `producer`     | All 5 Kafka producers (SSI channels)        | —        |
| `consumer`     | All 6 Kafka consumers (5 topics + OHLCV)   | —        |

---

## API Reference

Base: `http://localhost:8000/api/v1`

### REST Endpoints

| Endpoint                    | Method    | Auth | Description                    |
|----------------------------|-----------|------|--------------------------------|
| `/auth/register`            | POST     | —    | Register user                  |
| `/auth/login`              | POST     | —    | Login → JWT (24h)             |
| `/users/me`                | GET      | JWT  | Current user                  |
| `/users/me/watchlist`       | GET/PUT  | JWT  | Watchlist CRUD                |
| `/stocks`                  | GET      | —    | All symbols + latest prices   |
| `/stocks/{symbol}`          | GET      | —    | Symbol metadata               |
| `/stocks/{symbol}/quote`    | GET      | —    | Live bid/ask from data DB     |
| `/stocks/{symbol}/orderbook`| GET      | —    | Top 3 bid/ask levels         |
| `/stocks/{symbol}/ohlcv`    | GET      | —    | Intraday OHLCV (`?interval=1m`) |
| `/stocks/{symbol}/history`  | GET      | —    | Daily OHLCV (`?days=30`)     |
| `/market/overview`          | GET      | —    | Indices + top gainers/losers |
| `/health`                  | GET      | —    | Health check                 |

### WebSocket Endpoints

| Endpoint             | Auth      | Description             |
|----------------------|-----------|------------------------|
| `/ws/stocks/{symbol}` | Optional  | Per-symbol live updates |
| `/ws/market`         | Optional  | Market-wide updates    |

**Message types:** `price_update` · `orderbook_update` · `index_update` · `candlestick_update`

---

## Database Schema

Two MySQL databases — **`data`** (raw + reference + candlestick) and **`warehouse`** (star-schema DW).

### `data` — Raw + reference + candlestick

| Table               | Source / Writer                | PK + Index                              |
|---------------------|-------------------------------|------------------------------------------|
| `data_trade`         | `market_data_trade` topic      | `PRIMARY KEY (id)`, `INDEX(symbol, trading_date)` |
| `data_quote`         | `market_data_quote` topic       | `PRIMARY KEY (id)`, `INDEX(symbol_id, trading_date)` |
| `index_data`         | `index_data` topic             | `PRIMARY KEY (id)`, `INDEX(index_id, trading_date)` |
| `foreign_room`        | `foreign_room_data` topic       | `PRIMARY KEY (id)`, `INDEX(symbol, trading_date)` |
| `securities_status`   | `securities_status` topic       | `PRIMARY KEY (id)`, `INDEX(symbol_id, trading_date)` |
| `candlestick_1m`     | CandlestickConsumer             | `PRIMARY KEY (symbol, time_start)` |
| `candlestick_1d`      | CandlestickConsumer             | `PRIMARY KEY (symbol, trading_date)` |

> `candlestick_1m` is the **source of truth** for 1m OHLCV; larger timeframes are derived at query time.

### `warehouse` — Star-schema DW

| Table           | Key / Composite PK columns                    |
|-----------------|-------------------------------------------|
| `date`          | `tradingdate_key` (`date` needs backticks) |
| `time`          | `time_key` (`time` needs backticks)         |
| `symbol`        | `symbol_key`                               |
| `market_index`  | `index_key`                               |
| `exchange`      | `exchange_key`                             |
| `tradingsession`| `trading_session_key`                      |
| `stockorderbook`| tradingdate_key, time_key, symbol_key, exchange_key, session_key |
| `stocktrade`    | same as stockorderbook                      |
| `marketindex`   | same as above                              |

---

## Environment Variables

All secrets are loaded from `.env` via `env_file` in docker-compose. The FastAPI service requires the variables below.

### Required (app fails fast if missing)

| Variable       | Example                               | Purpose                           |
|----------------|---------------------------------------|-----------------------------------|
| `SECRET_KEY`   | `b7649d31d...` (hex 64-char token)   | JWT signing — **never commit a default** |
| `DB_USER`      | `root` (dev) / `streamflow_app` (prod) | MySQL username                     |
| `DB_PASSWORD`  | —                                     | MySQL password                     |

Generate a secure `SECRET_KEY`:
```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Optional (have sensible defaults)

| Variable               | Default                              | Purpose                        |
|------------------------|--------------------------------------|--------------------------------|
| `CORS_ORIGINS`         | `http://localhost:3000`              | Comma-separated allowed origins  |
| `consumerID`           | —                                    | SSI API consumer ID             |
| `consumerSecret`       | —                                    | SSI API consumer secret         |
| `url`                 | `https://fc-data.ssi.com.vn/`         | SSI REST API base               |
| `stream_url`           | `https://fc-datahub.ssi.com.vn/`      | SSI WebSocket base             |
| `RAW_DB_URL`          | `jdbc:mysql://mysql:3306/data?...`    | Spark reads data DB            |
| `DW_DB_URL`           | `jdbc:mysql://mysql:3306/warehouse?...` | Spark writes warehouse DB      |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092`                      | All Kafka clients               |
| `SPARK_MASTER_URL`     | `spark://spark-master:7077`          | spark-submit target             |
| `MYSQL_JAR`           | `/streamflow/lib/mysql-connector-j-8.0.33.jar` | JDBC driver path       |

### Production: Limited-privilege MySQL user

Run this in MySQL to create a read-write app user (replace the password):

```sql
-- In docker exec mysql, or via any mysql client:
CREATE USER IF NOT EXISTS 'streamflow_app'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT, INSERT, UPDATE, DELETE ON `data`.* TO 'streamflow_app'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `warehouse`.* TO 'streamflow_app'@'%';
FLUSH PRIVILEGES;
-- Then update .env:
-- DB_USER=streamflow_app
-- DB_PASSWORD=<strong-password>
```

---

## Stopping

```bash
docker compose -f docker/docker-compose.yaml down            # stop containers (data persists)
docker compose -f docker/docker-compose.yaml down -v          # stop + remove volumes (data loss)
```

---

## License

MIT License — [bazzi24](https://github.com/bazzi24)
