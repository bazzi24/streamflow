# StreamFlow Project Guide

## Overview

**StreamFlow** is a real-time Vietnamese stock market data platform that provides a Bloomberg-style terminal dashboard with live market data streaming. The system ingests real-time market data from the SSI (Vietnam Stock Exchange) WebSocket API, processes it through Kafka, stores it in MySQL, and serves it via a FastAPI backend with WebSocket support to a React frontend.

**Purpose**: Provide real-time tick data, OHLCV candlesticks, market breadth indicators, and trading charts for Vietnamese stock markets (HOSE, HNX, UPCoM).

**Architecture**: Event-driven microservices with Kafka as the backbone, dual-database architecture (raw streaming + star-schema data warehouse), and containerized deployment.

---

## Directory Structure

```
/StreamFlow
├── api_service/              # FastAPI REST + WebSocket service
│   ├── src/
│   │   ├── api/v1/          # REST endpoints
│   │   ├── websocket/       # bridge.py (Kafka→WebSocket), manager.py
│   │   ├── config.py        # Pydantic settings
│   │   ├── database.py      # SQLAlchemy engines
│   │   ├── db/              # Repository layer
│   │   ├── models/          # ORM models (data, warehouse, api)
│   │   └── services/        # Business logic
│   └── pyproject.toml
│
├── consumer/                 # Kafka consumers (5 topics + OHLCV)
│   ├── consumer_unified.py  # Main orchestrator (6 threads)
│   ├── candlestick.py       # OHLCV aggregator (1m + 1d)
│   ├── dataTrade.py         # Trade data consumer
│   ├── dataQuote.py         # Quote/order book consumer
│   ├── index.py             # Index data consumer
│   ├── foreignRoom.py       # Foreign room data consumer
│   └── securitiesStatus.py  # Securities status consumer
│
├── kafkaStream/              # Kafka producers
│   ├── producer_unified.py  # Orchestrator for 5 SSI channels
│   └── producer_market_data.py  # Legacy single-producer
│
├── dataSSI/                  # SSI API data loading utilities
│   ├── client.py            # FCDataClient
│   ├── config.py            # SSI configuration
│   ├── initialDataLoad.py   # Bulk load: corporation, sector, exchange
│   ├── load_index_components.py  # VN30, HNX30 constituents
│   └── crawlSector.py       # Sector scraper
│
├── etl/                      # Spark ETL jobs (PySpark)
│   ├── dimDate.py, dimTime.py, dimExchange.py
│   ├── dimIndex.py, dimSession.py, dimSymbol.py
│   ├── factQuote.py, factTrade.py, factMarketIndex.py
│   └── featureML.py          # ML feature generation
│
├── frontend/                 # React SPA (TypeScript)
│   ├── src/
│   │   ├── api/             # API clients
│   │   ├── components/      # React components
│   │   ├── pages/           # Page components (PriceBoard, ChartPageV2)
│   │   ├── hooks/           # Custom hooks (useStockWebSocket)
│   │   ├── stores/          # Zustand state stores
│   │   └── locales/         # i18n translations (vi/en)
│   ├── package.json
│   └── Dockerfile           # Multi-stage: node builder → nginx
│
├── common/                   # Shared utilities
│   ├── decorator.py         # Retry decorator
│   └── time_utils.py        # Vietnam timezone utilities
│
├── docker/                   # Docker Compose and configuration
│   ├── docker-compose.yaml  # Full stack orchestration
│   ├── init.sql             # Database schema
│   ├── kafka-init.sh        # Kafka topic setup
│   └── migration.sh         # DB migration helper
│
├── lib/                     # JAR files (MySQL JDBC driver)
├── docs/                    # Documentation assets
├── logs/                    # Runtime logs (auto-created)
├── orchestration/           # Local launch scripts (gnome-terminal)
├── .env.example             # Environment variables template
└── pyproject.toml           # Root Python dependencies (PySpark, etc.)
```

---

## Technology Stack

### Backend & Data
- **Languages**: Python 3.12, PySpark 3.5.1
- **API**: FastAPI 0.115.0 + Uvicorn 0.32.0
- **Message Broker**: Apache Kafka (KRaft mode, no ZooKeeper)
- **Storage**: MySQL 8.0.39 (2 databases: `data` + `warehouse`)
- **ORM**: SQLAlchemy 2.0.36
- **Data Source**: SSI WebSocket API via `ssi-fc-data`
- **Authentication**: python-jose (JWT), passlib/bcrypt

### Frontend
- **Framework**: React 18.3.1 + TypeScript 5.6.0
- **Build**: Vite 5.4.0
- **Styling**: Tailwind CSS 3.4.0 + CSS Modules
- **Charts**: TradingView lightweight-charts 4.2.0
- **State**: Zustand 5.0.0
- **Data Fetching**: React Query 5.60.0
- **Routing**: React Router DOM 6.28.0
- **UI**: Custom components + Radix UI (Dialog, Dropdown, Tabs)

### Infrastructure
- **Containerization**: Docker, Docker Compose
- **Kafka UI**: provectuslabs/kafka-ui
- **Package Manager**: UV (Python), npm (Node)

---

## Key Components and Workflows

### Data Flow

```
SSI WebSocket → Producers (5 channels) → Kafka (5 topics)
     ↓
Consumers (6 threads) → MySQL `data` DB (raw + candlesticks)
     ↓
Spark ETL jobs → MySQL `warehouse` DB (star schema)
     ↓
FastAPI (REST + WebSocket) → React frontend
```

### Component Responsibilities

| Component | Purpose |
|-----------|---------|
| **producer_unified.py** | Connects to SSI WebSocket, publishes raw market data to Kafka |
| **consumer_unified.py** | 6 threads: 5 topic consumers + 1 candlestick aggregator + 1 trade archive |
| **candlestick.py** | Aggregates trade ticks into 1-minute and daily OHLCV bars |
| **api_service** | FastAPI REST endpoints + WebSocket bridge to frontend |
| **Spark ETL** | Transforms raw data into dimension/fact tables in data warehouse |
| **React frontend** | Bloomberg-style board, TradingView charts, market overview |

### Database Schemas

**`data` database** (raw streaming):
- Reference tables: `exchange`, `sector`, `corporation`, `corporation_detail`
- Streaming tables: `data_trade`, `data_quote`, `index_data`, `foreign_room`, `securities_status`
- Aggregated: `candlestick_1m`, `candlestick_1d`
- Archive: `trade_match_archive`

**`warehouse` database** (star schema):
- Dimensions: `date`, `time`, `symbol`, `exchange`, `market_index`, `tradingsession`
- Facts: `stockorderbook`, `stocktrade`, `marketindex`

---

## Configuration

### Environment Variables (`.env`)

**Required:**
```
SECRET_KEY=<jwt-signing-key-generate-with-secrets.token_hex(32)>
DB_USER=root
DB_PASSWORD=<your-password>
```

**Kafka:**
```
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_ADVERTISED_HOST=kafka
```

**Spark:**
```
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_MEMORY=4G
SPARK_WORKER_CORES=2
```

**SSI API (obtain from SSI):**
```
url=https://fc-data.ssi.com.vn/
stream_url=https://fc-datahub.ssi.com.vn/
consumerID=<your-id>
consumerSecret=<your-secret>
auth_type=Bearer
```

**Databases (JDBC format):**
```
RAW_DB_URL=jdbc:mysql://mysql:3306/data?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
DW_DB_URL=jdbc:mysql://mysql:3306/warehouse?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
```

**Other:**
```
CORS_ORIGINS=http://localhost:3000
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_JAR=/streamflow/lib/mysql-connector-j-8.0.33.jar
```

### Configuration Files

- `docker/docker-compose.yaml` - Service orchestration
- `docker/init.sql` - Database schema + seed data
- `api_service/src/config.py` - Pydantic settings (validates `SECRET_KEY`)
- `dataSSI/config.py` - SSI API configuration

---

## Setup and Development

### Prerequisites
- Docker + Docker Compose
- UV (Python package manager)
- Node.js 20+ (for frontend development)
- SSI API credentials

### Local Setup

1. **Clone and environment setup:**
   ```bash
   cp .env.example .env
   # Edit .env with your SSI credentials and generate SECRET_KEY
   uv venv
   source .venv/bin/activate
   uv sync
   ```

2. **JDBC driver:**
   ```bash
   mkdir -p lib
   curl -L -o lib/mysql-connector-j-8.0.33.jar \
     https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar
   ```

3. **Start infrastructure:**
   ```bash
   docker compose -f docker/docker-compose.yaml up -d
   docker compose ps  # Wait until all services healthy
   ```

4. **Run services (development):**
   ```bash
   # Terminal 1: Producer
   python kafkaStream/producer_unified.py

   # Terminal 2: Consumer
   python consumer/consumer_unified.py

   # Terminal 3: API
   cd api_service
   python src/main.py

   # Terminal 4: Frontend
   cd frontend
   npm install
   npm run dev
   ```

5. **Access:**
   - Frontend: http://localhost:3000
   - API: http://localhost:8000/docs (Swagger UI)
   - Kafka UI: http://localhost:8080

### Running Spark ETL

```bash
# Local mode (development)
python etl/dimSymbol.py

# Cluster mode (production)
spark-submit --master spark://spark-master:7077 etl/dimSymbol.py
```

### Docker Full Stack

```bash
docker compose -f docker/docker-compose.yaml up -d
docker compose logs -f  # Follow all logs
```

---

## Testing Approach

**Current State:** The project has limited formal testing. Tests are experimental/exploratory scripts in `/test` directory:
- `test/testStream/` - SSI client and Kafka stream tests
- `test/testML/` - LSTM model training experiments
- `test/testFlink/` - PyFlink exploration

**No automated test suite** (pytest/unittest) is configured. Frontend has no testing library (Jest/React Testing Library not installed).

**Recommendations for future:**
- Add unit tests for API endpoints (FastAPI TestClient)
- Add integration tests for Kafka consumers/producers (testcontainers)
- Add component tests for React (Testing Library + Jest)
- Add E2E tests (Playwright/Cypress)

---

## Common Workflows and Patterns

### Adding a New Kafka Topic

1. **Topic creation**: Add to `docker/kafka-init.sh`
2. **Producer**: Add new `ProducerThread` in `kafkaStream/producer_unified.py`
3. **Consumer**: Add new `ConsumerThread` in `consumer/consumer_unified.py`
4. **ORM model**: Create in `api_service/src/models/data.py`
5. **Pydantic schema**: Create in `api_service/src/schemas/`
6. **API endpoint**: Add REST route in `api_service/src/api/v1/`
7. **Frontend**: Add API client functions and UI components

### Adding a New API Endpoint

1. Create Pydantic schema in `api_service/src/schemas/`
2. Add SQLAlchemy model methods in `api_service/src/models/`
3. Create service function in `api_service/src/services/` (if complex logic)
4. Add route in `api_service/src/api/v1/{module}.py`
5. Include router in `main.py` with `app.include_router()`
6. Add React Query hook in `frontend/src/api/` and use in component

### Modifying Database Schema

1. Update `docker/init.sql` (idempotent CREATE/ALTER)
2. Update SQLAlchemy models in `api_service/src/models/`
3. For production migrations, use Alembic (not currently configured)
4. Update Pydantic schemas if API responses change

### Adding a Spark ETL Job

1. Create new script in `etl/` (follow pattern from existing jobs)
2. Ensure job is idempotent (use MERGE/UPSERT)
3. Add to README deployment instructions
4. Consider adding job to docker-compose as a one-off service

### Frontend Development

- Use TypeScript strictly (no `any` where possible)
- Follow existing component patterns: `components/` for reusable, `pages/` for routes
- State management: use Zustand stores for global state, React Query for server state
- Styling: Tailwind utility classes + CSS Modules for complex components
- Charts: Use lightweight-charts wrapper components (see `components/Charts/`)
- WebSocket: Use `useStockWebSocket` hook for live data

---

## Important Files Reference

### Backend Entry Points
- `kafkaStream/producer_unified.py:main()` - Producer orchestrator (5 threads)
- `consumer/consumer_unified.py:main()` - Consumer orchestrator (6 threads)
- `api_service/src/main.py:app` - FastAPI application entry
- `consumer/candlestick.py:CandlestickConsumer` - OHLCV aggregation logic

### Database Schema
- `docker/init.sql` - Complete schema + seed data
- `api_service/src/models/__init__.py` - All ORM models (400+ lines)

### Frontend Entry
- `frontend/src/main.tsx` - React entry point
- `frontend/src/App.tsx` - Router + layout
- `frontend/src/pages/PriceBoard/PriceBoardPage.tsx` - Main dashboard
- `frontend/src/pages/ChartPageV2.tsx` - TradingView chart

### Configuration
- `docker/docker-compose.yaml` - Service definitions
- `.env.example` - Configuration template
- `api_service/src/config.py` - Settings loader
- `frontend/vite.config.ts` - Vite bundler config

### Infrastructure
- `Dockerfile` - Producer/consumer multi-stage build
- `api_service/Dockerfile` - API service build
- `frontend/Dockerfile` - Frontend build (node → nginx)

---

## Observability and Debugging

### Logs

All services write to rotating file handlers in `./logs/`:
- `producer-*.log` - Each SSI channel (5 files)
- `consumer-*.log` - Each topic consumer (5 files) + candlestick.log
- `api_service.log` (if configured) - FastAPI access/error logs

Also view via Docker:
```bash
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f api
```

### Health Checks

- FastAPI: `GET /health` → returns `{"status": "ok", "connections": N}`
- Kafka UI: http://localhost:8080
- MySQL: `docker compose exec mysql mysqladmin ping`

### Monitoring Topics

Check Kafka topics and consumer groups:
```bash
docker compose exec kafka \
  kafka-topics.sh --bootstrap-server localhost:9092 --list

docker compose exec kafka \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe
```

### Common Issues

1. **No data in frontend**: Check producers are connected to SSI, check consumer logs for errors, verify MySQL tables exist
2. **Stale candlesticks**: Candlestick consumer may not be running or batch size too small
3. **WebSocket not connecting**: Check CORS in `api_service/src/config.py`, verify `/ws` route exists
4. **Kafka not ready**: Wait ~60s after `docker compose up` for KRaft cluster to form
5. **MySQL connection errors**: Verify `.env` DB credentials match `docker-compose.yaml` environment

---

## Development Notes

### Timezone Handling

Vietnam timezone (UTC+7) is used throughout:
- `common/time_utils.py:get_vietnam_time()` returns timezone-aware datetime
- Database stores times in UTC but displays as Vietnam time
- API endpoints convert to Vietnam time in serializers

### Trade Filtering

Consumer filters trades to **buy-initiated only** (`Side='BU'`):
```python
# in consumer/dataTrade.py
if trade["Side"] not in ["BU"]:
    continue
```
This avoids double-counting as SSI reports both buy and sell sides of matched orders.

### Batch Insert Strategy

Consumers batch 50,000 records before flushing to MySQL:
- Reduces transaction overhead
- Uses `executemany()` with `INSERT ... ON DUPLICATE KEY UPDATE`
- Manual commit (no autocommit) to ensure atomicity

### WebSocket Bridge Design

The API polls `candlestick_1m` every 10 seconds (configurable in `bridge.py`) and broadcasts updates:
- Trade-off: polling introduces delay but avoids coupling candlestick updates to Kafka
- Alternative: have candlestick consumer publish to a Kafka topic and bridge consume it (event-driven)

### Spark ETL Idempotency

ETL jobs use MERGE/UPSERT patterns:
```python
df.write \
  .mode("overwrite") \
  .insertInto("warehouse.dim_symbol")
```
Most jobs can be rerun safely without data duplication.

---

## Code Style and Conventions

- **Python**: PEP 8, type hints, docstrings for public functions
- **React**: TypeScript strict mode, functional components with hooks
- **CSS**: Tailwind utility classes preferred; CSS Modules for component-scoped styles
- **Naming**: snake_case for Python files/vars, PascalCase for React components, camelCase for JS variables
- **Imports**: Standard library → third-party → local (group with blank lines)
- **Error handling**: Use retry decorator (`common/decorator.py:retry`) for transient failures
- **Logging**: Structured logging with module-level logger (`logger = logging.getLogger(__name__)`)

---

## Deployment

### Production (Docker)

```bash
docker compose -f docker/docker-compose.yaml up -d
docker compose ps  # Verify all services running
```

**Volumes:**
- `mysql_data` - MySQL data (persisted)
- `kafka_data` - Kafka logs (persisted)
- `./logs` - Host-mounted logs for debugging

**Ports:**
- 3000: Frontend (Nginx)
- 8000: API (FastAPI)
- 8080: Kafka UI
- 9092: Kafka (internal only; producers/consumers in same network)

### One-off Tasks

Run database migrations or initial data loads:
```bash
docker compose run --rm load-index   # Load index constituents
docker compose run --rm etl-dim-symbol  # Run specific ETL job
```

---

## Future Improvements

- Add automated testing (pytest, React Testing Library, Playwright)
- Add metrics (Prometheus) and tracing (OpenTelemetry)
- Add rate limiting and request validation to API
- Implement Alembic for database migrations
- Replace WebSocket bridge polling with Kafka consumer (event-driven)
- Add request/response logging middleware
- Add frontend error boundaries and error reporting (Sentry)
- Consider moving candlestick state to Redis for faster access

---

## Resources

- **SSI Documentation**: https://doc.ssi.com.vn/ (requires SSI account)
- **FastAPI**: https://fastapi.tiangolo.com/
- **Kafka**: https://kafka.apache.org/documentation/
- **React Query**: https://tanstack.com/query/
- **TradingView Charts**: https://tradingview.github.io/lightweight-charts/

---

**Maintainer Note**: This is a complex, production-grade financial data platform. Ensure SSI credentials are kept secure and rotate keys periodically. Monitor disk usage on MySQL and Kafka volumes.
