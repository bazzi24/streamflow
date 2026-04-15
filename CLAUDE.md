# StreamFlow — Project Context

> Real-time Vietnamese stock market data pipeline: SSI WebSocket API → Kafka → MySQL (streaming) → Spark ETL → MySQL (DW) → FastAPI → React frontend

---

## Data Flow

```
SSI WebSocket API
        │
        ▼
┌──────────────────────────────────────────────────────┐
│  kafkaStream/producer_market_data.py                 │
│  5 producers → 5 Kafka topics (partitioned by symbol)│
│  topics: market_data_trade, market_data_quote,      │
│          index_data, foreign_room_data,               │
│          securities_status                            │
└────────────────────────────┬─────────────────────────┘
                             │ Kafka KRaft (6 partitions)
                             ▼
┌──────────────────────────────────────────────────────┐
│  consumer/*.py                                       │
│  5 consumers → MySQL data.streaming tables           │
│  batch_size=50000, executemany, enable_auto_commit=F │
│  + consumer/candlestick.py                           │
│    (CandlestickConsumer — 1m/1d OHLCV upserts)      │
└────┬─────────────────────┬──────────────────────────┘
     │                     │
     ▼                     ▼
data.streaming         data.corporation / data.market
(raw tables)           (reference data)
     │
     ├────────────────────┬──────────────────────┐
     ▼                    ▼                      ▼
data.candlestick    warehouse.dim            warehouse.fact
(pre-computed OHLCV) (Spark ETL)            (Spark ETL)
     │
     └──────────────┬───────────────┘
                    ▼
┌──────────────────────────────────────────────────────┐
│  api_service/src/ — FastAPI REST + WebSocket          │
│  • aiokafka bridge → WebSocket broadcast            │
│  • REST reads: data.streaming (live),               │
│    data.candlestick (OHLCV), warehouse (historical) │
│  • JWT auth via api DB (created at runtime)         │
│  • 2 DB engines: warehouse (DW), data (streaming)   │
└────────────────────────────┬─────────────────────────┘
                             │ REST + WebSocket
                             ▼
┌──────────────────────────────────────────────────────┐
│  frontend/src/ — React 18 + Vite + TypeScript        │
│  • PriceBoardPage: Bloomberg-style board (dark)       │
│  • ChartPageV2: white TradingView-style dashboard    │
│  • lightweight-charts v4                             │
│  • Zustand (persisted to localStorage)              │
│  • React Query (15s stale time)                     │
│  • WebSocket hook (3s auto-reconnect)              │
└──────────────────────────────────────────────────────┘
```

---

## Kafka Topics

| Topic | Producer channel | Consumer | Partition key |
|---|---|---|---|
| `market_data_trade` | `X-TRADE:ALL` | `consumer/dataTrade.py` | symbol |
| `market_data_quote` | `X-QUOTE:ALL` | `consumer/dataQuote.py` | symbol |
| `index_data` | `MI:ALL` | `consumer/index.py` | index_id |
| `foreign_room_data` | `R:ALL` | `consumer/foreignRoom.py` | symbol |
| `securities_status` | `F:ALL` | `consumer/securitiesStatus.py` | symbol |

> `candlestick_1m` is **not** a Kafka topic — pre-computed OHLCV is written directly to `data.candlestick_1m` by `consumer/candlestick.py`.

> All SSI messages arrive as `{"Content": "{...inner JSON string...}"}` — parse the inner string as JSON before processing.

---

## Database Schema

> Layout: 2 MySQL databases — `data` (raw + reference + candlestick) and `warehouse` (star-schema DW).
> `api` database (user + watchlist) is created at runtime by `api_service/src/main.py` lifespan, not via `init.sql`.
> `ml_data` has been dropped — ML pipeline is not running.

### `data` — Raw + reference + pre-computed charts

#### `data.market` — Exchange / index metadata

| Table | PK | Notes |
|---|---|---|
| `exchange` | `exchange_key` | UNIQUE on `exchange_name` |
| `indexlist` | `index_id` | FK → `exchange` |
| `indexcomponent` | `(index_id, symbol, effective_date)` | FK → `exchange`; populated by `dataSSI/load_index_components.py` |
| `dailyindex` | `(index_id, trading_date)` | Daily index snapshots |

#### `data.corporation` — Symbol master + reference

| Table | PK | Notes |
|---|---|---|
| `sector` | `sector_id` | — |
| `corporation` | `symbol_id` | FK → `sector`, `exchange` |
| `corporation_detail` | `symbol_id` | FK → `corporation`; listing_date, par_value, address, foreign_max_room, stock_type… |

#### `data.streaming` — Raw tick data (Kafka consumers)

| Table | Source topic | Indexes |
|---|---|---|
| `data_trade` | `market_data_trade` | `PRIMARY KEY (id)`, `INDEX (symbol, trading_date)` |
| `data_quote` | `market_data_quote` | `PRIMARY KEY (id)`, `INDEX (symbol_id, trading_date)` |
| `index_data` | `index_data` | `PRIMARY KEY (id)`, `INDEX (index_id, trading_date)` |
| `foreign_room` | `foreign_room_data` | `PRIMARY KEY (id)`, `INDEX (symbol, trading_date)` |
| `securities_status` | `securities_status` | `PRIMARY KEY (id)`, `INDEX (symbol_id, trading_date)` |

#### `data.candlestick` — Pre-computed OHLC candles

| Table | PK | Notes |
|---|---|---|
| `candlestick_1m` | `(symbol, time_start)` | Source of truth for 1m; larger timeframes derived at query time |
| `candlestick_1d` | `(symbol, trading_date)` | Includes nn_mua, nn_ban, room from foreign_room |

### `warehouse` — Star-schema data warehouse

#### `warehouse.dim` — Dimension tables (Spark ETL)

| Table | PK | UNIQUE constraint | Notes |
|---|---|---|---|
| `date` | `tradingdate_key` | `tradingdate` | `date` column needs backticks |
| `time` | `time_key` | `time_hh_mm_ss` | `time` column needs backticks |
| `symbol` | `symbol_key` | `symbol` | FK → `data.corporation.sector`, `data.exchange` |
| `market_index` | `index_key` | `index_name` | FK → `data.exchange` |
| `exchange` | `exchange_key` | `exchange_name` | — |
| `tradingsession` | `trading_session_key` | `trading_session` | — |

#### `warehouse.fact` — Fact tables (Spark ETL)

| Table | Composite PK columns | Indexes |
|---|---|---|
| `stockorderbook` | tradingdate_key, time_key, symbol_key, exchange_key, trading_session_key | `(symbol_key, tradingdate_key)`, `(tradingdate_key, exchange_key)` |
| `stocktrade` | same | same |
| `marketindex` | tradingdate_key, time_key, index_key, exchange_key, trading_session_key | `(index_key, tradingdate_key)`, `(tradingdate_key, exchange_key)` |

### `api` — Application data (created at runtime by `main.py` lifespan)

| Table | Notes |
|---|---|
| `user` | email, username, password_hash (bcrypt), is_active |
| `watchlist` | user_id FK → `api.user`, symbol, position |

### MySQL Reserved Words

These **must** be backtick-quoted in raw SQL / JDBC:

| Word | Where |
|---|---|
| `` `date` `` | Column in `warehouse.dim.date` |
| `` `time` `` | Column in `warehouse.dim.time` |
| `` `change` `` | Column in `data.streaming.data_trade`, `data.streaming.index_data`, `warehouse.fact.stocktrade`, `warehouse.fact.marketindex` |
| `` `floor` `` | Column in `data.streaming.data_trade`, `warehouse.fact.stocktrade` |

---

## ETL Jobs

| Job | Reads | Writes | Lookback |
|---|---|---|---|
| `etl/dimSymbol.py` | data.corporation, data.sector | warehouse.dim.symbol | Full (distinct) |
| `etl/dimDate.py` | data.data_trade | warehouse.dim.date | Full (distinct trading dates) |
| `etl/dimTime.py` | data.data_quote | warehouse.dim.time | Full (distinct times) |
| `etl/dimExchange.py` | data.exchange | warehouse.dim.exchange | Full |
| `etl/dimIndex.py` | data.indexlist | warehouse.dim.market_index | Full |
| `etl/dimSession.py` | data.data_trade | warehouse.dim.tradingsession | Full |
| `etl/factQuote.py` | data.data_quote + dims | warehouse.fact.stockorderbook | Last 2 days |
| `etl/factTrade.py` | data.data_trade + dims | warehouse.fact.stocktrade | Last 5 days |
| `etl/factMarketIndex.py` | data.index_data + dims | warehouse.fact.marketindex | All |

Run: `spark-submit --master spark://spark-master:7077 --jars lib/mysql-connector-j-8.0.33.jar etl/<job>.py`

---

## API Service (`api_service/src/`)

**Base URL**: `http://api:8000/api/v1`

### REST Endpoints

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/auth/register` | POST | — | Register user |
| `/auth/login` | POST | — | Login → JWT |
| `/users/me` | GET | JWT | Current user |
| `/users/me/watchlist` | GET/PUT | JWT | Watchlist CRUD |
| `/stocks` | GET | — | All symbols + latest prices (`?exchange=HOSE`, `?segment=WARRANT`) |
| `/stocks/{symbol}` | GET | — | Symbol metadata |
| `/stocks/{symbol}/quote` | GET | — | Current bid/ask (live from data DB) |
| `/stocks/{symbol}/orderbook` | GET | — | Top 3 bid/ask levels |
| `/stocks/{symbol}/ohlcv` | GET | — | Intraday OHLCV (`?interval=1m&limit=200`) |
| `/stocks/{symbol}/history` | GET | — | Daily OHLCV (`?days=30`) |
| `/market/overview` | GET | — | Indices + top gainers/losers |
| `/health` | GET | — | Health check |

### `/stocks` — Query Parameters

| Param | Values | Notes |
|---|---|---|
| `exchange` | `HOSE`, `HNX`, `UPCOM`, `VN30`, `HNX30` | Filter by exchange. **Warrants are excluded** from exchange listings. |
| `segment` | `WARRANT`, `ETF` | Filter by segment. Warrants: `len > 3 && last4 == digits && not ETF_prefix`. ETFs: VF, E1, SSIAM… prefixes. |

### `StockSummary` Response Shape

```typescript
{
  symbol, symbol_name, exchange,
  last_price, change, ratio_change, volume, total_vol, value,
  ceiling, floor, ref_price,
  best_bid_price, best_bid_vol,   // best buy side
  best_ask_price, best_ask_vol,    // best sell side
  bid_ask_levels: [{ bid_price, bid_vol, ask_price: 0, ask_vol: 0 }], // buy side top-3
  ask_levels:       [{ bid_price: 0, bid_vol: 0, ask_price, ask_vol }], // sell side top-3
  matched_price, time,
  highest, lowest,
  nn_mua, nn_ban, room,           // from data.foreign_room
  is_warrant, is_etf,
}
```

### WebSocket Endpoints

| Endpoint | Auth | Description |
|---|---|---|
| `/ws/stocks/{symbol}` | Optional JWT | Per-symbol live updates |
| `/ws/market` | Optional JWT | Market-wide updates |

**Message types**: `price_update`, `orderbook_update`, `index_update`, `candlestick_update`

---

## Frontend (`frontend/src/`)

### Tech Stack

- **React 18** + **Vite** + **TypeScript** (strict mode)
- **Tailwind CSS** (`darkMode: "class"`)
- **lightweight-charts v4** (TradingView, MIT)
- **Zustand** — persisted to `localStorage` key `"streamflow-store"`
- **TanStack React Query** (`retry: 1`, `refetchOnWindowFocus: false`)
- **React Router v6**
- **Axios** with request/response interceptors

### Key Files

| File | Role |
|---|---|
| `App.tsx` | Router + QueryClientProvider + ProtectedRoute |
| `stores/appStore.ts` | Zustand: auth, watchlist, symbol, theme, chart toolbar state |
| `stores/types.ts` | Shared types + `isWarrant()`, `isETF()` helpers |
| `api/client.ts` | Axios instance; attaches JWT from `localStorage.getItem("access_token")`; 401 → login |
| `api/stockApi.ts` | All REST API calls + TypeScript interfaces (`listStocks(exchange?, segment?)`) |
| `hooks/useStockWebSocket.ts` | WebSocket hook (3s auto-reconnect); returns `{ isConnected }` |
| `hooks/useStockPrice.ts` | REST seed + WS live merge for quotes |
| `hooks/useStockOHLCV.ts` | Intraday + daily OHLCV via React Query |
| `pages/PriceBoard/PriceBoardPage.tsx` | **Default landing** — Bloomberg-style dark board, 9 tabs, 27-column grid |
| `pages/PriceBoard/PriceBoardPage.module.css` | Dark terminal theme CSS |
| `pages/ChartPageV2.tsx` | **Main TradingView dashboard** (white theme) |
| `pages/ChartPageV2.module.css` | White TradingView theme CSS |
| `pages/ChartPage.tsx` | Full chart page with toolbar (v1, dark theme) |
| `pages/DashboardPage.tsx` | Legacy 3-panel layout |
| `pages/HomePage.tsx` | HOSE stock grid (default landing) |
| `pages/MarketsPage.tsx` | All/HOSE/HNX/VN30/HNX30/UPCOM tabs |
| `pages/FavoritesPage.tsx` | User watchlist |
| `pages/LoginPage.tsx` | Login/register |
| `components/StockGrid.tsx` | Sortable/searchable stock table (used by MarketsPage) |
| `components/ChartToolbar.tsx` | Toolbar: intervals, chart types, drawing tools, indicators |
| `components/OrderBook.tsx` | Bid/ask table with WS live updates |
| `components/TimeAndSales.tsx` | Trade tick tape with WS live updates |
| `components/StockInfo.tsx` | Symbol metadata + live quote snapshot |
| `components/MarketOverview.tsx` | Indices + top gainers/losers |
| `components/Watchlist.tsx` | Left sidebar symbol list → navigate to chart |
| `components/Header.tsx` | Top nav with markets dropdown + auth |
| `components/ui/Card.tsx` | Card, Button, Badge primitives |
| `lib/utils.ts` | `cn()`, `formatPrice()` (VND), `formatVolume()`, `formatChange()`, `priceColor()`, `pctColor()` |

### PriceBoardPage — Default Landing (`/`)

Bloomberg-style dark terminal board. Route: `/`.

**9 Segment Tabs:**
`Danh mục` · `VN30` · `HNX30` · `HOSE` · `HNX` · `UPCOM` · `ETF` · `Phái sinh` · `Warrant`

**27-Column Grid:**
CK · Trần · Sàn · TC · Giá 3/KL 3 · Giá 2/KL 2 · **Giá 1/KL 1 (Mua)** · Giá/KL (Khớp) · **Giá 1/KL 1 (Bán)** · Giá 2/KL 2 · Giá 3/KL 3 · +/- · % · Tổng KL · Cao · Thấp · NN Mua · NN Bán · Room

- Buy side columns (Giá 1–3 / KL 1–3): green
- Sell side columns: red
- Warrant symbols (>3 chars with 4-digit suffix): shown only on **Warrant** tab; show 🟡 **W** badge on CK cell
- ETF symbols (VF, E1, SSIAM…): shown only on **ETF** tab; show 🔵 **E** badge
- NN Mua / NN Bán / Room: populated from `data.foreign_room` via API join
- Price format: VND (dot thousands separator, e.g. `15.100`)
- Warrant detection: `len > 3 && last 4 chars are digits && not ETF prefix`

**Color Palette (dark theme):**
- Background: `#0b0e17` | Row A: `#101520` | Row B: `#0d1120`
- Up/Buy: `#00e676` | Down/Sell: `#ff3d57` | Accent: `#00d4ff` | Purple: `#c084fc`
- Font: IBM Plex Mono / JetBrains Mono

### ChartPageV2 — Main Dashboard

Route: `/chart/:symbol`, white TradingView-style theme.

**Layout:**
```
┌──────────────────────────────────────────────────────┬──────────────────┐
│  HEADER: Logo | Symbol Search | MA5/10/20/50 | 🔔 ⚙️ │ User Badge      │
├──────┬───────────────────────────────────────────────┼──────────────────┤
│ LEFT │  LEGEND BAR: VCB · HOSE · O/H/L/C/Vol · $   │  RIGHT SIDEBAR  │
│ TOOL │──────────────────────────────────────────────│  [★] [📋] [📰] │
│ BAR  │                                               │  ┌────────────┐ │
│  ⊕   │   CANDLESTICK CHART (lightweight-charts)     │  │ 🇻🇳 Stocks │ │
│  ╱   │   + Volume histogram below                   │  │  VCB MBB   │ │
│  ─   │   [crosshair → OHLCV overlay]               │  │  ACB FPT   │ │
│  ∿   │                                               │  ├────────────┤ │
│  │   ├──────────────────────────────────────────────│  │ ₿ Crypto   │ │
│  ▬   │  RANGE TABS: 1m 5m 15m 30m 1h 4h 1D 1W 1M  │  │  BTC ETH   │ │
│  ✱   │                                               │  │  SOL BNB   │ │
└──────┴───────────────────────────────────────────────┴──────────────────┘
```

**Dynamic View Switching:**
- **Vietnam Stocks** (VCB, MBB, ACB, FPT, HPG, VND, TCB, VPB): Full candlestick chart, volume histogram, OHLCV legend bar, crosshair tooltip overlay.
- **Crypto** (BTC, ETH, SOL, BNB, XRP): Chart hidden. Shows price card with large price, 24h change badge (▲/▼), notice banner, and 24h High/Low/Volume stats.

**Right Sidebar Tabs:**
- **Watchlist** (★): Dual-category — Vietnam Stocks + Crypto, collapsible sections, active row highlighting, live price + change%.
- **Details** (📋): Symbol name, price, change%, and key stats.
- **News** (📰): Static news items with timestamps.

**Color Palette (white theme):**
- Up: `#16a34a` | Down: `#dc2626` | Accent: `#2962ff` | Purple: `#7c3aed`
- Background: `#ffffff` | Secondary: `#f3f4f6` | Text: `#1f2937`

**Key TypeScript patterns:**
```typescript
// API responses are Axios responses — unwrap with .then((r) => r.data)
const { data: quote } = useQuery({
  queryKey: ["quote", symbol],
  queryFn: () => stockApi.getQuote(symbol).then((r) => r.data),
  staleTime: 15_000,
});

// listStocks with segment filter (e.g. Warrant tab)
stockApi.listStocks(undefined, "WARRANT").then((r) => r.data);

// Crosshair overlay: inject HTML into chart container
const overlay = container.querySelector(".tv-crosshair-legend") as HTMLElement | null;
if (overlay) overlay.innerHTML = `<span>...</span>`;
```

---

## Docker Services (`docker/docker-compose.yaml`)

19 services on the `streamflow` network:

| Service | Role | Port |
|---|---|---|
| `kafka` | KRaft broker | 9092 |
| `kafka-ui` | Kafka UI | 8080 |
| `mysql` | MySQL 8.0.39 | 3306 |
| `spark-master` | Spark master | 7077, 8082 |
| `spark-worker` | Spark worker | — |
| `api` | FastAPI | 8000 |
| `frontend` | React/Nginx | 3000→80 |
| `producer` | Kafka producers (5 channels) | — |
| `consumer` | Kafka consumers (5 topics + candlestick) | — |

Start: `docker compose -f docker/docker-compose.yaml up -d`

---

## Environment Variables

| Variable | Example | Purpose |
|---|---|---|
| `RAW_DB_URL` | `jdbc:mysql://mysql:3306/data?...` | Spark reads data DB (streaming + corporation + market) |
| `DW_DB_URL` | `jdbc:mysql://mysql:3306/warehouse?...` | Spark writes warehouse.dim + warehouse.fact |
| `DB_DRIVER` | `com.mysql.cj.jdbc.Driver` | JDBC class |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | All Kafka clients |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | spark-submit target |
| `MYSQL_JAR` | `/streamflow/lib/mysql-connector-j-8.0.33.jar` | Spark JDBC jar path |
| `consumerID` / `consumerSecret` | — | SSI API credentials |
| `url` | `https://fc-data.ssi.com.vn/` | SSI REST API base |
| `stream_url` | `https://fc-datahub.ssi.com.vn/` | SSI WebSocket base |

---

## Code Patterns

### Kafka message parsing (consumers)
```python
# SSI sends: {"Content": "{...json string...}"}
msg = json.loads(msg.value.decode())        # outer dict
inner = json.loads(msg["Content"])           # inner JSON string → dict
```

### Kafka producer keying (ensures per-symbol ordering)
```python
producer.send(topic, value=payload, key=symbol.encode())
producer.flush()   # flush every batch to avoid buffering loss
```

### Date conversion (DD/MM/YYYY → YYYY-MM-DD)
```python
from datetime import datetime
date_str = "20/03/2026"
iso_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
```

### Spark JDBC write with reserved words
```python
df.write.jdbc(url=DW_DB_URL, table="`warehouse`.`time`", mode="append")
```

### Axios response unwrapping (React Query)
```typescript
// stockApi methods return AxiosResponse<T>; unwrap with .then((r) => r.data)
useQuery({
  queryKey: ["quote", symbol],
  queryFn: () => stockApi.getQuote(symbol).then((r) => r.data),
})
```

### Warrant detection (backend = definitive, frontend = helper)
```python
# Backend: StockService._is_warrant()
def _is_warrant(symbol: str) -> bool:
    return (
        len(symbol) > 3
        and symbol[-4:].isdigit()
        and symbol[:2] not in {"VF", "E1", "SSIAM", "VOF", "VFA", "VCA"}
    )
```

---

## Directory Structure

```
StreamFlow/
├── .env                                    # Env vars (gitignored)
├── Dockerfile                              # App image (uv, ubuntu:24.04)
├── docker/
│   ├── docker-compose.yaml                # 8 services (kafka, mysql, spark, api, frontend, producer, consumer)
│   └── init.sql                            # Full DDL: data + warehouse databases
├── kafkaStream/
│   ├── producer_market_data.py             # Single-channel producer (key + flush)
│   └── producer_unified.py                 # All 5 channels in parallel threads
├── consumer/
│   ├── base_consumer.py                    # connect_kafka(), connect_db()
│   ├── consumer_unified.py                 # All 6 consumers in parallel threads (5 topics + candlestick)
│   ├── dataTrade.py                        # market_data_trade → data.data_trade
│   ├── dataQuote.py                        # market_data_quote → data.data_quote
│   ├── index.py                            # index_data → data.index_data
│   ├── foreignRoom.py                      # foreign_room_data → data.foreign_room
│   ├── securitiesStatus.py                 # securities_status → data.securities_status
│   └── candlestick.py                      # CandlestickConsumer: 1m/1d OHLCV upserts
├── etl/
│   ├── dimSymbol.py                        # data.corporation → warehouse.dim.symbol
│   ├── dimDate.py                          # data.data_trade → warehouse.dim.date
│   ├── dimTime.py                          # data.data_quote → warehouse.dim.time
│   ├── dimExchange.py                      # data.exchange → warehouse.dim.exchange
│   ├── dimIndex.py                         # data.indexlist → warehouse.dim.market_index
│   ├── dimSession.py                       # data.data_trade → warehouse.dim.tradingsession
│   ├── factQuote.py                        # data.data_quote + dims → warehouse.fact.stockorderbook
│   ├── factTrade.py                        # data.data_trade + dims → warehouse.fact.stocktrade
│   └── factMarketIndex.py                  # data.index_data + dims → warehouse.fact.marketindex
├── api_service/src/
│   ├── main.py                             # FastAPI app + lifespan (creates api DB at runtime)
│   ├── config.py                           # Pydantic settings (2 DB URLs)
│   ├── database.py                         # 2 SQLAlchemy engines: warehouse (DW) + data (streaming)
│   ├── api/
│   │   ├── deps.py                         # JWT auth dependency (uses api DB via lifespan)
│   │   ├── router.py                       # Root router (aggregates v1)
│   │   └── v1/
│   │       ├── auth.py                     # /auth/register, /auth/login
│   │       ├── market.py                   # /market/overview
│   │       ├── stocks.py                   # /stocks, /stocks/{symbol}, /stocks/{symbol}/*
│   │       ├── users.py                    # /users/me, /users/me/watchlist
│   │       └── websocket.py                # /ws/stocks/{symbol}, /ws/market
│   ├── core/
│   │   └── security.py                     # JWT token creation/verification
│   ├── db/
│   │   └── user_repo.py                    # User repository (get_by_email, create, etc.)
│   ├── models/__init__.py                 # SQLAlchemy models (data.* + warehouse.* + api.*)
│   ├── schemas/
│   │   ├── auth.py                        # LoginRequest, Token, TokenData
│   │   └── stock.py                       # Pydantic schemas (StockSummary, etc.)
│   ├── services/
│   │   ├── auth_service.py                 # AuthService (verify_password, create_access_token)
│   │   └── stock_service.py                # StockService (live reads, warrant detection)
│   └── websocket/
│       ├── manager.py                      # ConnectionManager (rooms + broadcast)
│       └── bridge.py                       # Kafka → WebSocket bridge loop
├── frontend/src/
│   ├── App.tsx                             # Router + providers
│   ├── api/
│   │   ├── client.ts                       # Axios instance + interceptors
│   │   └── stockApi.ts                     # REST calls + TypeScript types
│   ├── components/                         # ChartToolbar, OrderBook, TimeAndSales, etc.
│   ├── hooks/
│   │   ├── useStockWebSocket.ts             # WS hook (3s auto-reconnect)
│   │   ├── useStockPrice.ts               # REST seed + WS live merge
│   │   └── useStockOHLCV.ts              # Intraday + daily OHLCV
│   ├── pages/
│   │   ├── PriceBoard/
│   │   │   ├── PriceBoardPage.tsx         # ★ Default landing — Bloomberg board (dark)
│   │   │   └── PriceBoardPage.module.css  # Dark terminal theme
│   │   ├── ChartPageV2.tsx                 # ★ Main TradingView dashboard (white)
│   │   ├── ChartPageV2.module.css         # White TV theme CSS
│   │   ├── ChartPage.tsx                  # v1 dark theme chart page
│   │   ├── Dashboard/                        # Legacy 3-panel layout
│   │   │   ├── DashboardPage.tsx
│   │   │   ├── DashboardPage.module.css
│   │   │   ├── components/                   # MainChart, MarketBreath, MarketHeatmap,
│   │   │   │   ├── MainChart/               #   OrderBookPanel, StatsPanel, WatchlistPanel
│   │   │   │   ├── MarketBreath/
│   │   │   │   ├── MarketHeatmap/
│   │   │   │   ├── OrderBookPanel/
│   │   │   │   ├── StatsPanel/
│   │   │   │   └── WatchlistPanel/
│   │   │   ├── hooks/                       # useMarketBreath, useMarketHeatmap,
│   │   │   │   └── useSynchronizedView/
│   │   │   └── types/
│   │   │       └── dashboard.ts
│   │   ├── HomePage.tsx                   # HOSE stock grid
│   │   ├── MarketsPage.tsx                # All/HOSE/HNX/VN30/HNX30/UPCOM
│   │   ├── FavoritesPage.tsx              # User watchlist
│   │   └── LoginPage.tsx                  # Login/register
│   ├── stores/
│   │   ├── appStore.ts                    # Zustand store
│   │   └── types.ts                       # MarketSegment, isWarrant(), isETF()
│   └── lib/utils.ts                       # Formatting helpers (VND formatPrice)
├── lib/
│   ├── mysql-connector-j-8.0.33.jar       # JDBC driver for Spark
│   └── (Flink libs removed — candlestick computed by consumer/candlestick.py)
├── orchestration/
│   ├── orchestration.sh                    # Start 5 producers (gnome-terminal)
│   └── orchestration_consumer.sh           # Start 5 consumers (gnome-terminal)
├── common/
│   └── time_utils.py                      # Date/time helpers
├── dataSSI/                               # SSI API client + initial load scripts
└── test/                                  # Unit tests
```
