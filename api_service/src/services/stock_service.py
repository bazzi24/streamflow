from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from sqlalchemy.exc import OperationalError
from ..models import (
    StreamingDataTrade, StreamingDataQuote, StreamingIndexData,
    SymbolDim, StockTradeFact, StockOrderBookFact, MarketIndexFact,
    Candlestick1M, Candlestick1D, TradeMatchArchive,
)
from ..schemas.stock import (
    StockQuote, OrderBook, OrderBookLevel, OHLCVBar, SymbolMeta,
    StockSummary, IndexOverview, MarketOverviewResponse, BidAskLevel,
    TradeMatch, PaginatedStocksResponse,
)
from ..schemas.validators import (
    validate_symbol,
    validate_interval,
    validate_limit,
    validate_offset,
    validate_optional_date,
    validate_optional_exchange,
    validate_segment,
)
from common.decorator import retry
from datetime import datetime, timezone, timedelta
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

# Vietnam is UTC+7 — Naive datetime from MySQL is stored in Asia/Ho_Chi_Minh time.
VIETNAM_TZ = timezone(timedelta(hours=7))

# ETF prefix list used in warrant detection (must stay in sync with frontend)
_ETF_PREFIXES: frozenset[str] = frozenset({"VF", "E1", "SSIAM", "VOF", "VFA", "VCA"})

def _to_ms(dt) -> int:
    """
    Convert a naive datetime to Unix ms.
    - DATETIME (time_start from candlestick_1m): MySQL stores local Vietnam time → treat as UTC+7.
    - DATE    (trading_date from candlestick_1d): midnight in MySQL → market open (09:00 Vietnam = 02:00 UTC).
      Detected by checking if hour/minute/second are all 0.
    """
    if dt is None:
        return 0
    # DATE (pure date, e.g. 2026-04-15 00:00:00) → shift to 09:00 Vietnam market open
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        dt = dt.replace(hour=9)
    return int(dt.replace(tzinfo=VIETNAM_TZ).timestamp() * 1000)


# ── Interval helpers ─────────────────────────────────────────────────────────────

def _interval_to_seconds(interval: str) -> int:
    """Convert a timeframe string (1m, 5m, 1h, 1D, 1W, 1M …) to seconds."""
    unit = interval[-1]
    num = int(interval[:-1])
    if unit == "m":
        return num * 60
    if unit == "h":
        return num * 3600
    if unit == "D":
        return num * 86400
    if unit == "W":
        return num * 604800
    if unit == "M":
        # Approximate 1 month = 30 days for bucketing purposes
        return num * 2592000
    # Fallback: treat as minutes
    return num * 60


# Vietnam market open = 09:00 local = 02:00 UTC.
# When bucketing D/W/M candles, align bucket start to Vietnam market open
# instead of midnight (UTC).  Offset = 2 hours in milliseconds.
_VIETNAM_OPEN_MS = 2 * 3600 * 1000  # 2 hours → milliseconds


def _f(val, *, scale=1) -> float:
    if val is None:
        return 0.0
    try:
        return float(val) / scale
    except (TypeError, ValueError):
        return 0.0


def _i(val) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


class StockService:
    """Reads live data from streaming DB (populated by Kafka consumers).
    Falls back to DW tables (fact.*, dim.*) if streaming is empty.
    """

    def __init__(self, streaming_db: Session, warehouse_db: Session):
        self.streaming_db = streaming_db
        self.warehouse_db = warehouse_db

    # ── Symbols ──────────────────────────────────────────────────────────

    def list_symbols(self) -> list[SymbolMeta]:
        # Try DW first
        rows = self.warehouse_db.query(SymbolDim).filter(
            SymbolDim.symbol.isnot(None)
        ).all()
        if rows:
            return [
                SymbolMeta(symbol=r.symbol or "", symbol_name=r.symbol_name or "", sector=r.sector)
                for r in rows
            ]
        # Fall back to streaming distinct symbols
        rows = self.streaming_db.query(
            StreamingDataTrade.symbol,
            func.max(StreamingDataTrade.trading_date).label("latest"),
        ).group_by(StreamingDataTrade.symbol).all()
        return [
            SymbolMeta(symbol=r.symbol or "", symbol_name=r.symbol or "", sector=None)
            for r in rows if r.symbol
        ]

    def get_symbol_meta(self, symbol: str) -> SymbolMeta | None:
        row = self.warehouse_db.query(SymbolDim).filter(
            SymbolDim.symbol == symbol
        ).first()
        if row:
            return SymbolMeta(
                symbol=row.symbol or "",
                symbol_name=row.symbol_name or "",
                sector=row.sector,
            )
        # Fall back to streaming
        row = self.streaming_db.query(StreamingDataTrade).filter(
            StreamingDataTrade.symbol == symbol
        ).first()
        if row:
            return SymbolMeta(symbol=row.symbol or "", symbol_name=row.symbol or "", sector=None)
        return None

    # ── Quote ────────────────────────────────────────────────────────────

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    def get_quote(self, symbol: str) -> StockQuote | None:
        symbol = validate_symbol(symbol)
        # Try streaming first (most up-to-date)
        row = (
            self.streaming_db.query(StreamingDataTrade)
            .filter(StreamingDataTrade.symbol == symbol)
            .order_by(desc(StreamingDataTrade.id))
            .first()
        )
        if row:
            return StockQuote(
                symbol=symbol,
                last_price=_f(row.last_price),
                change=_f(row.change, scale=1000),
                ratio_change=_f(row.ratio_change),
                volume=_i(row.total_vol),
                value=_f(row.total_val),
                highest=_f(row.highest),
                lowest=_f(row.lowest),
                ref_price=_f(row.ref_price),
                ceiling=_f(row.ceiling),
                floor=_f(row.floor),
                time=row.time or "",
            )
        # Fall back to DW
        sym = self.warehouse_db.query(SymbolDim).filter(
            SymbolDim.symbol == symbol
        ).first()
        if not sym:
            return None
        max_date_key = (
            self.warehouse_db.query(func.max(StockTradeFact.tradingdate_key))
            .filter(StockTradeFact.symbol_key == sym.symbol_key)
            .scalar()
        )
        if not max_date_key:
            return None
        row = (
            self.warehouse_db.query(StockTradeFact)
            .filter(
                StockTradeFact.symbol_key == sym.symbol_key,
                StockTradeFact.tradingdate_key == max_date_key,
            )
            .order_by(desc(StockTradeFact.time_key))
            .first()
        )
        if not row:
            return None
        return StockQuote(
            symbol=symbol,
            last_price=_f(row.last_price),
            change=_f(row.change, scale=1000),
            ratio_change=_f(row.ratio_change),
            volume=_i(row.total_vol),
            value=_f(row.total_val),
            highest=_f(row.highest),
            lowest=_f(row.lowest),
            ref_price=_f(row.ref_price),
            ceiling=_f(row.ceiling),
            floor=_f(row.floor),
            time="",
        )

    # ── All symbols latest prices ────────────────────────────────────────

    @staticmethod
    def _is_warrant(symbol: str) -> bool:
        """
        Warrant detection: symbols ending with 4+ digits (e.g., VN30F2306).
        Note: This is a heuristic pattern match. If SSI provides an instrument_type
        field in future, that authoritative source should be used instead.
        """
        return (
            len(symbol) > 3
            and symbol[-4:].isdigit()
            and symbol[:2] not in _ETF_PREFIXES
        )

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    def list_latest_quotes(
        self, exchange: str | None = None, segment: str | None = None
    ) -> list[StockSummary]:
        """Get latest price for every symbol — streaming DB.
        Optionally filter by exchange (e.g. 'HOSE', 'HNX', 'UPCOM').
        Optionally filter by segment ('WARRANT', 'ETF').
        When exchange is 'VN30' or 'HNX30', filters to index constituents
        from data.indexcomponent.
        Warrants are excluded from regular exchange listings.

        Performance: a single JOIN query (with correlated subqueries) replaces
        the previous 4-query approach, eliminating the Python-side dict joins
        and reducing RAM pressure when many symbols are loaded.
        """
        return self._build_summaries(exchange, segment)

    def _build_summaries(
        self, exchange: str | None = None, segment: str | None = None
    ) -> list[StockSummary]:
        """Core query logic that builds StockSummary list. Used by both
        list_latest_quotes (all) and list_latest_quotes_paginated (paged)."""
        exchange = validate_optional_exchange(exchange)
        segment = validate_segment(segment) if segment is not None else None

        from ..models import StreamingDataQuote, StreamingForeignRoom, DataIndexComponent

        # ── VN30 / HNX30: resolve index constituents ──────────────────────────
        index_symbols: set[str] | None = None
        if exchange in ("VN30", "HNX30"):
            latest_date = self.streaming_db.query(
                func.max(DataIndexComponent.effective_date)
            ).filter(DataIndexComponent.index_id == exchange).scalar()
            if latest_date:
                rows = (
                    self.streaming_db.query(DataIndexComponent.symbol)
                    .filter(
                        DataIndexComponent.index_id == exchange,
                        DataIndexComponent.effective_date == latest_date,
                    )
                    .all()
                )
                index_symbols = {r.symbol for r in rows if r.symbol}
            logger.debug("Index %s constituents at %s: %d symbols", exchange, latest_date, len(index_symbols or []))

        # ── Latest trade row per symbol (deduplication) ─────────────────────
        from sqlalchemy.orm import aliased

        ranked = (
            self.streaming_db.query(
                StreamingDataTrade,
                func.row_number()
                .over(
                    partition_by=StreamingDataTrade.symbol,
                    order_by=desc(StreamingDataTrade.id),
                )
                .label("rn")
            )
            .subquery()
        )
        ranked_trade = aliased(StreamingDataTrade, ranked)

        latest_quote_id = (
            self.streaming_db.query(func.max(StreamingDataQuote.id))
            .filter(StreamingDataQuote.symbol_id == ranked_trade.symbol)
            .correlate(ranked)
            .scalar_subquery()
        )
        latest_fr_id = (
            self.streaming_db.query(func.max(StreamingForeignRoom.id))
            .filter(StreamingForeignRoom.symbol == ranked_trade.symbol)
            .correlate(ranked)
            .scalar_subquery()
        )

        from sqlalchemy.orm import aliased as _aliased

        QuoteAlias = _aliased(StreamingDataQuote)
        FRAlias    = _aliased(StreamingForeignRoom)

        base = (
            self.streaming_db.query(ranked_trade, QuoteAlias, FRAlias)
            .select_from(ranked_trade)
            .filter(ranked.c.rn == 1)
            .outerjoin(QuoteAlias, QuoteAlias.id == latest_quote_id)
            .outerjoin(FRAlias,    FRAlias.id    == latest_fr_id)
        )

        if exchange and exchange not in ("VN30", "HNX30"):
            base = base.filter(ranked_trade.exchange == exchange)

        rows = base.all()

        if index_symbols is not None:
            rows = [(r, q_row, f_row) for r, q_row, f_row in rows if r.symbol in index_symbols]

        summaries = []
        for trade_row, quote_row, foreign_row in rows:
            symbol = trade_row.symbol or ""

            is_warrant = self._is_warrant(symbol)
            is_etf = symbol[:2] in _ETF_PREFIXES or symbol[:5] in _ETF_PREFIXES

            if segment == "WARRANT":
                if not is_warrant:
                    continue
            elif segment == "ETF":
                if not is_etf:
                    continue
            else:
                if is_warrant:
                    continue

            bid_ask_levels: list[BidAskLevel] = []
            ask_levels: list[BidAskLevel] = []

            if quote_row:
                for i in range(1, 4):
                    bp = getattr(quote_row, f"bid_price{i}", None)
                    bv = getattr(quote_row, f"bid_vol{i}", None)
                    ap = getattr(quote_row, f"ask_price{i}", None)
                    av = getattr(quote_row, f"ask_vol{i}", None)
                    bid_ask_levels.append(
                        BidAskLevel(bid_price=_f(bp), bid_vol=_i(bv), ask_price=0, ask_vol=0)
                    )
                    if ap not in (None, 0):
                        ask_levels.append(
                            BidAskLevel(bid_price=0, bid_vol=0, ask_price=_f(ap), ask_vol=_i(av))
                        )

            best_bid_price = bid_ask_levels[0].bid_price if len(bid_ask_levels) >= 1 else 0
            best_bid_vol   = bid_ask_levels[0].bid_vol   if len(bid_ask_levels) >= 1 else 0
            best_ask_price = ask_levels[0].ask_price if len(ask_levels) >= 1 else 0
            best_ask_vol   = ask_levels[0].ask_vol     if len(ask_levels) >= 1 else 0

            summaries.append(
                StockSummary(
                    symbol=symbol,
                    symbol_name=trade_row.symbol or "",
                    exchange=trade_row.exchange or "",
                    last_price=_f(trade_row.last_price),
                    change=_f(trade_row.change),
                    ratio_change=_f(trade_row.ratio_change),
                    volume=_i(trade_row.total_vol),
                    last_vol=_i(trade_row.last_vol),
                    total_vol=_i(trade_row.total_vol),
                    value=_f(trade_row.total_val),
                    ceiling=_f(trade_row.ceiling),
                    floor=_f(trade_row.floor),
                    ref_price=_f(trade_row.ref_price),
                    best_bid_price=best_bid_price,
                    best_bid_vol=best_bid_vol,
                    best_ask_price=best_ask_price,
                    best_ask_vol=best_ask_vol,
                    bid_ask_levels=bid_ask_levels,
                    ask_levels=ask_levels,
                    matched_price=_f(trade_row.last_price),
                    time=trade_row.time or "",
                    highest=_f(trade_row.highest),
                    lowest=_f(trade_row.lowest),
                    nn_mua=_i(foreign_row.buy_vol) if foreign_row else 0,
                    nn_ban=_i(foreign_row.sell_vol) if foreign_row else 0,
                    room=(_i(foreign_row.current_room) if foreign_row else 0),
                    is_warrant=is_warrant,
                    is_etf=is_etf,
                )
            )
        return summaries

    def list_latest_quotes_paginated(
        self, exchange: str | None = None, segment: str | None = None,
        limit: int = 100, offset: int = 0
    ) -> PaginatedStocksResponse:
        """Get latest prices with pagination. Returns items + total count."""
        limit = validate_limit(limit)
        offset = validate_offset(offset)

        all_summaries = self._build_summaries(exchange, segment)
        total = len(all_summaries)
        items = all_summaries[offset:offset + limit]

        return PaginatedStocksResponse(
            items=items,
            total=total,
            limit=limit,
            offset=offset,
        )

    # ── Order Book ──────────────────────────────────────────────────────

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    def get_orderbook(self, symbol: str) -> OrderBook | None:
        symbol = validate_symbol(symbol)
        row = (
            self.streaming_db.query(StreamingDataQuote)
            .filter(StreamingDataQuote.symbol_id == symbol)
            .order_by(desc(StreamingDataQuote.id))
            .first()
        )
        if not row:
            return OrderBook(symbol=symbol, bids=[], asks=[], time="")

        bids, asks = [], []
        for i in range(1, 11):
            bp = getattr(row, f"bid_price{i}", None)
            bv = getattr(row, f"bid_vol{i}", None)
            ap = getattr(row, f"ask_price{i}", None)
            av = getattr(row, f"ask_vol{i}", None)
            if bp is not None:
                bids.append(OrderBookLevel(price=_f(bp), volume=_i(bv)))
            if ap is not None:
                asks.append(OrderBookLevel(price=_f(ap), volume=_i(av)))

        return OrderBook(
            symbol=symbol,
            bids=bids[:10],
            asks=asks[:10],
            time=row.time or "",
        )

    # ── OHLCV ──────────────────────────────────────────────────────────

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    def get_ohlcv(self, symbol: str, interval: str = "5m", limit: int = 200) -> list[OHLCVBar]:
        """Intraday OHLCV — reads pre-computed 1m candles from candlestick_1m,
        derives larger intervals at query time. Falls back to streaming.data_trade
        if candlestick_1m is empty.

        Bug fix (Step 2): prior version ordered by INSERTION order (ASC id),
        returning oldest N ticks instead of the latest N.
        """
        symbol = validate_symbol(symbol)
        interval = validate_interval(interval)
        limit = validate_limit(limit)

        # ── 1m candles from candlestick_1m (primary path) ───────────────────
        rows = (
            self.streaming_db.query(Candlestick1M)
            .filter(Candlestick1M.symbol == symbol)
            .order_by(desc(Candlestick1M.time_start))
            .limit(limit)
            .all()
        )

        if rows:
            # Candlestick1M rows are already in DESC order (latest first);
            # reverse to chronological for client consumption.
            rows = list(reversed(rows))

            # For 1m interval, return as-is.
            if interval == "1m":
                return [
                    OHLCVBar(
                        timestamp=_to_ms(r.time_start),
                        open=_f(r.open),
                        high=_f(r.high),
                        low=_f(r.low),
                        close=_f(r.close),
                        volume=_i(r.volume),
                    )
                    for r in rows
                ]

            # Derive larger intervals by bucketing 1m candles.
            interval_sec = _interval_to_seconds(interval)
            interval_ms = interval_sec * 1000
            # For D/W/M, align bucket to Vietnam market open (09:00 local = 02:00 UTC).
            # Subtract the 2h offset BEFORE bucketing so bucket starts at 02:00 UTC, not midnight.
            unit = interval[-1]
            offset_sec = 2 * 3600 if unit in ("D", "W", "M") else 0
            bars: dict[int, OHLCVBar] = {}
            for r in rows:
                ts = _to_ms(r.time_start)
                ts_sec = ts // 1000
                bucket_sec = ((ts_sec - offset_sec) // interval_sec) * interval_sec + offset_sec
                bucket_ms = bucket_sec * 1000
                if bucket_ms not in bars:
                    bars[bucket_ms] = OHLCVBar(
                        timestamp=bucket_ms,
                        open=_f(r.open),
                        high=_f(r.high),
                        low=_f(r.low),
                        close=_f(r.close),
                        volume=_i(r.volume),
                    )
                else:
                    b = bars[bucket_ms]
                    b.high = max(b.high, _f(r.high))
                    b.low = min(b.low, _f(r.low))
                    b.close = _f(r.close)
                    b.volume += _i(r.volume)
            return sorted(bars.values(), key=lambda x: x.timestamp)

        # ── Fallback: streaming.data_trade (Step 2 corrected ordering) ─────
        # Order DESC so we get the LATEST `limit` ticks, then reverse for
        # chronological bucketing — fixes the original ASC-ordering bug.
        rows = (
            self.streaming_db.query(StreamingDataTrade)
            .filter(StreamingDataTrade.symbol == symbol)
            .order_by(desc(StreamingDataTrade.id))
            .limit(limit)
            .all()
        )
        if not rows:
            return []

        # Reverse: oldest-first for correct chronological bucketing.
        rows = list(reversed(rows))

        interval_sec = _interval_to_seconds(interval)

        bars: dict[int, OHLCVBar] = {}
        for r in rows:
            try:
                t = r.time or "00:00:00"
                h, m, s = t.split(":")
                secs = int(h) * 3600 + int(m) * 60 + int(s)
                bucket = (secs // interval_sec) * interval_sec
            except Exception:
                bucket = r.id or 0

            if bucket not in bars:
                bars[bucket] = OHLCVBar(
                    timestamp=bucket * 1000,
                    open=_f(r.last_price),
                    high=_f(r.last_price),
                    low=_f(r.last_price),
                    close=_f(r.last_price),
                    volume=_i(r.last_vol),  # FIX: use _i() for integer volume, not _f()
                )
            else:
                b = bars[bucket]
                b.high = max(b.high, _f(r.last_price))
                b.low = min(b.low, _f(r.last_price))
                b.close = _f(r.last_price)
                b.volume += _i(r.last_vol)  # FIX: use _i() for integer volume, not _f()

        return sorted(bars.values(), key=lambda x: x.timestamp)

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    def get_history(self, symbol: str, days: int = 30) -> list[OHLCVBar]:
        """Daily OHLCV — reads pre-computed daily candles from candlestick_1d.
        Falls back to streaming.data_trade GROUP BY if candlestick_1d is empty.
        """
        symbol = validate_symbol(symbol)
        days = validate_limit(days)  # reuse limit validator for days range

        from datetime import date as date_cls, datetime, time

        # ── candlestick_1d (primary path) ───────────────────────────────────
        rows = (
            self.streaming_db.query(Candlestick1D)
            .filter(Candlestick1D.symbol == symbol)
            .order_by(desc(Candlestick1D.trading_date))
            .limit(days)
            .all()
        )
        if rows:
            # Return in chronological order (oldest first).
            rows = list(reversed(rows))
            # Use explicit datetime: trading_date + 09:00 Vietnam market open
            bars = []
            for r in rows:
                dt_vietnam = datetime.combine(r.trading_date, time(hour=9)).replace(tzinfo=VIETNAM_TZ)
                timestamp = int(dt_vietnam.timestamp() * 1000)
                bars.append(OHLCVBar(
                    timestamp=timestamp,
                    open=_f(r.open),
                    high=_f(r.high),
                    low=_f(r.low),
                    close=_f(r.close),
                    volume=_i(r.volume),
                ))
            return bars

        # ── Fallback: streaming.data_trade GROUP BY (corrected ordering) ────
        # Order DESC + limit so we get the LATEST `days` trading dates,
        # then reverse to chronological order — fixes original ASC bug.
        rows = (
            self.streaming_db.query(
                StreamingDataTrade.trading_date,
                func.max(StreamingDataTrade.last_price).label("high"),
                func.min(StreamingDataTrade.last_price).label("low"),
                func.sum(StreamingDataTrade.last_vol).label("volume"),
                func.min(StreamingDataTrade.id).label("min_id"),
            )
            .filter(StreamingDataTrade.symbol == symbol)
            .group_by(StreamingDataTrade.trading_date)
            .order_by(desc(StreamingDataTrade.trading_date))
            .limit(days)
            .all()
        )
        if not rows:
            return []

        # Reverse: oldest-first for chronological output.
        rows = list(reversed(rows))
        bars = []
        for r in rows:
            open_row = self.streaming_db.query(StreamingDataTrade).filter(
                StreamingDataTrade.id == _i(r.min_id)
            ).first()
            open_price = _f(open_row.last_price) if open_row else _f(r.high)
            # Use explicit datetime for timestamp
            if r.trading_date:
                dt_vietnam = datetime.combine(r.trading_date, time(hour=9)).replace(tzinfo=VIETNAM_TZ)
                ts = int(dt_vietnam.timestamp() * 1000)
            else:
                ts = 0
            bars.append(
                OHLCVBar(
                    timestamp=ts,
                    open=open_price,
                    high=_f(r.high),
                    low=_f(r.low),
                    close=open_price,
                    volume=_f(r.volume),
                )
            )
        return bars

    # ── Market Overview ────────────────────────────────────────────────

    def _fetch_all_summaries(self) -> list[StockSummary]:
        """Fetch all stocks (used for top gainers/losers in market overview)."""
        return self.list_latest_quotes()

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    async def get_market_overview(self) -> MarketOverviewResponse:
        from ..services.stock_cache import stock_cache

        # Get indices from latest index_data rows
        idx_rows = (
            self.streaming_db.query(
                StreamingIndexData.index_id,
                StreamingIndexData.index_name,
                func.max(StreamingIndexData.id).label("max_id"),
            )
            .group_by(StreamingIndexData.index_id, StreamingIndexData.index_name)
            .subquery()
        )
        latest_indices = (
            self.streaming_db.query(StreamingIndexData)
            .join(idx_rows, StreamingIndexData.id == idx_rows.c.max_id)
            .all()
        )

        indices: list[IndexOverview] = []
        seen = set()
        for r in latest_indices:
            key = r.index_id or ""
            if key in seen:
                continue
            seen.add(key)
            indices.append(
                IndexOverview(
                    index_id=key,
                    index_name=r.index_name or key,
                    index_value=_f(r.index_value),
                    change=_f(r.change, scale=1000),
                    ratio_change=_f(r.ratio_change),
                    advances=_i(r.advances),
                    declines=_i(r.declines),
                    nochanges=_i(r.nochanges),
                    total_qtty=_i(r.total_qtty),
                    total_value=_f(r.total_value),
                    time=r.time or "",
                )
            )

        # Cache the full quotes list (slow query ~5s; cached for 5s)
        summaries = await stock_cache.get_or_set(
            "overview:summaries",
            self._fetch_all_summaries,
        )
        sorted_summaries = sorted(summaries, key=lambda x: x.ratio_change, reverse=True)
        top_gainers = sorted_summaries[:5]
        top_losers = sorted_summaries[-5:] if len(sorted_summaries) >= 5 else sorted_summaries

        return MarketOverviewResponse(
            indices=indices,
            top_gainers=top_gainers,
            top_losers=top_losers,
        )

    # ── Trade Match Archive ────────────────────────────────────────────────

    @retry(max_attempts=3, base_delay=0.5, max_delay=5.0, exceptions=(OperationalError,))
    def get_trade_matches(self, symbol: str, date: str | None = None) -> list[TradeMatch]:
        """
        Return all matched-trade rows for a symbol from data.trade_match_archive.

        If date is None, returns today's session trades (9:15 AM – 3:30 PM).
        Sorted ascending by time so the tape reads chronologically.

        Outside market hours, this table will be empty — the frontend will
        fall back to the live WebSocket tape automatically.
        """
        symbol = validate_symbol(symbol)
        date = validate_optional_date(date)

        query = (
            self.streaming_db.query(TradeMatchArchive)
            .filter(TradeMatchArchive.symbol == symbol)
        )
        if date:
            query = query.filter(TradeMatchArchive.trading_date == date)
        else:
            # Default to today (Vietnam time)
            today = (datetime.now(VIETNAM_TZ)).date()
            query = query.filter(TradeMatchArchive.trading_date == today)

        query = query.order_by(TradeMatchArchive.time.asc())

        rows = query.all()
        return [
            TradeMatch(
                trading_date=str(r.trading_date),
                time=r.time or "",
                symbol=r.symbol or "",
                price=float(r.price) if r.price else 0.0,
                volume=int(r.volume) if r.volume else 0,
                side=r.side or "",
                price_change=float(r.price_change) if r.price_change else None,
            )
            for r in rows
        ]
