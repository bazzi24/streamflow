from sqlalchemy import Column, Integer, String, BigInteger, Numeric, Date, Time, Boolean, DateTime, ForeignKey
from sqlalchemy.sql import func
from ..database import Base


# ── data.market ──────────────────────────────────────────────────────────────

class DataExchange(Base):
    """Maps data.exchange — exchange reference table."""
    __tablename__ = "exchange"
    __table_args__ = {"schema": "data"}

    exchange_key = Column(Integer, primary_key=True, autoincrement=True)
    exchange_name = Column(String(100), unique=True, nullable=False)


class DataIndexList(Base):
    """Maps data.indexlist — market index definitions."""
    __tablename__ = "indexlist"
    __table_args__ = {"schema": "data"}

    index_id = Column(String(50), primary_key=True)
    index_name = Column(String(100), nullable=False, default="")
    exchange_key = Column(Integer, ForeignKey("data.exchange.exchange_key"), nullable=False)


class DataIndexComponent(Base):
    """Maps data.indexcomponent — constituent symbols of an index."""
    __tablename__ = "indexcomponent"
    __table_args__ = {"schema": "data"}

    index_id = Column(String(50), primary_key=True)
    symbol = Column(String(20), primary_key=True)
    exchange_key = Column(Integer, ForeignKey("data.exchange.exchange_key"), nullable=False)
    weight = Column(Numeric(10, 4))
    effective_date = Column(Date, primary_key=True)


class DataDailyIndex(Base):
    """Maps data.dailyindex — daily index snapshots."""
    __tablename__ = "dailyindex"
    __table_args__ = {"schema": "data"}

    index_id = Column(String(50), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    close_value = Column(Numeric(20, 4))
    change = Column("change", Numeric(20, 4))
    ratio_change = Column(Numeric(10, 4))
    total_qtty = Column(BigInteger)
    total_value = Column(Numeric(20, 4))
    advances = Column(Integer)
    declines = Column(Integer)


# ── data.corporation ────────────────────────────────────────────────────────

class DataSector(Base):
    """Maps data.sector — industry sector reference."""
    __tablename__ = "sector"
    __table_args__ = {"schema": "data"}

    sector_id = Column(String(50), primary_key=True)
    sector_name = Column(String(255), nullable=False, default="")


class DataCorporation(Base):
    """Maps data.corporation — symbol master table."""
    __tablename__ = "corporation"
    __table_args__ = {"schema": "data"}

    symbol_id = Column(String(20), primary_key=True)
    symbol_name = Column(String(255), nullable=False, default="")
    symbol_en_name = Column(String(255), nullable=False, default="")
    sector_id = Column(String(50), ForeignKey("data.sector.sector_id"))
    exchange_key = Column(Integer, ForeignKey("data.exchange.exchange_key"))


class DataCorporationDetail(Base):
    """Maps data.corporation_detail — extended symbol metadata."""
    __tablename__ = "corporation_detail"
    __table_args__ = {"schema": "data"}

    symbol_id = Column(String(20), primary_key=True)
    listing_date = Column(Date)
    par_value = Column(Numeric(20, 4))
    lot_size = Column(Integer)
    issuedshares = Column(BigInteger)
    listedshares = Column(BigInteger)
    address = Column(String(500))
    telephone = Column(String(100))
    fax = Column(String(100))
    website = Column(String(255))
    foreign_max_room = Column(BigInteger)
    stock_type = Column(String(50))


# ── data.streaming ───────────────────────────────────────────────────────────
# Raw tick data — populated by Kafka consumers.
# No FKs — optimised for high-throughput INSERT only.

class StreamingDataTrade(Base):
    """Maps data.data_trade — live tick data from Kafka consumer."""
    __tablename__ = "data_trade"
    __table_args__ = {"schema": "data"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    rtype = Column(String(50))
    trading_date = Column(Date)
    time = Column(String(20))
    isin = Column(String(20))
    symbol = Column(String(20))
    ceiling = Column(Numeric(20, 4))
    floor = Column("floor", Numeric(20, 4))
    ref_price = Column(Numeric(20, 4))
    avg_price = Column(Numeric(20, 4))
    prior_val = Column("prior_val", Numeric(20, 4))
    last_price = Column(Numeric(20, 4))
    last_vol = Column(BigInteger)
    total_val = Column(Numeric(20, 4))
    total_vol = Column(BigInteger)
    market_id = Column(String(50))
    exchange = Column(String(50))
    trading_session = Column(String(50))
    trading_status = Column(String(50))
    change = Column("change", Numeric(20, 4))
    ratio_change = Column(Numeric(20, 4))
    est_matched_price = Column(Numeric(20, 4))
    highest = Column(Numeric(20, 4))
    lowest = Column(Numeric(20, 4))
    side = Column(String(10))


class StreamingDataQuote(Base):
    """Maps data.data_quote — live order-book data from Kafka consumer."""
    __tablename__ = "data_quote"
    __table_args__ = {"schema": "data"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    trading_date = Column(Date)
    time = Column(String(20))
    exchange = Column(String(50))
    symbol_id = Column(String(20))
    rtype = Column(String(50))
    trading_session = Column(String(50))
    ask_price1 = Column(Numeric(20, 4));  ask_vol1 = Column(BigInteger)
    ask_price2 = Column(Numeric(20, 4));  ask_vol2 = Column(BigInteger)
    ask_price3 = Column(Numeric(20, 4));  ask_vol3 = Column(BigInteger)
    ask_price4 = Column(Numeric(20, 4));  ask_vol4 = Column(BigInteger)
    ask_price5 = Column(Numeric(20, 4));  ask_vol5 = Column(BigInteger)
    ask_price6 = Column(Numeric(20, 4));  ask_vol6 = Column(BigInteger)
    ask_price7 = Column(Numeric(20, 4));  ask_vol7 = Column(BigInteger)
    ask_price8 = Column(Numeric(20, 4));  ask_vol8 = Column(BigInteger)
    ask_price9 = Column(Numeric(20, 4));  ask_vol9 = Column(BigInteger)
    ask_price10 = Column(Numeric(20, 4)); ask_vol10 = Column(BigInteger)
    bid_price1 = Column(Numeric(20, 4));  bid_vol1 = Column(BigInteger)
    bid_price2 = Column(Numeric(20, 4));  bid_vol2 = Column(BigInteger)
    bid_price3 = Column(Numeric(20, 4));  bid_vol3 = Column(BigInteger)
    bid_price4 = Column(Numeric(20, 4));  bid_vol4 = Column(BigInteger)
    bid_price5 = Column(Numeric(20, 4));  bid_vol5 = Column(BigInteger)
    bid_price6 = Column(Numeric(20, 4));  bid_vol6 = Column(BigInteger)
    bid_price7 = Column(Numeric(20, 4));  bid_vol7 = Column(BigInteger)
    bid_price8 = Column(Numeric(20, 4));  bid_vol8 = Column(BigInteger)
    bid_price9 = Column(Numeric(20, 4));  bid_vol9 = Column(BigInteger)
    bid_price10 = Column(Numeric(20, 4)); bid_vol10 = Column(BigInteger)


class StreamingIndexData(Base):
    """Maps data.index_data — live market index data from Kafka consumer."""
    __tablename__ = "index_data"
    __table_args__ = {"schema": "data"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_id = Column(String(50))
    index_value = Column(Numeric(20, 4))
    prior_index_value = Column(Numeric(20, 4))
    trading_date = Column(Date)
    time = Column(String(20))
    total_trade = Column(BigInteger)
    total_qtty = Column(BigInteger)
    total_value = Column(Numeric(20, 4))
    index_name = Column(String(100))
    advances = Column(Integer)
    nochanges = Column(Integer)
    declines = Column(Integer)
    ceilings = Column(Integer)
    floors = Column(Integer)
    change = Column("change", Numeric(20, 4))
    ratio_change = Column(Numeric(20, 4))
    total_qtty_pt = Column(BigInteger)
    total_value_pt = Column(Numeric(20, 4))
    exchange = Column(String(50))
    all_qtty = Column(BigInteger)
    all_value = Column(Numeric(20, 4))
    index_type = Column(String(50))
    trading_session = Column(String(50))
    market_id = Column(String(50))
    rtype = Column(String(50))
    total_qtty_od = Column(BigInteger)
    total_value_od = Column(Numeric(20, 4))


class StreamingForeignRoom(Base):
    """Maps data.foreign_room — foreign room data (nn_mua, nn_ban, room)."""
    __tablename__ = "foreign_room"
    __table_args__ = {"schema": "data"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    rtype = Column(String(50))
    trading_date = Column(Date)
    time = Column(String(20))
    isin = Column(String(20))
    symbol = Column(String(20))
    total_room = Column(BigInteger)
    current_room = Column(BigInteger)
    buy_vol = Column(BigInteger)
    sell_vol = Column(BigInteger)
    buy_val = Column(Numeric(20, 4))
    sell_val = Column(Numeric(20, 4))
    market_id = Column(String(50))
    exchange = Column(String(50))


# ── data.candlestick ────────────────────────────────────────────────────────
# Pre-computed OHLC candles written by CandlestickConsumer (no Flink).
# 1m table is the source of truth; larger timeframes derived at query time.

class Candlestick1M(Base):
    """Maps data.candlestick_1m — 1-minute OHLCV candles."""
    __tablename__ = "candlestick_1m"
    __table_args__ = {"schema": "data"}

    symbol = Column(String(20), primary_key=True)
    time_start = Column(DateTime, primary_key=True)
    trading_date = Column(Date, nullable=False, default="2000-01-01")
    time = Column(String(20), nullable=False, default="00:00:00")
    open = Column(Numeric(20, 4))
    high = Column(Numeric(20, 4))
    low = Column(Numeric(20, 4))
    close = Column(Numeric(20, 4))
    volume = Column(BigInteger)


class TradeMatchArchive(Base):
    """Maps data.trade_match_archive — one row per matched trade."""
    __tablename__ = "trade_match_archive"
    __table_args__ = {"schema": "data"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    trading_date = Column(Date, nullable=False)
    time = Column(String(20), nullable=False)
    symbol = Column(String(20), nullable=False)
    price = Column(Numeric(20, 4), nullable=False)
    volume = Column(BigInteger, nullable=False, default=0)
    side = Column(String(10), nullable=False)
    price_change = Column(Numeric(20, 4))
    created_at = Column(DateTime)


class Candlestick1D(Base):
    """Maps data.candlestick_1d — daily OHLCV candles (includes foreign room data)."""
    __tablename__ = "candlestick_1d"
    __table_args__ = {"schema": "data"}

    symbol = Column(String(20), primary_key=True)
    trading_date = Column(Date, primary_key=True)
    open = Column(Numeric(20, 4))
    high = Column(Numeric(20, 4))
    low = Column(Numeric(20, 4))
    close = Column(Numeric(20, 4))
    volume = Column(BigInteger)
    nn_mua = Column(BigInteger)
    nn_ban = Column(BigInteger)
    room = Column(BigInteger)


# ── warehouse.dim ────────────────────────────────────────────────────────────

class DateDim(Base):
    __tablename__ = "date"
    __table_args__ = {"schema": "warehouse"}

    tradingdate_key = Column(Integer, primary_key=True)
    tradingdate = Column(Date)
    Year = Column(Integer)
    Quarter = Column(Integer)
    Month = Column(Integer)
    Day = Column(Integer)
    Weekday = Column(Integer)


class TimeDim(Base):
    __tablename__ = "time"
    __table_args__ = {"schema": "warehouse"}

    time_key = Column(Integer, primary_key=True)
    time_hh_mm_ss = Column(Time)
    Hour = Column(Integer)
    Minute = Column(Integer)
    Second = Column(Integer)


class SymbolDim(Base):
    __tablename__ = "symbol"
    __table_args__ = {"schema": "warehouse"}

    symbol_key = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(20))
    symbol_name = Column(String(255))
    symbol_en_name = Column(String(255))
    sector = Column(String(255))
    sector_id = Column(String(50), ForeignKey("data.sector.sector_id"))
    exchange_key = Column(Integer, ForeignKey("data.exchange.exchange_key"))


class MarketIndexDim(Base):
    __tablename__ = "market_index"
    __table_args__ = {"schema": "warehouse"}

    index_key = Column(Integer, primary_key=True, autoincrement=True)
    index_name = Column(String(100))
    exchange_key = Column(Integer, ForeignKey("data.exchange.exchange_key"))


class ExchangeDim(Base):
    __tablename__ = "exchange"
    __table_args__ = {"schema": "warehouse"}

    exchange_key = Column(Integer, primary_key=True, autoincrement=True)
    exchange_name = Column(String(100))


class TradingSessionDim(Base):
    __tablename__ = "tradingsession"
    __table_args__ = {"schema": "warehouse"}

    trading_session_key = Column(Integer, primary_key=True, autoincrement=True)
    trading_session = Column(String(50))


# ── warehouse.fact ───────────────────────────────────────────────────────────

class StockTradeFact(Base):
    __tablename__ = "stocktrade"
    __table_args__ = {"schema": "warehouse"}

    tradingdate_key = Column(Integer, primary_key=True)
    time_key = Column(Integer, primary_key=True)
    symbol_key = Column(Integer, primary_key=True)
    exchange_key = Column(Integer, primary_key=True)
    trading_session_key = Column(Integer, primary_key=True)
    last_price = Column(Numeric(20, 4))
    avg_price = Column(Numeric(20, 4))
    ceiling = Column(Numeric(20, 4))
    floor = Column("floor", Numeric(20, 4))
    ref_price = Column(Numeric(20, 4))
    prio_val = Column(Numeric(20, 4))
    last_vol = Column(BigInteger)
    total_val = Column(Numeric(20, 4))
    total_vol = Column(BigInteger)
    change = Column("change", Numeric(20, 4))
    ratio_change = Column(Numeric(20, 4))
    highest = Column(Numeric(20, 4))
    lowest = Column(Numeric(20, 4))


class StockOrderBookFact(Base):
    __tablename__ = "stockorderbook"
    __table_args__ = {"schema": "warehouse"}

    tradingdate_key = Column(Integer, primary_key=True)
    time_key = Column(Integer, primary_key=True)
    symbol_key = Column(Integer, primary_key=True)
    exchange_key = Column(Integer, primary_key=True)
    trading_session_key = Column(Integer, primary_key=True)
    ask_price1 = Column(Numeric(20, 4));  ask_vol1 = Column(BigInteger)
    ask_price2 = Column(Numeric(20, 4));  ask_vol2 = Column(BigInteger)
    ask_price3 = Column(Numeric(20, 4));  ask_vol3 = Column(BigInteger)
    bid_price1 = Column(Numeric(20, 4));  bid_vol1 = Column(BigInteger)
    bid_price2 = Column(Numeric(20, 4));  bid_vol2 = Column(BigInteger)
    bid_price3 = Column(Numeric(20, 4));  bid_vol3 = Column(BigInteger)


class MarketIndexFact(Base):
    __tablename__ = "marketindex"
    __table_args__ = {"schema": "warehouse"}

    tradingdate_key = Column(Integer, primary_key=True)
    time_key = Column(Integer, primary_key=True)
    index_key = Column(Integer, primary_key=True)
    exchange_key = Column(Integer, primary_key=True)
    trading_session_key = Column(Integer, primary_key=True)
    index_value = Column(Numeric(20, 4))
    prio_index_value = Column(Numeric(20, 4))
    change = Column("change", Numeric(20, 4))
    ratio_change = Column(Numeric(20, 4))
    total_qtty = Column(BigInteger)
    total_value = Column(Numeric(20, 4))
    total_qtty_pt = Column(BigInteger)
    total_value_pt = Column(Numeric(20, 4))
    advances = Column(Integer)
    nochanges = Column(Integer)
    declines = Column(Integer)
    ceilings = Column(Integer)
    floors = Column(Integer)


# ── api.user / api.watchlist ─────────────────────────────────────────────────
# Kept (not dropped) because auth endpoints still use them.
# These tables are created at runtime by main.py lifespan, not via init.sql.

class User(Base):
    __tablename__ = "user"
    __table_args__ = {"schema": "api"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    username = Column(String(100), nullable=False)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)


class Watchlist(Base):
    __tablename__ = "watchlist"
    __table_args__ = {"schema": "api"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    user_id = Column(BigInteger, ForeignKey("api.user.id", ondelete="CASCADE"), nullable=False)
    symbol = Column(String(20), nullable=False)
    position = Column(Integer, default=0)
    added_at = Column(DateTime, server_default=func.now())
