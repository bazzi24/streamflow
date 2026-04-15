from pydantic import BaseModel
from typing import Optional


# ── Shared ───────────────────────────────────────────────────────────────

class OrderBookLevel(BaseModel):
    price: float
    volume: int


class OrderBook(BaseModel):
    symbol: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    time: str


class StockQuote(BaseModel):
    symbol: str
    last_price: float
    change: float
    ratio_change: float
    volume: int
    value: float
    highest: float
    lowest: float
    ref_price: float
    ceiling: float
    floor: float
    time: str


class OHLCVBar(BaseModel):
    timestamp: int  # Unix ms
    open: float
    high: float
    low: float
    close: float
    volume: float


class SymbolMeta(BaseModel):
    symbol: str
    symbol_name: str
    sector: Optional[str] = None


class BidAskLevel(BaseModel):
    bid_price: float = 0
    bid_vol: int = 0
    ask_price: float = 0
    ask_vol: int = 0


class StockSummary(BaseModel):
    symbol: str
    symbol_name: str
    exchange: str = ""
    last_price: float
    change: float
    ratio_change: float
    volume: int
    last_vol: int = 0
    total_vol: int = 0
    value: float
    ceiling: float = 0
    floor: float = 0
    ref_price: float = 0
    best_bid_price: float = 0
    best_bid_vol: int = 0
    best_ask_price: float = 0
    best_ask_vol: int = 0
    bid_ask_levels: list[BidAskLevel] = []   # buy side: [best, 2nd, 3rd]
    ask_levels: list[BidAskLevel] = []        # sell side: [best, 2nd, 3rd]
    matched_price: float = 0
    time: str = ""
    highest: float = 0
    lowest: float = 0
    nn_mua: float = 0
    nn_ban: float = 0
    room: float = 0
    is_warrant: bool = False
    is_etf: bool = False


# ── WebSocket message types ───────────────────────────────────────────────

class WsTradeUpdate(BaseModel):
    type: str = "price_update"
    symbol: str
    last_price: float
    change: float
    ratio_change: float
    volume: int
    value: float
    time: str


class WsQuoteUpdate(BaseModel):
    type: str = "orderbook_update"
    symbol: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    time: str


class WsIndexUpdate(BaseModel):
    type: str = "index_update"
    index_id: str
    index_value: float
    change: float
    ratio_change: float
    advances: int
    declines: int
    time: str


class WsCandleUpdate(BaseModel):
    type: str = "candlestick_update"
    symbol: str
    interval: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


# ── Market Overview ───────────────────────────────────────────────────────

class IndexOverview(BaseModel):
    index_id: str
    index_name: str
    index_value: float
    change: float
    ratio_change: float
    advances: int
    declines: int
    nochanges: int = 0
    total_qtty: int = 0   # total traded quantity (volume)
    total_value: float = 0.0
    time: str


class MarketOverviewResponse(BaseModel):
    indices: list[IndexOverview]
    top_gainers: list[StockSummary]
    top_losers: list[StockSummary]

