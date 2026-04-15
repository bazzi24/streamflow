from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from ...database import get_streaming_db
from ...services.stock_service import StockService
from ...schemas.stock import (
    StockQuote, OrderBook, OHLCVBar, SymbolMeta, StockSummary,
    MarketOverviewResponse,
)
from typing import Annotated

router = APIRouter(prefix="/stocks", tags=["stocks"])


def get_stock_service(db: Session = Depends(get_streaming_db)) -> StockService:
    return StockService(db)


@router.get("", response_model=list[StockSummary])
def list_stocks(
    exchange: Annotated[str | None, Query(
        description="Filter by exchange: HOSE, HNX, UPCOM, VN30, HNX30"
    )] = None,
    segment: Annotated[str | None, Query(
        description="Filter by segment: WARRANT, ETF"
    )] = None,
    svc: StockService = Depends(get_stock_service),
):
    """List all symbols with latest prices. Optionally filter by exchange or segment.
    Warrants are excluded from exchange listings unless segment=WARRANT."""
    return svc.list_latest_quotes(exchange=exchange, segment=segment)


@router.get("/{symbol}", response_model=SymbolMeta)
def get_symbol(symbol: str, svc: StockService = Depends(get_stock_service)):
    """Symbol metadata."""
    meta = svc.get_symbol_meta(symbol)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")
    return meta


@router.get("/{symbol}/quote", response_model=StockQuote)
def get_quote(symbol: str, svc: StockService = Depends(get_stock_service)):
    """Current price (last trade tick)."""
    quote = svc.get_quote(symbol)
    if quote is None:
        raise HTTPException(status_code=404, detail=f"No quote for '{symbol}'")
    return quote


@router.get("/{symbol}/orderbook", response_model=OrderBook)
def get_orderbook(symbol: str, svc: StockService = Depends(get_stock_service)):
    """Top 3 bid/ask levels (latest data)."""
    return svc.get_orderbook(symbol)


@router.get("/{symbol}/ohlcv", response_model=list[OHLCVBar])
def get_ohlcv(
    symbol: str,
    interval: Annotated[str, Query(description="e.g. 1m, 5m, 1h, 1d")] = "5m",
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    svc: StockService = Depends(get_stock_service),
):
    """OHLCV bars for a given interval."""
    return svc.get_ohlcv(symbol, interval=interval, limit=limit)


@router.get("/{symbol}/history", response_model=list[OHLCVBar])
def get_history(
    symbol: str,
    days: Annotated[int, Query(ge=1, le=365)] = 30,
    svc: StockService = Depends(get_stock_service),
):
    """Daily OHLCV bars for N days."""
    return svc.get_history(symbol, days=days)
