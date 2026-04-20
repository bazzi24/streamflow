from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ...database import get_streaming_db, get_db
from ...services.stock_service import StockService
from ...schemas.stock import MarketOverviewResponse

router = APIRouter(prefix="/market", tags=["market"])


def get_stock_service(
    streaming_db: Session = Depends(get_streaming_db),
    warehouse_db: Session = Depends(get_db)
) -> StockService:
    return StockService(streaming_db, warehouse_db)


@router.get("/overview", response_model=MarketOverviewResponse)
async def get_market_overview(svc: StockService = Depends(get_stock_service)):
    """Index values + top gainers + top losers."""
    return await svc.get_market_overview()
