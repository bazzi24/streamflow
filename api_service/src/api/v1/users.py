from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from ..database import get_api_db
from ..deps import get_current_user
from ...db.user_repo import UserRepository
from ...models import User
from ...schemas.auth import UserResponse
from pydantic import BaseModel

router = APIRouter(prefix="/users", tags=["users"])


class WatchlistRequest(BaseModel):
    symbols: list[str]


class WatchlistResponse(BaseModel):
    symbols: list[str]


@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_user)):
    return UserResponse.model_validate(current_user)


@router.put("/me/watchlist", response_model=WatchlistResponse)
def update_watchlist(
    req: WatchlistRequest,
    current_user: User = Depends(get_current_user),
    api_db: Session = Depends(get_api_db),
):
    repo = UserRepository(api_db)
    repo.update_watchlist(current_user.id, req.symbols)
    return WatchlistResponse(symbols=req.symbols)


@router.get("/me/watchlist", response_model=WatchlistResponse)
def get_watchlist(
    current_user: User = Depends(get_current_user),
    api_db: Session = Depends(get_api_db),
):
    repo = UserRepository(api_db)
    items = repo.get_watchlist(current_user.id)
    return WatchlistResponse(symbols=[w.symbol for w in items])
