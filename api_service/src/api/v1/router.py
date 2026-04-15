from fastapi import APIRouter
from . import auth, stocks, market, users, websocket

api_router = APIRouter(prefix="/api/v1")

api_router.include_router(auth.router)
api_router.include_router(stocks.router)
api_router.include_router(market.router)
api_router.include_router(users.router)

# WebSocket routes are attached directly on the app in main.py
# (FastAPI mounts them via a different mechanism)
ws_router = websocket.router
