import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from ...websocket.manager import ws_manager
from ...core.security import decode_access_token
from ...config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["websocket"])


@router.websocket("/ws/stocks/{symbol}")
async def ws_stocks(
    websocket: WebSocket,
    symbol: str,
    token: str = Query(None),
):
    """
    WebSocket endpoint for live stock updates.
    Optionally authenticate via ?token=<jwt>.
    """
    # Optional auth
    if token:
        payload = decode_access_token(token, settings.secret_key)
        if payload is None:
            await websocket.close(code=4001, reason="Invalid token")
            return

    await ws_manager.connect(websocket, symbol)
    logger.info("WS connected: symbol=%s", symbol)
    try:
        while True:
            # Keep connection alive — receive any control messages
            data = await websocket.receive_text()
            # Allow clients to subscribe/unsubscribe dynamically
            try:
                msg = json.loads(data)
                action = msg.get("action", "")
                sym = msg.get("symbol", "")
                if action == "subscribe" and sym:
                    await ws_manager.subscribe(websocket, sym)
                elif action == "unsubscribe" and sym:
                    await ws_manager.unsubscribe(websocket, sym)
            except json.JSONDecodeError:
                pass  # ignore non-JSON control messages
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
        logger.info("WS disconnected: symbol=%s", symbol)


@router.websocket("/ws/market")
async def ws_market(websocket: WebSocket, token: str = Query(None)):
    """
    WebSocket endpoint for all-market real-time updates (index, trades, etc).
    """
    if token:
        payload = decode_access_token(token, settings.secret_key)
        if payload is None:
            await websocket.close(code=4001, reason="Invalid token")
            return

    await ws_manager.connect(websocket, symbol=None)
    logger.info("WS market connection opened")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
        logger.info("WS market connection closed")
