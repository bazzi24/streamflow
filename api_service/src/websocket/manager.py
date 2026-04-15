import asyncio
import json
import logging
from collections import defaultdict
from typing import Any
from fastapi import WebSocket
import threading

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Thread-safe WebSocket connection manager.
    Manages per-symbol rooms so clients only receive messages for symbols they subscribed to.
    """

    def __init__(self):
        # symbol -> set of websocket connections
        self._symbol_rooms: dict[str, set[WebSocket]] = defaultdict(set)
        # ws -> set of symbols subscribed
        self._ws_symbols: dict[WebSocket, set[str]] = defaultdict(set)
        # all active ws
        self._all_connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket, symbol: str | None = None) -> None:
        await ws.accept()
        async with self._lock:
            self._all_connections.add(ws)
            if symbol:
                self._symbol_rooms[symbol.upper()].add(ws)
                self._ws_symbols[ws].add(symbol.upper())

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._all_connections.discard(ws)
            for sym in self._ws_symbols.pop(ws, []):
                self._symbol_rooms[sym].discard(ws)
                if not self._symbol_rooms[sym]:
                    del self._symbol_rooms[sym]

    async def subscribe(self, ws: WebSocket, symbol: str) -> None:
        sym = symbol.upper()
        async with self._lock:
            self._symbol_rooms[sym].add(ws)
            self._ws_symbols[ws].add(sym)

    async def unsubscribe(self, ws: WebSocket, symbol: str) -> None:
        sym = symbol.upper()
        async with self._lock:
            self._symbol_rooms[sym].discard(ws)
            self._ws_symbols[ws].discard(sym)
            if not self._symbol_rooms.get(sym):
                del self._symbol_rooms[sym]

    async def broadcast_to_symbol(self, symbol: str, message: dict[str, Any]) -> None:
        """Send a JSON message to all WS connected to a specific symbol room."""
        sym = symbol.upper()
        text = json.dumps(message)
        async with self._lock:
            targets = list(self._symbol_rooms.get(sym, set()))

        if not targets:
            return

        dead = []
        for ws in targets:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    await self._disconnect_unsafe(ws)

    async def broadcast_all(self, message: dict[str, Any]) -> None:
        """Broadcast to all connected clients (market-wide)."""
        text = json.dumps(message)
        async with self._lock:
            targets = list(self._all_connections)

        if not targets:
            return

        dead = []
        for ws in targets:
            try:
                await ws.send_text(text)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    await self._disconnect_unsafe(ws)

    async def _disconnect_unsafe(self, ws: WebSocket) -> None:
        """Disconnect without lock — caller must hold lock."""
        self._all_connections.discard(ws)
        for sym in list(self._ws_symbols.pop(ws, [])):
            self._symbol_rooms[sym].discard(ws)

    @property
    def connection_count(self) -> int:
        return len(self._all_connections)


# Singleton instance
ws_manager = ConnectionManager()
