import { useEffect, useRef, useCallback, useState } from "react";
import type { StockQuote, OrderBook } from "../api/stockApi";

const WS_BASE =
  import.meta.env.VITE_WS_URL || "ws://localhost:8000/api/v1";

export type WsMessage =
  | ({ type: "price_update" } & StockQuote)
  | ({ type: "orderbook_update" } & Omit<OrderBook, "type">)
  | { type: "index_update"; [key: string]: unknown }
  | { type: "candlestick_update"; [key: string]: unknown }
  | { type: "crypto_price_update"; symbol: string; price: number; quantity: number; trade_time: number; is_buyer_maker: boolean }
  | { type: "crypto_candlestick_update"; symbol: string; interval: string; timestamp: number; open: number; high: number; low: number; close: number; volume: number };

interface UseStockWebSocketOptions {
  symbol?: string;
  market?: boolean;  // subscribe to all-market updates
  token?: string | null;
  onMessage?: (msg: WsMessage) => void;
  onConnect?: () => void;
  onDisconnect?: () => void;
}

export function useStockWebSocket({
  symbol,
  market = false,
  token,
  onMessage,
  onConnect,
  onDisconnect,
}: UseStockWebSocketOptions) {
  const wsRef = useRef<WebSocket | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectAttempts = useRef(0);
  const onMessageRef = useRef(onMessage);
  onMessageRef.current = onMessage;

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    let url: string;
    if (market) {
      url = `${WS_BASE}/ws/market${token ? `?token=${token}` : ""}`;
    } else if (symbol) {
      url = `${WS_BASE}/ws/stocks/${symbol}${token ? `?token=${token}` : ""}`;
    } else {
      return;
    }

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setIsConnected(true);
      reconnectAttempts.current = 0; // reset on successful connect
      onConnect?.();
    };

    ws.onmessage = (event) => {
      try {
        const msg: WsMessage = JSON.parse(event.data);
        onMessageRef.current?.(msg);
      } catch {
        // ignore parse errors
      }
    };

    ws.onerror = () => {
      ws.close();
    };

    ws.onclose = () => {
      setIsConnected(false);
      onDisconnect?.();
      // Exponential backoff reconnection: 1s, 2s, 4s, 8s, max 30s
      if (!reconnectTimer.current) {
        const delay = Math.min(1000 * 2 ** reconnectAttempts.current, 30000);
        reconnectAttempts.current += 1;
        reconnectTimer.current = setTimeout(() => {
          reconnectTimer.current = null;
          connect();
        }, delay);
      }
    };
  }, [symbol, market, token, onConnect, onDisconnect]);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimer.current) {
        clearTimeout(reconnectTimer.current);
        reconnectTimer.current = null;
      }
      wsRef.current?.close();
    };
  }, [connect]);

  return { isConnected };
}
