import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { stockApi } from "../../../api/stockApi";
import { useAppStore } from "../../../stores/appStore";
import { useStockWebSocket, type WsMessage } from "../../../hooks/useStockWebSocket";
import type { StockQuote } from "../../../api/stockApi";

/**
 * useSynchronizedView
 *
 * Central coordination hook that:
 * 1. Subscribes to per-symbol WebSocket updates (price_update)
 * 2. Merges live WS data into React Query cache for quote, orderbook, ohlcv
 * 3. Optionally prefetches all data for a new symbol
 *
 * All panels consume from React Query cache — this hook keeps the cache fresh.
 */
export function useSynchronizedView() {
  const queryClient = useQueryClient();
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const activeInterval = useAppStore((s) => s.activeInterval);
  const token = useAppStore((s) => s.token);
  const prevSymbolRef = useRef<string>(selectedSymbol);

  // ── Prefetch on symbol change ────────────────────────────────────────────
  useEffect(() => {
    if (!selectedSymbol || selectedSymbol === prevSymbolRef.current) return;
    prevSymbolRef.current = selectedSymbol;

    // Warm the cache before the panel re-renders
    queryClient.prefetchQuery({
      queryKey: ["quote", selectedSymbol],
      queryFn: () => stockApi.getQuote(selectedSymbol).then((r) => r.data),
    });

    queryClient.prefetchQuery({
      queryKey: ["orderbook", selectedSymbol],
      queryFn: () => stockApi.getOrderBook(selectedSymbol).then((r) => r.data),
    });

    queryClient.prefetchQuery({
      queryKey: ["ohlcv", selectedSymbol, activeInterval],
      queryFn: () => stockApi.getOHLCV(selectedSymbol, activeInterval).then((r) => r.data),
    });

    queryClient.prefetchQuery({
      queryKey: ["history", selectedSymbol, 30],
      queryFn: () => stockApi.getHistory(selectedSymbol, 30).then((r) => r.data),
    });
  }, [selectedSymbol, activeInterval, queryClient]);

  // ── Per-symbol WebSocket: merge live ticks into cache ─────────────────────
  const handleMessage = (msg: WsMessage) => {
    if (msg.type !== "price_update") return;
    if (msg.symbol !== selectedSymbol) return;

    const quote = msg as unknown as StockQuote;

    // Merge live price into quote cache
    queryClient.setQueryData(["quote", selectedSymbol], quote);

    // Merge into stocks list cache (for heatmap/watchlist)
    queryClient.setQueryData(
      ["stocks", "ALL"],
      (old: Array<StockQuote & { symbol: string }> | undefined) => {
        if (!old) return old;
        return old.map((s) => (s.symbol === selectedSymbol ? { ...s, ...quote } : s));
      }
    );
  };

  useStockWebSocket({
    symbol: selectedSymbol,
    token,
    onMessage: handleMessage,
  });
}
