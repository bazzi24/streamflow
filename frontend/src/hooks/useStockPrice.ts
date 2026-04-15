import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { stockApi, type StockQuote } from "../api/stockApi";
import { useStockWebSocket, type WsMessage } from "./useStockWebSocket";
import { useAppStore } from "../stores/appStore";

export function useStockQuote(symbol: string) {
  const [quote, setQuote] = useState<StockQuote | null>(null);
  const token = useAppStore((s) => s.token);

  // Fetch latest quote from REST API
  const { data: restQuote, refetch } = useQuery({
    queryKey: ["quote", symbol],
    queryFn: () => stockApi.getQuote(symbol).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 30_000,
  });

  // Real-time update via WebSocket
  useStockWebSocket({
    symbol,
    token,
    onMessage: (msg: WsMessage) => {
      if (msg.type === "price_update" && msg.symbol === symbol) {
        setQuote(msg as StockQuote);
      }
    },
  });

  // Seed state from REST on mount / refetch
  useEffect(() => {
    if (restQuote) setQuote(restQuote);
  }, [restQuote]);

  return { quote, refetch };
}
