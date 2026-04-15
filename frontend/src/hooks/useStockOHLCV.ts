import { useQuery } from "@tanstack/react-query";
import { stockApi } from "../api/stockApi";

export function useStockOHLCV(symbol: string, interval = "5m", days = 30) {
  const { data: intraday, isLoading: intradayLoading } = useQuery({
    queryKey: ["ohlcv", symbol, interval],
    queryFn: () => stockApi.getOHLCV(symbol, interval).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 60_000,
  });

  const { data: history, isLoading: historyLoading } = useQuery({
    queryKey: ["history", symbol, days],
    queryFn: () => stockApi.getHistory(symbol, days).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });

  return {
    intraday: intraday ?? [],
    history: history ?? [],
    intradayLoading,
    historyLoading,
  };
}
