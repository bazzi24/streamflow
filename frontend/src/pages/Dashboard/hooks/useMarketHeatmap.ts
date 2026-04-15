import { useMemo, useCallback } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { stockApi } from "../../../api/stockApi";
import { useStockWebSocket, type WsMessage } from "../../../hooks/useStockWebSocket";
import type { HeatmapCell } from "../types/dashboard";
import { useAppStore } from "../../../stores/appStore";

// ── Color computation ────────────────────────────────────────────────────────

/** Map ratio_change (-7..+7) to an RGBA color string */
export function heatColor(ratioChange: number): string {
  const cap = 7; // color saturates at ±7%
  const clamped = Math.max(-cap, Math.min(cap, ratioChange));
  const intensity = Math.abs(clamped) / cap;
  const alpha = 0.15 + intensity * 0.7;

  if (ratioChange >= 0) {
    return `rgba(0, 230, 118, ${alpha})`;
  } else {
    return `rgba(255, 61, 87, ${alpha})`;
  }
}

/** Log-scale cell size (40..90px) */
export function heatCellSize(volume: number): number {
  const log = Math.log10(volume + 1);
  const min = 5;  // log10(10K)
  const max = 8;  // log10(100M)
  const t = Math.max(0, Math.min(1, (log - min) / (max - min)));
  return 44 + t * 44; // 44–88px
}

// ── Hook ────────────────────────────────────────────────────────────────────

export function useMarketHeatmap() {
  const heatmapExchange = useAppStore((s) => s.heatmapExchange);
  const queryClient = useQueryClient();

  const { data: stocks, isLoading, isError } = useQuery({
    queryKey: ["stocks", heatmapExchange],
    queryFn: () => stockApi.listStocks(heatmapExchange === "ALL" ? undefined : heatmapExchange).then((r) => r.data),
    staleTime: 30_000,
  });

  // ── WebSocket live updates ───────────────────────────────────────────────
  const handleMessage = useCallback(
    (msg: WsMessage) => {
      if (msg.type !== "price_update") return;
      const { symbol, last_price, change, ratio_change, volume } = msg;
      queryClient.setQueryData<HeatmapCell[]>(
        ["stocks", heatmapExchange],
        (old) => {
          if (!old) return old;
          return old.map((cell) =>
            cell.symbol === symbol
              ? {
                  ...cell,
                  last_price,
                  change,
                  ratio_change,
                  volume,
                  bgColor: heatColor(ratio_change),
                  intensity: Math.abs(ratio_change) / 7,
                }
              : cell
          );
        }
      );
    },
    [queryClient, heatmapExchange]
  );

  useStockWebSocket({ market: true, onMessage: handleMessage });

  // ── Transform to HeatmapCell[] ───────────────────────────────────────────
  const cells = useMemo<HeatmapCell[]>(() => {
    if (!stocks) return [];
    return stocks.map((s) => ({
      symbol: s.symbol,
      symbol_name: s.symbol_name,
      exchange: s.exchange,
      last_price: s.last_price,
      change: s.change,
      ratio_change: s.ratio_change,
      volume: s.volume,
      sector: null,
      bgColor: heatColor(s.ratio_change),
      intensity: Math.abs(s.ratio_change) / 7,
      cellSize: heatCellSize(s.volume),
    }));
  }, [stocks]);

  return { cells, isLoading, isError, totalCount: stocks?.length ?? 0 };
}
