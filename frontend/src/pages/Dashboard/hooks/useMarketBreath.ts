import { useMemo, useCallback } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { marketApi } from "../../../api/stockApi";
import { useStockWebSocket, type WsMessage } from "../../../hooks/useStockWebSocket";
import type { BreathIndex } from "../types/dashboard";

// ── Hook ────────────────────────────────────────────────────────────────────

export function useMarketBreath() {
  const queryClient = useQueryClient();

  const { data: overview, isLoading, isError } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    staleTime: 15_000,
  });

  // ── WebSocket: live index_update → update cached data in-place ────────────
  const handleMessage = useCallback(
    (msg: WsMessage) => {
      if (msg.type !== "index_update") return;
      const { index_id, index_value, advances, declines } = msg as Record<string, unknown>;

      queryClient.setQueryData(
        ["market-overview"],
        (old: { indices?: BreathIndex[] } | undefined) => {
          if (!old?.indices) return old;
          return {
            ...old,
            indices: old.indices.map((idx) =>
              idx.index_id === index_id
                ? {
                    ...idx,
                    index_value: index_value as number,
                    advances: (advances as number) ?? idx.advances,
                    declines: (declines as number) ?? idx.declines,
                    adRatio: (advances as number) / ((declines as number) || 1),
                    advancePct:
                      (advances as number) /
                      ((advances as number) + (declines as number) + (idx.nochanges ?? 0) || 1),
                    mcClellan:
                      ((advances as number) - (declines as number)) /
                      ((advances as number) + (declines as number) || 1) * 100,
                  } as BreathIndex
                : idx
            ),
          };
        }
      );
    },
    [queryClient]
  );

  useStockWebSocket({ market: true, onMessage: handleMessage });

  // ── Transform to BreathIndex[] ─────────────────────────────────────────────
  const indices = useMemo<BreathIndex[]>(() => {
    if (!overview?.indices) return [];
    return overview.indices.map((idx) => {
      const total = idx.advances + idx.declines + (idx.nochanges ?? 0) || 1;
      const adRatio = idx.advances / (idx.declines || 1);
      const advancePct = idx.advances / total;
      const mcClellan = ((idx.advances - idx.declines) / (idx.advances + idx.declines || 1)) * 100;

      return {
        index_id: idx.index_id,
        index_name: idx.index_name,
        index_value: idx.index_value,
        change: idx.change,
        ratio_change: idx.ratio_change,
        advances: idx.advances,
        declines: idx.declines,
        nochanges: idx.nochanges ?? 0,
        adRatio,
        advancePct,
        mcClellan,
      };
    });
  }, [overview]);

  return { indices, isLoading, isError };
}
