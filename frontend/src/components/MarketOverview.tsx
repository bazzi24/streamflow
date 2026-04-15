import { useQuery } from "@tanstack/react-query";
import { marketApi } from "../api/stockApi";
import { Card } from "./ui/Card";
import { formatPrice, pctColor, priceColor } from "../lib/utils";

export function MarketOverview() {
  const { data: overview } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    refetchInterval: 30_000,
  });

  return (
    <div className="flex flex-col gap-3">
      {/* Indices */}
      <Card>
        <p className="mb-2 text-xs font-semibold uppercase text-gray-400">Market Indices</p>
        <div className="space-y-1">
          {overview?.indices.map((idx) => (
            <div key={idx.index_id} className="flex items-center justify-between text-sm">
              <span className="text-gray-300">{idx.index_name}</span>
              <span className="font-mono font-medium">{formatPrice(idx.index_value)}</span>
              <span className={pctColor(idx.ratio_change)}>
                {idx.ratio_change >= 0 ? "+" : ""}
                {idx.ratio_change.toFixed(2)}%
              </span>
            </div>
          ))}
          {(!overview?.indices || overview.indices.length === 0) && (
            <p className="text-xs text-gray-500">No index data</p>
          )}
        </div>
      </Card>

      {/* Top Gainers */}
      <Card>
        <p className="mb-2 text-xs font-semibold uppercase text-green-400">Top Gainers</p>
        <div className="space-y-1">
          {overview?.top_gainers.map((s) => (
            <div key={s.symbol} className="flex items-center justify-between text-sm">
              <span className="font-medium text-gray-200">{s.symbol}</span>
              <span className={priceColor(s.change)}>{formatPrice(s.last_price)}</span>
              <span className={pctColor(s.ratio_change)}>+{s.ratio_change.toFixed(2)}%</span>
            </div>
          ))}
          {(!overview?.top_gainers || overview.top_gainers.length === 0) && (
            <p className="text-xs text-gray-500">No data</p>
          )}
        </div>
      </Card>

      {/* Top Losers */}
      <Card>
        <p className="mb-2 text-xs font-semibold uppercase text-red-400">Top Losers</p>
        <div className="space-y-1">
          {overview?.top_losers.map((s) => (
            <div key={s.symbol} className="flex items-center justify-between text-sm">
              <span className="font-medium text-gray-200">{s.symbol}</span>
              <span className={priceColor(s.change)}>{formatPrice(s.last_price)}</span>
              <span className={pctColor(s.ratio_change)}>{s.ratio_change.toFixed(2)}%</span>
            </div>
          ))}
          {(!overview?.top_losers || overview.top_losers.length === 0) && (
            <p className="text-xs text-gray-500">No data</p>
          )}
        </div>
      </Card>
    </div>
  );
}
