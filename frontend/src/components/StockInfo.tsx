import { useQuery } from "@tanstack/react-query";
import { stockApi } from "../api/stockApi";
import { useAppStore } from "../stores/appStore";
import { useStockQuote } from "../hooks/useStockPrice";
import { Card } from "./ui/Card";
import { Badge } from "./ui/Card";
import { formatPrice, pctColor, priceColor, formatVolume } from "../lib/utils";

export function StockInfo() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const { quote } = useStockQuote(selectedSymbol);

  const { data: meta } = useQuery({
    queryKey: ["symbol", selectedSymbol],
    queryFn: () => stockApi.getSymbol(selectedSymbol).then((r) => r.data),
    enabled: !!selectedSymbol,
    staleTime: 60_000,
  });

  return (
    <Card className="text-sm">
      <div className="mb-2 flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-white">{selectedSymbol}</h2>
          <p className="text-xs text-gray-400">{meta?.symbol_name ?? "—"}</p>
        </div>
        <Badge variant={quote && quote.change > 0 ? "green" : quote && quote.change < 0 ? "red" : "default"}>
          {quote ? pctColor(quote.ratio_change) : "—"}
        </Badge>
      </div>

      {quote && (
        <div className="grid grid-cols-2 gap-x-4 gap-y-1">
          <Row label="Last Price" value={formatPrice(quote.last_price)} valueClass={priceColor(quote.change)} />
          <Row label="Change" value={`${quote.change >= 0 ? "+" : ""}${quote.change.toFixed(2)}`} valueClass={priceColor(quote.change)} />
          <Row label="Ref Price" value={formatPrice(quote.ref_price)} />
          <Row label="Ceiling" value={formatPrice(quote.ceiling)} valueClass="text-red-400" />
          <Row label="Floor" value={formatPrice(quote.floor)} valueClass="text-green-400" />
          <Row label="Volume" value={formatVolume(quote.volume)} />
          <Row label="Value" value={formatVolume(quote.value)} />
          <Row label="High" value={formatPrice(quote.highest)} valueClass="text-green-400" />
          <Row label="Low" value={formatPrice(quote.lowest)} valueClass="text-red-400" />
        </div>
      )}

      {!quote && (
        <p className="text-xs text-gray-500">No quote data available</p>
      )}
    </Card>
  );
}

function Row({ label, value, valueClass = "text-white" }: { label: string; value: string; valueClass?: string }) {
  return (
    <div className="flex justify-between">
      <span className="text-gray-400">{label}</span>
      <span className={`font-mono font-medium ${valueClass}`}>{value}</span>
    </div>
  );
}
