import { useNavigate } from "react-router-dom";
import { useAppStore } from "../stores/appStore";
import { useQuery } from "@tanstack/react-query";
import { stockApi } from "../api/stockApi";
import { formatPrice, priceColor } from "../lib/utils";

export function Watchlist() {
  const navigate = useNavigate();
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const setSelectedSymbol = useAppStore((s) => s.setSelectedSymbol);

  const { data: stocks } = useQuery({
    queryKey: ["stocks"],
    queryFn: () => stockApi.listStocks().then((r) => r.data),
    refetchInterval: 30_000,
  });

  const symbols = stocks?.map((s) => s.symbol) ?? [];

  function handleClick(sym: string) {
    setSelectedSymbol(sym);
    navigate(`/chart/${sym}`);
  }

  return (
    <div className="flex flex-col gap-1">
      <p className="mb-1 text-xs font-semibold uppercase text-gray-400">Watchlist</p>
      {symbols.slice(0, 20).map((sym) => {
        const s = stocks?.find((x) => x.symbol === sym);
        return (
          <button
            key={sym}
            onClick={() => handleClick(sym)}
            className={`flex w-full items-center justify-between rounded px-2 py-1 text-left text-sm transition-colors ${
              selectedSymbol === sym
                ? "bg-blue-900/50 text-white"
                : "text-gray-300 hover:bg-gray-700"
            }`}
          >
            <span className="font-medium">{sym}</span>
            {s && (
              <span className={priceColor(s.change)}>{formatPrice(s.last_price)}</span>
            )}
          </button>
        );
      })}
      {symbols.length === 0 && (
        <p className="text-xs text-gray-500">No symbols loaded</p>
      )}
    </div>
  );
}
