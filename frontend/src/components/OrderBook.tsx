import { useQuery } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import { stockApi, type OrderBook } from "../api/stockApi";
import { useAppStore } from "../stores/appStore";
import { useStockWebSocket } from "../hooks/useStockWebSocket";
import { formatPrice, formatVolume } from "../lib/utils";

export function OrderBook() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const token = useAppStore((s) => s.token);
  const [book, setBook] = useState<OrderBook | null>(null);

  const { data: restBook } = useQuery({
    queryKey: ["orderbook", selectedSymbol],
    queryFn: () => stockApi.getOrderBook(selectedSymbol).then((r) => r.data),
    enabled: !!selectedSymbol,
    staleTime: 10_000,
  });

  useStockWebSocket({
    symbol: selectedSymbol,
    token,
    onMessage: (msg) => {
      if (msg.type === "orderbook_update" && (msg as any).symbol === selectedSymbol) {
        const m = msg as any;
        setBook({ symbol: m.symbol, bids: m.bids, asks: m.asks, time: m.time });
      }
    },
  });

  useEffect(() => {
    if (restBook) setBook(restBook);
  }, [restBook]);

  const bids = book?.bids ?? [];
  const asks = book?.asks ?? [];

  return (
    <div className="overflow-hidden rounded border border-gray-700">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-gray-700 text-gray-400">
            <th className="py-1 pl-2 text-left">Bid Vol</th>
            <th className="py-1 text-right">Bid</th>
            <th className="py-1 text-right">Ask</th>
            <th className="py-1 pr-2 text-right">Ask Vol</th>
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: Math.max(bids.length, asks.length, 5) }).map((_, i) => {
            const bid = bids[i];
            const ask = asks[i];
            return (
              <tr key={i} className="border-b border-gray-800/50">
                <td className="py-0.5 pl-2 text-right text-green-400">
                  {bid ? formatVolume(bid.volume) : ""}
                </td>
                <td className="py-0.5 text-right font-mono text-green-400">
                  {bid ? formatPrice(bid.price) : ""}
                </td>
                <td className="py-0.5 text-right font-mono text-red-400">
                  {ask ? formatPrice(ask.price) : ""}
                </td>
                <td className="py-0.5 pr-2 text-right text-red-400">
                  {ask ? formatVolume(ask.volume) : ""}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
