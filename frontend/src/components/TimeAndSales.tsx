import { useState } from "react";
import { useStockWebSocket, type WsMessage } from "../hooks/useStockWebSocket";
import { useAppStore } from "../stores/appStore";
import { formatPrice, priceColor } from "../lib/utils";

interface TradeTick {
  time: string;
  price: number;
  volume: number;
  change: number;
}

export function TimeAndSales() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const token = useAppStore((s) => s.token);
  const [ticks, setTicks] = useState<TradeTick[]>([]);

  useStockWebSocket({
    symbol: selectedSymbol,
    token,
    onMessage: (msg: WsMessage) => {
      if (msg.type === "price_update" && (msg as any).symbol === selectedSymbol) {
        const m = msg as any;
        setTicks((prev) => [
          {
            time: m.time,
            price: m.last_price,
            volume: m.volume,
            change: m.change,
          },
          ...prev.slice(0, 49), // keep last 50
        ]);
      }
    },
  });

  return (
    <div className="overflow-hidden rounded border border-gray-700">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-gray-700 text-gray-400">
            <th className="py-1 pl-2 text-left">Time</th>
            <th className="py-1 text-right">Price</th>
            <th className="py-1 pr-2 text-right">Volume</th>
          </tr>
        </thead>
        <tbody>
          {ticks.map((tick, i) => (
            <tr key={i} className="border-b border-gray-800/50">
              <td className="py-0.5 pl-2 text-gray-400">{tick.time}</td>
              <td className={`py-0.5 text-right font-mono ${priceColor(tick.change)}`}>
                {formatPrice(tick.price)}
              </td>
              <td className="py-0.5 pr-2 text-right text-gray-400">
                {tick.volume.toLocaleString()}
              </td>
            </tr>
          ))}
          {ticks.length === 0 && (
            <tr>
              <td colSpan={3} className="py-4 text-center text-gray-500">
                Waiting for trades...
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
