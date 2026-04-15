import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  createChart,
  type IChartApi,
  type HistogramData,
  ColorType,
} from "lightweight-charts";
import { stockApi, type OrderBook } from "../api/stockApi";
import { useStockWebSocket } from "../hooks/useStockWebSocket";
import { formatPrice } from "../lib/utils";
import styles from "./MarketDepthChart.module.css";

interface MarketDepthChartProps {
  /** Symbol to show depth for (defaults to VND) */
  symbol?: string;
}

function fmtVol(v: number): string {
  if (v >= 1_000_000) return (v / 1_000_000).toFixed(2) + "M";
  if (v >= 1_000) return (v / 1_000).toFixed(1) + "K";
  return v.toString();
}


export function MarketDepthChart({ symbol = "VND" }: MarketDepthChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const bidSeriesRef = useRef<ReturnType<IChartApi["addHistogramSeries"]> | null>(null);
  const askSeriesRef = useRef<ReturnType<IChartApi["addHistogramSeries"]> | null>(null);

  const [book, setBook] = useState<OrderBook | null>(null);

  const { data: restBook } = useQuery({
    queryKey: ["orderbook", symbol],
    queryFn: () => stockApi.getOrderBook(symbol).then((r) => r.data),
    staleTime: 10_000,
  });

  useStockWebSocket({
    symbol,
    onMessage: (msg) => {
      if (msg.type === "orderbook_update" && (msg as { symbol?: string }).symbol === symbol) {
        const m = msg as { symbol: string; bids: OrderBook["bids"]; asks: OrderBook["asks"]; time: string };
        setBook({ symbol: m.symbol, bids: m.bids, asks: m.asks, time: m.time });
      }
    },
  });

  useEffect(() => { if (restBook) setBook(restBook); }, [restBook]);

  // ── Mount chart ──────────────────────────────────────────────────────────
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      width: el.clientWidth,
      height: el.clientHeight,
      layout: {
        background: { type: ColorType.Solid, color: "#ffffff" },
        textColor: "#64748b",
        fontSize: 10,
        fontFamily: "Inter, -apple-system, sans-serif",
      },
      grid: {
        vertLines: { color: "#f1f5f9" },
        horzLines: { color: "#f1f5f9" },
      },
      rightPriceScale: { borderColor: "#e2e8f0" },
      timeScale: { visible: false },
      crosshair: { mode: 0 },
    });

    const bidSeries = chart.addHistogramSeries({
      color: "rgba(5, 150, 105, 0.6)",
      priceFormat: { type: "price", precision: 2, minMove: 0.01 },
    });

    const askSeries = chart.addHistogramSeries({
      color: "rgba(220, 38, 38, 0.6)",
      priceFormat: { type: "price", precision: 2, minMove: 0.01 },
    });

    chartRef.current = chart;
    bidSeriesRef.current = bidSeries;
    askSeriesRef.current = askSeries;

    const ro = new ResizeObserver(() => {
      if (chartRef.current && el) {
        chartRef.current.applyOptions({ width: el.clientWidth, height: el.clientHeight });
      }
    });
    ro.observe(el);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Update data ──────────────────────────────────────────────────────────
  useEffect(() => {
    if (!book || !bidSeriesRef.current || !askSeriesRef.current) return;
    const { bids, asks } = book;

    // Merge bid and ask levels for display
    // Use price as time-slot key (numeric) for histogram
    const bidData: HistogramData[] = bids.map((b, i) => ({
      time: (i + 1) as unknown as import("lightweight-charts").Time,
      value: b.volume,
      color: "rgba(5, 150, 105, 0.7)",
    }));
    const askData: HistogramData[] = asks.map((a, i) => ({
      time: (bids.length + 1 + i) as unknown as import("lightweight-charts").Time,
      value: a.volume,
      color: "rgba(220, 38, 38, 0.7)",
    }));

    bidSeriesRef.current.setData(bidData);
    askSeriesRef.current.setData(askData);
  }, [book]);

  // Compute spread
  const spread = book && book.bids[0] && book.asks[0]
    ? book.asks[0].price - book.bids[0].price
    : null;

  return (
    <div className={styles.wrap}>
      <div className={styles.header}>
        <span className={styles.title}>📊 Market Depth</span>
        {spread !== null && (
          <span className={styles.spread}>
            Spread: <strong>{formatPrice(spread)}</strong>
          </span>
        )}
      </div>
      <div className={styles.chartArea}>
        <div ref={containerRef} className={styles.chart} />
        {/* Bid/Ask summary bars */}
        {book && (
          <div className={styles.depthBars}>
            <div className={styles.depthRow}>
              <span className={styles.depthLabel} style={{ color: "var(--up)" }}>Bid</span>
              {book.bids.slice(0, 5).map((b, i) => (
                <div key={i} className={styles.depthCell}>
                  <span className={styles.depthPrice} style={{ color: "var(--up)" }}>{formatPrice(b.price)}</span>
                  <span className={styles.depthVol}>{fmtVol(b.volume)}</span>
                </div>
              ))}
            </div>
            <div className={styles.depthRow}>
              <span className={styles.depthLabel} style={{ color: "var(--down)" }}>Ask</span>
              {book.asks.slice(0, 5).map((a, i) => (
                <div key={i} className={styles.depthCell}>
                  <span className={styles.depthPrice} style={{ color: "var(--down)" }}>{formatPrice(a.price)}</span>
                  <span className={styles.depthVol}>{fmtVol(a.volume)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
