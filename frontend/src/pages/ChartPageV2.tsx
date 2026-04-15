import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import {
  createChart,
  PriceScaleMode,
  ColorType,
  CrosshairMode,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type LineData,
  type Time,
} from "lightweight-charts";
import { stockApi } from "../api/stockApi";
import type { OHLCVBar, OrderBook, StockQuote } from "../api/stockApi";
import { useStockWebSocket } from "../hooks/useStockWebSocket";
import {
  computeSMA,
  computeEMA,
  computeBollinger,
  computeMACD,
  computeRSI,
} from "../hooks/useIndicatorCompute";
import { formatPrice, formatVolume } from "../lib/utils";
import styles from "./ChartPageV2.module.css";

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

interface TradeTick {
  time: string;
  price: number;
  volume: number;
  change: number;
  side: "buy" | "sell" | "neutral";
}

// ─────────────────────────────────────────────────────────────────────────────
// Static Data
// ─────────────────────────────────────────────────────────────────────────────

const WATCHLIST_ITEMS = [
  { symbol: "VCB", name: "Vietcombank" },
  { symbol: "MBB", name: "MB Bank" },
  { symbol: "ACB", name: "ACB Bank" },
  { symbol: "FPT", name: "FPT Corp" },
  { symbol: "HPG", name: "Hoa Phat" },
  { symbol: "VND", name: "VNDirect" },
  { symbol: "TCB", name: "Techcombank" },
  { symbol: "VPB", name: "VPBank" },
  { symbol: "SSI", name: "SSI Securities" },
  { symbol: "VNM", name: "Vinamilk" },
  { symbol: "HDB", name: "HDBank" },
  { symbol: "STB", name: "Sacombank" },
  { symbol: "CTG", name: "VietinBank" },
  { symbol: "BID", name: "BIDV" },
];

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function fmtChange(v: number, decimals = 2): string {
  return `${v >= 0 ? "+" : ""}${v.toFixed(decimals)}`;
}

function toChartTime(ts: number): Time {
  return (ts / 1000) as Time;
}

// ─────────────────────────────────────────────────────────────────────────────
// Indicator Config
// ─────────────────────────────────────────────────────────────────────────────

type IndType = "SMA" | "EMA" | "BB" | "MACD" | "RSI";

const INDICATOR_COLORS: Record<string, { color: string; period: number; type: IndType }> = {
  MA5:   { color: "#2563eb", period: 5,  type: "SMA"  },
  MA10:  { color: "#7c3aed", period: 10, type: "SMA"  },
  MA20:  { color: "#059669", period: 20, type: "SMA"  },
  MA50:  { color: "#d97706", period: 50, type: "SMA"  },
  EMA12: { color: "#0891b2", period: 12, type: "EMA"  },
  EMA26: { color: "#be185d", period: 26, type: "EMA"  },
  BB:    { color: "#f59e0b", period: 20, type: "BB"   },
  MACD:  { color: "#2962ff", period: 12, type: "MACD" },
  RSI:   { color: "#f59e0b", period: 14, type: "RSI"  },
};
const INDICATOR_KEYS = Object.keys(INDICATOR_COLORS);

const INTRADAY_INTERVALS = new Set(["1m", "5m", "15m", "30m", "1h", "4h"]);

// ─────────────────────────────────────────────────────────────────────────────
// MainChart — lightweight-charts with overlay panes + crosshair legend
// ─────────────────────────────────────────────────────────────────────────────

interface MainChartProps {
  symbol: string;
  interval: string;
  activeIndicators: string[];
  chartType: "candlestick" | "line" | "area";
  priceScaleMode: "linear" | "log";
}

function MainChart({ symbol, interval, activeIndicators, chartType, priceScaleMode }: MainChartProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const lineRef   = useRef<ISeriesApi<"Line"> | null>(null);
  const areaRef   = useRef<ISeriesApi<"Area"> | null>(null);
  const volRef    = useRef<ISeriesApi<"Histogram"> | null>(null);
  const macdRef   = useRef<ISeriesApi<"Histogram"> | null>(null);
  const rsiRef    = useRef<ISeriesApi<"Line"> | null>(null);
  const maMap     = useRef(new Map<string, ISeriesApi<"Line">>());
  const bbMap     = useRef(new Map<string, ISeriesApi<"Line">>());
  const legendRef = useRef<HTMLDivElement | null>(null);
  const plMap     = useRef(new Map<string, ReturnType<ISeriesApi<"Candlestick">["createPriceLine"]>>());
  const liveBarRef = useRef<{ timestamp: number; open: number; high: number; low: number; close: number } | null>(null);
  const barsRef = useRef<OHLCVBar[]>([]);

  const { data: ohlcvData } = useQuery({
    queryKey: ["ohlcv", symbol, interval],
    queryFn: () => stockApi.getOHLCV(symbol, interval, 300).then((r) => r.data),
    staleTime: 0,
  });
  const { data: historyData } = useQuery({
    queryKey: ["history", symbol],
    queryFn: () => stockApi.getHistory(symbol, 90).then((r) => r.data),
    staleTime: 60_000,
    enabled: ["1D", "1W", "1M"].includes(interval),
  });

  const bars: OHLCVBar[] = useMemo(() => {
    const isDaily = ["1D", "1W", "1M"].includes(interval);
    // Daily intervals: prefer ohlcvData (1m candles), fallback to historyData
    // Intraday intervals: always use ohlcvData
    return isDaily ? (ohlcvData ?? historyData ?? []) : (ohlcvData ?? []);
  }, [ohlcvData, historyData, interval]);

  // ── Build crosshair legend HTML ─────────────────────────────────────────────
  const buildLegendHTML = useCallback((bar: OHLCVBar | null): string => {
    if (!bar) return "";
    const up = bar.close >= bar.open;
    const color = up ? "#00e676" : "#ff3d57";
    const volColor = up ? "rgba(0,230,118,0.5)" : "rgba(255,61,87,0.5)";
    const dateStr = new Date(bar.timestamp).toLocaleString("vi-VN", { dateStyle: "short", timeStyle: "short" });
    return (
      `<div style="display:flex;align-items:center;gap:8px;pointer-events:none;">` +
      `<span style="color:#4a5a6a;">O</span><span style="color:#8899aa;font-weight:600;">${formatPrice(bar.open)}</span>` +
      `<span style="color:#00e676;">H</span><span style="color:#00e676;font-weight:600;">${formatPrice(bar.high)}</span>` +
      `<span style="color:#ff3d57;">L</span><span style="color:#ff3d57;font-weight:600;">${formatPrice(bar.low)}</span>` +
      `<span style="color:#4a5a6a;">C</span><span style="color:${color};font-weight:700;">${formatPrice(bar.close)}</span>` +
      `<span style="color:#4a5a6a;">Vol</span><span style="color:#8899aa;">${formatVolume(bar.volume)}</span>` +
      `<span style="color:${volColor};font-size:9px;">●</span>` +
      `<span style="color:#2a3a4a;font-size:10px;margin-left:6px;">${dateStr}</span>` +
      `</div>`
    );
  }, []);

  // ── Mount ──────────────────────────────────────────────────────────────────
  useEffect(() => {
    const wrapper = wrapperRef.current;
    if (!wrapper) return;

    const chart = createChart(wrapper, {
      layout: {
        background: { type: ColorType.Solid, color: "#080c14" },
        textColor: "#4a5a6a",
        fontSize: 11,
        fontFamily: "IBM Plex Mono, JetBrains Mono, monospace",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.03)" },
        horzLines: { color: "rgba(255,255,255,0.03)" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "#2a3a4a", width: 1, style: 2, labelBackgroundColor: "#00d4ff" },
        horzLine: { color: "#2a3a4a", width: 1, style: 2, labelBackgroundColor: "#00d4ff" },
      },
      rightPriceScale: {
        borderColor: "rgba(255,255,255,0.05)",
        scaleMargins: { top: 0.05, bottom: 0.35 },
        mode: priceScaleMode === "log" ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
      },
      timeScale: {
        borderColor: "rgba(255,255,255,0.05)",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    // Candlestick series (Pane 0 — main)
    const candleSeries = chart.addCandlestickSeries({
      upColor: "#00e676", downColor: "#ff3d57",
      borderUpColor: "#00e676", borderDownColor: "#ff3d57",
      wickUpColor: "#00e676", wickDownColor: "#ff3d57",
    });

    // Line series (hidden until activated)
    const lineSeries = chart.addLineSeries({
      color: "#00d4ff", lineWidth: 2,
      priceLineVisible: false, lastValueVisible: false,
    });

    // Area series (hidden until activated)
    const areaSeries = chart.addAreaSeries({
      lineColor: "#00d4ff", topColor: "rgba(0,212,255,0.15)", bottomColor: "rgba(0,212,255,0.02)",
      lineWidth: 2, priceLineVisible: false, lastValueVisible: false,
    });

    // Volume histogram — overlay price scale, positioned in bottom 20%
    const volSeries = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "", // overlay — no separate scale
      color: "rgba(0,230,118,0.15)",
    });
    volSeries.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    // MACD histogram — overlay, below volume pane
    const macdSeries = chart.addHistogramSeries({
      priceFormat: { type: "custom", formatter: (p: number) => p.toFixed(2) },
      priceScaleId: "", // overlay
      color: "#2962ff",
    });
    macdSeries.priceScale().applyOptions({ scaleMargins: { top: 0.90, bottom: 0 } });
    macdSeries.applyOptions({ visible: false });

    // RSI line — overlay
    const rsiSeries = chart.addLineSeries({
      color: "#f59e0b", lineWidth: 1,
      priceLineVisible: false, lastValueVisible: false,
      priceScaleId: "", // overlay
    });
    rsiSeries.priceScale().applyOptions({ scaleMargins: { top: 0.90, bottom: 0 } });
    rsiSeries.applyOptions({ visible: false });

    // Inject legend HTML overlay directly into chart DOM
    const legendEl = document.createElement("div");
    legendEl.style.cssText =
      "position:absolute;top:4px;left:8px;z-index:10;" +
      "font-size:11px;font-family:'IBM Plex Mono',monospace;" +
      "color:#8899aa;pointer-events:none;";
    wrapper.appendChild(legendEl);
    legendRef.current = legendEl;

    chartRef.current = chart;
    candleRef.current = candleSeries;
    lineRef.current   = lineSeries;
    areaRef.current  = areaSeries;
    volRef.current   = volSeries;
    macdRef.current   = macdSeries;
    rsiRef.current    = rsiSeries;

    // ResizeObserver for responsive width
    const ro = new ResizeObserver(() => {
      if (chartRef.current && wrapper) {
        chartRef.current.applyOptions({ width: wrapper.clientWidth, height: wrapper.clientHeight });
      }
    });
    ro.observe(wrapper);

    // Crosshair move → reads latest bars via refs (no stale closure)
    chart.subscribeCrosshairMove((param) => {
      const legend = legendRef.current;
      if (!legend) return;
      const w = wrapperRef.current;
      const currentBars = barsRef.current;

      const outOfBounds = !param.time || !param.point
        || !w || param.point.x < 0 || param.point.y < 0
        || param.point.x > w.clientWidth
        || param.point.y > w.clientHeight;

      if (outOfBounds) {
        const last = currentBars[currentBars.length - 1];
        if (last) legend.innerHTML = buildLegendHTML(last);
        return;
      }

      const t = param.time as number;
      const bar = currentBars.find((b) => Math.abs(b.timestamp / 1000 - t) < 5);
      if (bar) legend.innerHTML = buildLegendHTML(bar);
    });

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      candleRef.current = null;
      lineRef.current   = null;
      areaRef.current   = null;
      volRef.current    = null;
      macdRef.current   = null;
      rsiRef.current    = null;
      legendRef.current = null;
      plMap.current.clear();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Data update ────────────────────────────────────────────────────────────
  useEffect(() => {
    barsRef.current = bars; // synced to ref so mount-time crosshair callback always reads latest
    if (!chartRef.current || bars.length === 0) return;

    console.debug("[ChartPageV2] setData → interval:", interval, "bars:", bars.length, "first:", bars[0]?.timestamp, "last:", bars[bars.length - 1]?.timestamp);

    const cd: CandlestickData[] = bars.map((b) => ({
      time: toChartTime(b.timestamp),
      open: b.open, high: b.high, low: b.low, close: b.close,
    }));
    const ld: LineData[] = bars.map((b) => ({
      time: toChartTime(b.timestamp), value: b.close,
    }));
    const vd: HistogramData[] = bars.map((b) => ({
      time: toChartTime(b.timestamp),
      value: b.volume,
      color: b.close >= b.open ? "rgba(0,230,118,0.35)" : "rgba(255,61,87,0.35)",
    }));

    candleRef.current?.setData(cd);
    lineRef.current?.setData(ld);
    areaRef.current?.setData(ld);
    volRef.current?.setData(vd);

    // Show/hide series based on chartType
    candleRef.current?.applyOptions({ visible: chartType === "candlestick" });
    lineRef.current?.applyOptions({ visible: chartType === "line" });
    areaRef.current?.applyOptions({ visible: chartType === "area" });

    // Price scale mode
    chartRef.current.applyOptions({
      rightPriceScale: {
        mode: priceScaleMode === "log" ? PriceScaleMode.Logarithmic : PriceScaleMode.Normal,
      },
    });

    // Price reference lines — prev close + open
    plMap.current.forEach((pl) => candleRef.current?.removePriceLine(pl));
    plMap.current.clear();
    if (bars.length >= 2) {
      const prevClose = bars[bars.length - 2].close;
      const todayOpen = bars[bars.length - 1].open;
      if (candleRef.current) {
        plMap.current.set("prevClose", candleRef.current.createPriceLine({
          price: prevClose, color: "#fbbf24", lineStyle: 2,
          lineWidth: 1, axisLabelVisible: true, title: "Prev",
        }));
        plMap.current.set("todayOpen", candleRef.current.createPriceLine({
          price: todayOpen, color: "#60a5fa", lineStyle: 1,
          lineWidth: 1, axisLabelVisible: true, title: "Open",
        }));
      }
    }

    chartRef.current.timeScale().fitContent();

    // Reset live bar on new dataset
    liveBarRef.current = null;

    // Scroll to realtime on intraday intervals
    if (INTRADAY_INTERVALS.has(interval)) {
      setTimeout(() => chartRef.current?.timeScale().scrollToRealTime(), 100);
    }

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bars, chartType, priceScaleMode, interval]);

  // ── MA overlays ─────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!chartRef.current) return;
    const current = maMap.current;
    for (const [id, s] of current) {
      if (!activeIndicators.includes(id)) {
        chartRef.current.removeSeries(s);
        current.delete(id);
      }
    }
    for (const indId of activeIndicators) {
      if (current.has(indId) || indId === "BB" || indId === "MACD" || indId === "RSI") continue;
      const cfg = INDICATOR_COLORS[indId];
      if (!cfg || cfg.type === "BB" || cfg.type === "MACD" || cfg.type === "RSI") continue;
      const data = cfg.type === "EMA" ? computeEMA(bars, cfg.period) : computeSMA(bars, cfg.period);
      if (!data.length) continue;
      const series = chartRef.current.addLineSeries({
        color: cfg.color, lineWidth: 1,
        priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
      });
      series.setData(data as LineData[]);
      current.set(indId, series);
    }
  }, [bars, activeIndicators]);

  // ── Bollinger Bands ─────────────────────────────────────────────────────────
  useEffect(() => {
    if (!chartRef.current) return;
    const bb = bbMap.current;
    for (const [, s] of bb) { chartRef.current.removeSeries(s); }
    bb.clear();
    if (!activeIndicators.includes("BB") || bars.length < 20) return;
    const { upper, middle, lower } = computeBollinger(bars, 20, 2);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mk = (color: string, ls: 0|1|2|3|4 = 0) =>
      chartRef.current!.addLineSeries({ color, lineWidth: 1, lineStyle: ls,
        priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false }) as ISeriesApi<"Line">;
    const us = mk("#f59e0b", 2), ms = mk("#f59e0b"), ls2 = mk("#f59e0b", 2);
    us.setData(upper as LineData[]); ms.setData(middle as LineData[]); ls2.setData(lower as LineData[]);
    bb.set("upper", us); bb.set("middle", ms); bb.set("lower", ls2);
  }, [bars, activeIndicators]);

  // ── MACD overlay ───────────────────────────────────────────────────────────
  useEffect(() => {
    if (!macdRef.current) return;
    const show = activeIndicators.includes("MACD");
    macdRef.current.applyOptions({ visible: show });
    if (!show) return;
    const { histogram } = computeMACD(bars, 12, 26, 9);
    if (histogram.length) {
      macdRef.current.setData(histogram as unknown as HistogramData[]);
    }
  }, [bars, activeIndicators]);

  // ── RSI overlay ─────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!rsiRef.current) return;
    const show = activeIndicators.includes("RSI");
    rsiRef.current.applyOptions({ visible: show });
    if (!show) return;
    const { rsiLine } = computeRSI(bars, 14);
    if (rsiLine.length) {
      rsiRef.current.setData(rsiLine as LineData[]);
    }
  }, [bars, activeIndicators]);

  // ── Series Markers — "Now" marker on latest bar ────────────────────────────
  useEffect(() => {
    if (!candleRef.current || bars.length === 0) return;
    const lastBar = bars[bars.length - 1];
    candleRef.current.setMarkers([
      {
        time: toChartTime(lastBar.timestamp),
        position: "belowBar",
        color: "#2962ff",
        shape: "circle",
        text: "Now",
      },
    ]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bars]);

  // ── WebSocket live tick ─────────────────────────────────────────────────────
  const handleWsMsg = useCallback((msg: unknown) => {
    const m = msg as { type?: string; last_price?: number; highest?: number; lowest?: number; volume?: number };
    if (m.type !== "price_update" || m.last_price == null) return;
    const price = m.last_price;

    if (!liveBarRef.current) {
      liveBarRef.current = {
        timestamp: Math.floor(Date.now() / 1000),
        open: price, high: price, low: price, close: price,
      };
    } else {
      liveBarRef.current.high = Math.max(liveBarRef.current.high, price);
      liveBarRef.current.low  = Math.min(liveBarRef.current.low, price);
      liveBarRef.current.close = price;
    }

    candleRef.current?.update({
      time: liveBarRef.current.timestamp as Time,
      open: liveBarRef.current.open,
      high: liveBarRef.current.high,
      low:  liveBarRef.current.low,
      close: liveBarRef.current.close,
    });
    volRef.current?.update({
      time: liveBarRef.current.timestamp as Time,
      value: m.volume ?? 0,
      color: price >= liveBarRef.current.open ? "rgba(0,230,118,0.5)" : "rgba(255,61,87,0.5)",
    });
  }, []);

  useStockWebSocket({ symbol, onMessage: handleWsMsg });

  return (
    <div ref={wrapperRef} style={{ width: "100%", height: "100%", position: "relative" }}>
      {bars.length === 0 && (
        <div className={styles["chart-canvas-placeholder"]}>
          <div className={styles["chart-spinner"]} />
          <span>Loading chart data…</span>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// ChartLegendBar — shows last bar OHLCV (no crosshair state — it's injected)
// ─────────────────────────────────────────────────────────────────────────────

function ChartLegendBar({
  symbol, interval, quote, bars,
}: {
  symbol: string; interval: string;
  quote: StockQuote | null; bars: OHLCVBar[];
}) {
  const lastBar = bars[bars.length - 1];
  const isUp = (lastBar?.close ?? quote?.last_price ?? 0) >= (lastBar?.open ?? quote?.ref_price ?? 0);

  return (
    <div className={styles["chart-legend-bar"]}>
      <span className={styles["chart-breadcrumb-sym"]}>{symbol}</span>
      <span className={styles["chart-breadcrumb-sep"]}>·</span>
      <span>{interval}</span>
      <span className={styles["chart-breadcrumb-sep"]}>·</span>
      <span>HOSE</span>

      <span className={styles["legend-sep"]}>|</span>

      {(["O", "H", "L", "C", "Vol"] as const).map((key) => {
        const val = lastBar
          ? key === "O" ? lastBar.open : key === "H" ? lastBar.high :
            key === "L" ? lastBar.low : key === "C" ? lastBar.close : lastBar.volume
          : key === "O" ? quote?.ref_price : key === "H" ? quote?.highest :
            key === "L" ? quote?.lowest : key === "C" ? quote?.last_price : quote?.volume;
        return (
          <span key={key} className={styles["legend-item"]}>
            <span className={styles["legend-label"]}>{key} </span>
            <span className={styles["legend-value"]} style={
              key === "H" ? { color: "#00e676" } :
              key === "L" ? { color: "#ff3d57" } :
              key === "C" ? (isUp ? { color: "#00e676" } : { color: "#ff3d57" }) : {}
            }>
              {key === "Vol" ? formatVolume(val as number) : formatPrice(val as number)}
            </span>
          </span>
        );
      })}

      <span style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 4 }}>
        <div className={styles["live-dot"]} />
        <span className={styles["live-label"]}>Live</span>
      </span>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// DepthChart — canvas-based depth histogram
// ─────────────────────────────────────────────────────────────────────────────

function DepthChart({ book }: { book: OrderBook | null }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const bids = book?.bids.filter((b) => b.price > 0) ?? [];
    const asks = book?.asks.filter((a) => a.price > 0) ?? [];
    const allPrices = [...bids.map((b) => b.price), ...asks.map((a) => a.price)];
    if (allPrices.length === 0) return;

    const minP = Math.min(...allPrices);
    const maxP = Math.max(...allPrices);
    const range = maxP - minP || 1;

    const W = canvas.width = canvas.offsetWidth * 2;
    const H = canvas.height = canvas.offsetHeight * 2;
    ctx.scale(2, 2);
    const w = W / 2, h = H / 2;
    ctx.clearRect(0, 0, w, h);

    const maxVol = Math.max(...bids.map((b) => b.volume), ...asks.map((a) => a.volume), 1);

    const drawBar = (price: number, vol: number, color: string) => {
      const x = ((price - minP) / range) * w;
      const barH = (vol / maxVol) * (h - 4);
      ctx.fillStyle = color;
      ctx.fillRect(x - 2, h - barH, 4, barH);
    };

    bids.forEach((b) => drawBar(b.price, b.volume, "rgba(0,230,118,0.5)"));
    asks.forEach((a) => drawBar(a.price, a.volume, "rgba(255,61,87,0.5)"));

    ctx.fillStyle = "#3a4a5a";
    ctx.font = "8px IBM Plex Mono, monospace";
    ctx.textAlign = "center";
    ctx.fillText(formatPrice(minP), 4, h - 2);
    ctx.fillText(formatPrice(maxP), w - 4, h - 2);
  }, [book]);

  return <canvas ref={canvasRef} style={{ width: "100%", height: "100%", display: "block" }} />;
}

// ─────────────────────────────────────────────────────────────────────────────
// MarketDepth
// ─────────────────────────────────────────────────────────────────────────────

function MarketDepth({ symbol, t }: { symbol: string; t: (key: string) => string }) {
  const [book, setBook] = useState<OrderBook | null>(null);

  const { data: restBook } = useQuery({
    queryKey: ["orderbook", symbol],
    queryFn: () => stockApi.getOrderBook(symbol).then((r) => r.data),
    staleTime: 10_000,
  });

  // Fetch quote for ceiling/floor to color price cells
  const { data: quote } = useQuery({
    queryKey: ["quote", symbol],
    queryFn: () => stockApi.getQuote(symbol).then((r) => r.data),
    staleTime: 15_000,
  });

  useStockWebSocket({
    symbol,
    onMessage: (msg) => {
      if (msg.type === "orderbook_update" && (msg as unknown as { symbol: string }).symbol === symbol) {
        const m = msg as unknown as { symbol: string; bids: OrderBook["bids"]; asks: OrderBook["asks"]; time: string };
        setBook({ symbol: m.symbol, bids: m.bids, asks: m.asks, time: m.time });
      }
    },
  });

  useEffect(() => { if (restBook) setBook(restBook); }, [restBook]);

  const bids = book?.bids ?? [];
  const asks = book?.asks ?? [];
  const totalBidVol = bids.reduce((s, b) => s + b.volume, 0);
  const totalAskVol = asks.reduce((s, a) => s + a.volume, 0);
  const totalVol = totalBidVol + totalAskVol || 1;
  const bidPct = (totalBidVol / totalVol) * 100;
  const maxVol = Math.max(...bids.map((b) => b.volume), ...asks.map((a) => a.volume), 1);

  const refPrice = quote?.ref_price ?? 0;
  const ceiling  = quote?.ceiling  ?? 0;
  const floor    = quote?.floor    ?? 0;

  // ── Color helpers ─────────────────────────────────────────────────────────
  // Universal rules for BOTH bid and ask prices:
  //   = ceiling  → purple  (#c084fc)
  //   = floor    → blue    (#60a5fa)
  //   = ref      → yellow  (#fbbf24)
  //   > ref      → green   (#00e676)
  //   < ref      → red     (#ff3d57)
  const priceColor = (price: number | undefined): string => {
    if (price == null || price === 0) return "#60a5fa";
    if (floor > 0 && price === floor)   return "#60a5fa";
    if (ceiling > 0 && price === ceiling) return "#c084fc";
    if (price === refPrice)            return "#fbbf24";
    if (price > refPrice)              return "#00e676";
    return "#ff3d57";                  // below ref
  };

  const DEPTH_ROWS = 3;

  return (
    <div className={styles["market-depth-section"]}>
      <div className={styles["section-header"]}>
        <span className={styles["section-title"]}>{t("chart.marketDepth")}</span>
      </div>
      <div className={styles["depth-summary"]}>
        <div className={styles["depth-sum-col"]}>
          <div className={styles["depth-sum-label"]}>{t("chart.totalBidVol")}</div>
          <div className={styles["depth-sum-bid"]}>{formatVolume(totalBidVol)}</div>
          <div className={styles["depth-bar-container"]}>
            <div className={styles["depth-bar-fill"]} style={{ width: `${bidPct}%` }} />
          </div>
        </div>
        <div className={styles["depth-sum-col"]}>
          <div className={styles["depth-sum-label"]}>{t("chart.totalAskVol")}</div>
          <div className={styles["depth-sum-ask"]}>{formatVolume(totalAskVol)}</div>
          <div className={styles["depth-bar-container"]}>
            <div className={styles["depth-bar-fill"]} style={{ width: `${100 - bidPct}%`, background: "#ff3d57" }} />
          </div>
        </div>
      </div>
      <table className={styles["depth-table"]}>
        <thead>
          <tr>
            <th>{t("chart.klMua")}</th><th>{t("chart.giaMua")}</th><th>{t("chart.giaBan")}</th><th>{t("chart.klBan")}</th>
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: DEPTH_ROWS }).map((_, i) => {
            const bid = bids[i];
            const ask = asks[i];

            const bidVolStyle: React.CSSProperties = {
              width: `${(bid ? bid.volume / maxVol : 0) * 100}%`,
            };
            const askVolStyle: React.CSSProperties = {
              width: `${(ask ? ask.volume / maxVol : 0) * 100}%`,
            };

            const pColor = priceColor(bid?.price);

            return (
              <tr key={i} className={styles["depth-row"]}>
                {/* Bid Vol */}
                <td className={styles["depth-bar-cell"]}>
                  <div className={`${styles["depth-bar"]} ${styles.bid}`} style={bidVolStyle} />
                  <span style={{ color: pColor }}>
                    {bid ? formatVolume(bid.volume) : "–"}
                  </span>
                </td>
                {/* Giá mua */}
                <td className={styles["depth-price"]} style={{ color: pColor }}>
                  {bid ? formatPrice(bid.price) : "–"}
                </td>
                {/* Giá bán */}
                <td className={styles["depth-price"]} style={{ color: priceColor(ask?.price) }}>
                  {ask ? formatPrice(ask.price) : "–"}
                </td>
                {/* Ask Vol */}
                <td className={styles["depth-bar-cell"]}>
                  <div className={`${styles["depth-bar"]} ${styles.ask}`} style={askVolStyle} />
                  <span style={{ color: priceColor(ask?.price) }}>
                    {ask ? formatVolume(ask.volume) : "–"}
                  </span>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// TradeMatching — live trade tape
// ─────────────────────────────────────────────────────────────────────────────

function TradeMatching({ symbol, t }: { symbol: string; t: (key: string) => string }) {
  const [ticks, setTicks] = useState<TradeTick[]>([]);

  useStockWebSocket({
    symbol,
    onMessage: (msg) => {
      if (msg.type === "price_update" && (msg as unknown as { symbol: string }).symbol === symbol) {
        const m = msg as unknown as { time?: string; last_price: number; volume: number; change: number };
        const side: TradeTick["side"] = m.change > 0 ? "buy" : m.change < 0 ? "sell" : "neutral";
        setTicks((prev) => [
          { time: m.time ?? new Date().toLocaleTimeString("vi-VN"), price: m.last_price, volume: m.volume ?? 0, change: m.change ?? 0, side },
          ...prev.slice(0, 49),
        ]);
      }
    },
  });

  return (
    <div className={styles["trade-section"]}>
      <div className={styles["section-header"]}>
        <span className={styles["section-title"]}>{t("chart.tradeMatching")}</span>
        <span className={styles["section-header-right"]}>{ticks.length} ticks</span>
      </div>
      <div className={styles["trade-table-wrap"]}>
        <table className={styles["trade-table"]}>
          <thead>
            <tr>
              <th>{t("chart.time")}</th><th>{t("chart.price")}</th><th>{t("chart.vol")}</th><th>{t("chart.change")}</th><th>{t("chart.side")}</th>
            </tr>
          </thead>
          <tbody>
            {ticks.length === 0 && (
              <tr><td colSpan={5} className={styles["trade-empty"]}>{t("chart.waiting")}</td></tr>
            )}
            {ticks.map((tick, idx) => (
              <tr key={idx} className={`${styles["trade-row"]} ${tick.side === "buy" ? styles.buy : tick.side === "sell" ? styles.sell : ""}`}>
                <td className={styles["trade-time"]}>{tick.time}</td>
                <td className={`${styles["trade-price"]} ${tick.change >= 0 ? styles.up : styles.down}`}>
                  {formatPrice(tick.price)}
                </td>
                <td className={styles["trade-vol"]}>{formatVolume(tick.volume)}</td>
                <td className={styles["trade-change"]}>
                  <span style={{ color: tick.change >= 0 ? "#00e676" : "#ff3d57" }}>{fmtChange(tick.change)}</span>
                </td>
                <td className={`${styles["trade-side"]} ${tick.side === "buy" ? styles.up : tick.side === "sell" ? styles.down : ""}`}>
                  {tick.side === "buy" ? "M" : tick.side === "sell" ? "B" : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// ChartModal
// ─────────────────────────────────────────────────────────────────────────────

interface ChartModalProps {
  symbol: string;
  onClose: () => void;
}

export function ChartModal({ symbol, onClose }: ChartModalProps) {
  const { t } = useTranslation();
  const [activeSymbol, setActiveSymbol] = useState(symbol);
  const [interval, setInterval] = useState("1D");
  const [chartType, setChartType] = useState<"candlestick" | "line" | "area">("candlestick");
  const [activeIndicators, setActiveIndicators] = useState<string[]>(["MA20"]);
  const [priceScaleMode, setPriceScaleMode] = useState<"linear" | "log">("linear");
  const [activeTab, setActiveTab] = useState(t("chart.tab.transaction"));

  const { data: ohlcvData } = useQuery({
    queryKey: ["ohlcv", activeSymbol, interval],
    queryFn: () => stockApi.getOHLCV(activeSymbol, interval, 300).then((r) => {
      console.debug("[ChartModal] getOHLCV", activeSymbol, interval, "→ bars:", r.data.length);
      return r.data;
    }),
    staleTime: 30_000,
  });
  const { data: historyData } = useQuery({
    queryKey: ["history", activeSymbol],
    queryFn: () => stockApi.getHistory(activeSymbol, 90).then((r) => {
      console.debug("[ChartModal] getHistory", activeSymbol, "→ bars:", r.data.length);
      return r.data;
    }),
    staleTime: 60_000,
    enabled: ["1D", "1W", "1M"].includes(interval),
  });
  const { data: quote } = useQuery({
    queryKey: ["quote", activeSymbol],
    queryFn: () => stockApi.getQuote(activeSymbol).then((r) => r.data),
    staleTime: 15_000,
  });
  const { data: orderBook } = useQuery({
    queryKey: ["orderbook", activeSymbol],
    queryFn: () => stockApi.getOrderBook(activeSymbol).then((r) => r.data),
    staleTime: 10_000,
  });

  const bars: OHLCVBar[] = useMemo(() => {
    const isDaily = ["1D", "1W", "1M"].includes(interval);
    // Daily intervals: prefer ohlcvData (1m candles), fallback to historyData
    // Intraday intervals: always use ohlcvData
    return isDaily ? (ohlcvData ?? historyData ?? []) : (ohlcvData ?? []);
  }, [ohlcvData, historyData, interval]);
  const isUp = (quote?.change ?? 0) >= 0;

  useEffect(() => { setActiveSymbol(symbol); }, [symbol]);

  const toggleIndicator = useCallback((ind: string) => {
    setActiveIndicators((prev) => prev.includes(ind) ? prev.filter((i) => i !== ind) : [...prev, ind]);
  }, []);

  const handleOverlayClick = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  }, [onClose]);

  return (
    <div className={styles.overlay} onClick={handleOverlayClick}>
      <div className={styles.modal} onClick={(e) => e.stopPropagation()}>

        {/* ── Header ── */}
        <div className={styles["modal-header"]}>
          <div className={styles["header-tabs"]}>
            {[t("chart.tab.transaction"), t("chart.tab.profile"), t("chart.tab.shareholder"), t("chart.tab.capitalDividend"), t("chart.tab.news"), t("chart.tab.stats")].map((tab) => (
              <button
                key={tab}
                className={`${styles["header-tab"]} ${activeTab === tab ? styles.active : ""}`}
                onClick={() => setActiveTab(tab)}
              >
                {tab}
              </button>
            ))}
          </div>
          <div className={styles["header-right"]}>
            <button className={styles["order-btn"]}>{t("chart.orderBtn")}</button>
            <button className={styles["close-btn"]} onClick={onClose}>✕</button>
          </div>
        </div>

        {/* ── Body ── */}
        <div className={styles["modal-body"]}>
          <div className={styles["main-content"]}>

            {/* ── Stock Info Bar ── */}
            <div className={styles["stock-info-bar"]}>
              <div className={styles["stock-info-left"]}>
                <span className={styles["stock-symbol-text"]}>{activeSymbol}</span>
                <span className={styles["stock-exchange-badge"]}>HOSE</span>
                <span className={styles["stock-name-text"]}>
                  {WATCHLIST_ITEMS.find((i) => i.symbol === activeSymbol)?.name ?? activeSymbol}
                </span>
              </div>
              <div className={styles["stock-price-block"]}>
                <span className={styles["stock-price"]} style={{ color: isUp ? "#00e676" : "#ff3d57" }}>
                  {formatPrice(quote?.last_price)}
                </span>
                <span className={`${styles["stock-change-badge"]} ${isUp ? styles.up : styles.down}`}>
                  {quote ? `${fmtChange(quote.change)} (${fmtChange(quote.ratio_change)}%)` : "—"}
                </span>
              </div>
              <div className={styles["stock-divider"]} />
              <div className={styles["stock-stats"]}>
                <div className={styles["stock-stat"]}>
                  <span className={styles["stock-stat-label"]}>{t("chart.openTC")}</span>
                  <span className={`${styles["stock-stat-value"]} ${styles.ref}`}>{formatPrice(quote?.ref_price)}</span>
                </div>
                <span className={styles["stat-sep"]}>|</span>
                <div className={styles["stock-stat"]}>
                  <span className={styles["stock-stat-label"]}>{t("col.high")}</span>
                  <span className={`${styles["stock-stat-value"]} ${styles.up}`}>{formatPrice(quote?.highest)}</span>
                </div>
                <span className={styles["stat-sep"]}>|</span>
                <div className={styles["stock-stat"]}>
                  <span className={styles["stock-stat-label"]}>{t("col.low")}</span>
                  <span className={`${styles["stock-stat-value"]} ${styles.down}`}>{formatPrice(quote?.lowest)}</span>
                </div>
                <span className={styles["stat-sep"]}>|</span>
                <div className={styles["stock-stat"]}>
                  <span className={styles["stock-stat-label"]}>{t("col.ceiling")}</span>
                  <span className={`${styles["stock-stat-value"]} ${styles.ceiling}`}>{formatPrice(quote?.ceiling)}</span>
                </div>
                <span className={styles["stat-sep"]}>|</span>
                <div className={styles["stock-stat"]}>
                  <span className={styles["stock-stat-label"]}>{t("col.floor")}</span>
                  <span className={`${styles["stock-stat-value"]} ${styles.floor}`}>{formatPrice(quote?.floor)}</span>
                </div>
                <span className={styles["stat-sep"]}>|</span>
                <div className={styles["stock-stat"]}>
                  <span className={styles["stock-stat-label"]}>{t("col.totalVol")}</span>
                  <span className={styles["stock-stat-value"]}>{formatVolume(quote?.volume)}</span>
                </div>
              </div>
              <div className={styles["info-bar-right"]}>
                <div className={styles["live-badge"]}>
                  <div className={styles["live-dot"]} />
                  <span className={styles["live-label"]}>Live</span>
                </div>
              </div>
            </div>

            {/* ── Chart Section ── */}
            <div className={styles["chart-section"]}>
              <div className={styles["chart-main"]}>

                {/* Top bar */}
                <div className={styles["chart-topbar"]}>
                  <div className={styles["chart-breadcrumb"]}>
                    <span className={styles["chart-breadcrumb-sym"]}>{activeSymbol}</span>
                    <span className={styles["chart-breadcrumb-sep"]}>·</span>
                    <span>{interval}</span>
                    <span className={styles["chart-breadcrumb-sep"]}>·</span>
                    <span>HOSE</span>
                  </div>

                  <div className={styles["chart-controls"]}>
                    {(["candlestick", "line", "area"] as const).map((ct) => (
                      <button key={ct}
                        className={`${styles["chart-control-btn"]} ${chartType === ct ? styles.active : ""}`}
                        onClick={() => setChartType(ct)}>
                        {ct === "candlestick" ? "📊" : ct === "line" ? "📈" : "📉"}
                      </button>
                    ))}
                    <div className={styles["ctrl-sep"]} />

                    {INDICATOR_KEYS.map((ind) => (
                      <button key={ind}
                        className={`${styles["chart-control-btn"]} ${activeIndicators.includes(ind) ? styles.active : ""}`}
                        onClick={() => toggleIndicator(ind)}
                        style={activeIndicators.includes(ind) ? {
                          borderColor: INDICATOR_COLORS[ind]?.color,
                          color: INDICATOR_COLORS[ind]?.color,
                        } : {}}>
                        {ind}
                      </button>
                    ))}
                  </div>
                </div>

                {/* Legend bar */}
                <ChartLegendBar
                  symbol={activeSymbol}
                  interval={interval}
                  quote={quote ?? null}
                  bars={bars}
                />

                {/* Chart canvas */}
                <div className={styles["chart-canvas-area"]}>
                  <MainChart
                    symbol={activeSymbol}
                    interval={interval}
                    activeIndicators={activeIndicators}
                    chartType={chartType}
                    priceScaleMode={priceScaleMode}
                  />
                </div>

                {/* Bottom bar */}
                <div className={styles["chart-bottombar"]}>
                  <div className={styles["range-tabs"]}>
                    {(["1m", "5m", "15m", "30m", "1h", "4h", "1D", "1W", "1M"] as const).map((r) => (
                      <button key={r}
                        className={`${styles["range-tab"]} ${interval === r ? styles.active : ""}`}
                        onClick={() => setInterval(r)}>
                        {r}
                      </button>
                    ))}
                  </div>
                  <div className={styles["chart-info-right"]}>
                    <button
                      className={`${styles["scale-btn"]} ${priceScaleMode === "log" ? styles.active : ""}`}
                      onClick={() => setPriceScaleMode((p) => p === "log" ? "linear" : "log")}
                    >
                      {priceScaleMode === "log" ? "Log" : "Lin"}
                    </button>
                  </div>
                </div>
              </div>

              {/* Right sidebar */}
              <div className={styles["right-sidebar"]}>
                <MarketDepth symbol={activeSymbol} t={t} />
                <div className={styles["depth-histogram"]}>
                  <DepthChart book={orderBook ?? null} />
                </div>
                <TradeMatching symbol={activeSymbol} t={t} />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Standalone export
// ─────────────────────────────────────────────────────────────────────────────

export { ChartModal as ChartPageV2 };