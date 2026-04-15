import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  createChart,
  ColorType,
  CrosshairMode,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type Time,
} from "lightweight-charts";
import { useAppStore } from "../../../../stores/appStore";
import { stockApi } from "../../../../api/stockApi";
import { formatPrice, formatVolume } from "../../../../lib/utils";
import styles from "./MainChart.module.css";

// ── Symbol to crypto detection ───────────────────────────────────────────────
const CRYPTO = new Set(["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "DOT"]);

// ── Interval options ──────────────────────────────────────────────────────────
const INTERVALS = ["1m", "5m", "15m", "30m", "1h", "4h", "1D", "1W", "1M"] as const;

type Interval = typeof INTERVALS[number];

// ── Simulated crypto data ────────────────────────────────────────────────────
const CRYPTO_DATA: Record<string, { price: number; change24h: number; high24h: number; low24h: number; vol24h: number }> = {
  BTC: { price: 67142.50, change24h: 2.34, high24h: 68500, low24h: 65400, vol24h: 28_500_000_000 },
  ETH: { price: 3456.20, change24h: 1.87, high24h: 3520, low24h: 3380, vol24h: 14_200_000_000 },
  SOL: { price: 182.45, change24h: -1.23, high24h: 188, low24h: 178, vol24h: 3_400_000_000 },
  BNB: { price: 598.30, change24h: 0.56, high24h: 605, low24h: 589, vol24h: 1_800_000_000 },
  XRP: { price: 0.5234, change24h: -0.87, high24h: 0.535, low24h: 0.51, vol24h: 1_200_000_000 },
};

// ── Component ────────────────────────────────────────────────────────────────

export function MainChart() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const setActiveInterval = useAppStore((s) => s.setActiveInterval);
  const activeInterval = useAppStore((s) => s.activeInterval);

  const isCrypto = CRYPTO.has(selectedSymbol);

  return (
    <div className={styles.chartPanel}>
      {/* Toolbar */}
      <div className={styles.chartToolbar}>
        <span className={styles.chartSymbolBadge}>{selectedSymbol}</span>
        <div className={styles.toolbarDivider} />
        {INTERVALS.map((iv) => (
          <button
            key={iv}
            className={`${styles.intervalBtn} ${activeInterval === iv ? styles.active : ""}`}
            onClick={() => setActiveInterval(iv as Interval)}
          >
            {iv}
          </button>
        ))}
        <div className={styles.toolbarDivider} />
        <ChartTypeButtons />
      </div>

      {/* Content */}
      {isCrypto ? (
        <CryptoView symbol={selectedSymbol} />
      ) : (
        <CandlestickChart symbol={selectedSymbol} interval={activeInterval} />
      )}
    </div>
  );
}

// ── Chart type toggle buttons ────────────────────────────────────────────────

function ChartTypeButtons() {
  const setActiveChartType = useAppStore((s) => s.setActiveChartType);
  const activeChartType = useAppStore((s) => s.activeChartType);

  return (
    <div className={styles.chartTypeGroup}>
      <button
        className={`${styles.chartTypeBtn} ${activeChartType === "candlestick" ? styles.active : ""}`}
        onClick={() => setActiveChartType("candlestick")}
        title="Candlestick"
      >
        📊
      </button>
      <button
        className={`${styles.chartTypeBtn} ${activeChartType === "line" ? styles.active : ""}`}
        onClick={() => setActiveChartType("line")}
        title="Line"
      >
        📈
      </button>
      <button
        className={`${styles.chartTypeBtn} ${activeChartType === "area" ? styles.active : ""}`}
        onClick={() => setActiveChartType("area")}
        title="Area"
      >
        ▦
      </button>
    </div>
  );
}

// ── Candlestick Chart ─────────────────────────────────────────────────────────

interface CandlestickChartProps {
  symbol: string;
  interval: string;
}

function CandlestickChart({ symbol, interval }: CandlestickChartProps) {
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [legend, setLegend] = useState<{
    time: string; open: number; high: number; low: number; close: number; vol: number;
  } | null>(null);

  // ── Fetch OHLCV data ─────────────────────────────────────────────────────
  const { data: bars } = useQuery({
    queryKey: ["ohlcv", symbol, interval],
    queryFn: () => stockApi.getOHLCV(symbol, interval).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 60_000,
  });

  // ── Chart initialization ─────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "#0a0e17" },
        textColor: "#64748b",
        fontFamily: "JetBrains Mono, monospace",
        fontSize: 10,
      },
      grid: {
        vertLines: { color: "rgba(0,212,255,0.04)" },
        horzLines: { color: "rgba(0,212,255,0.04)" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "rgba(0,212,255,0.2)", labelBackgroundColor: "#0f1623" },
        horzLine: { color: "rgba(0,212,255,0.2)", labelBackgroundColor: "#0f1623" },
      },
      rightPriceScale: {
        borderColor: "rgba(0,212,255,0.08)",
        textColor: "#64748b",
        scaleMargins: { top: 0.1, bottom: 0.2 },
      },
      timeScale: {
        borderColor: "rgba(0,212,255,0.08)",
        timeVisible: true,
        secondsVisible: false,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    });

    chartRef.current = chart;

    // Candlestick series
    const candleSeries = chart.addCandlestickSeries({
      upColor: "#00e676",
      downColor: "#ff3d57",
      borderUpColor: "#00e676",
      borderDownColor: "#ff3d57",
      wickUpColor: "#00e676",
      wickDownColor: "#ff3d57",
    });
    candleSeriesRef.current = candleSeries;

    // Volume histogram
    const volumeSeries = chart.addHistogramSeries({
      color: "rgba(0,212,255,0.15)",
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });
    volumeSeriesRef.current = volumeSeries;

    // Crosshair legend
    chart.subscribeCrosshairMove((param) => {
      if (!param || !param.seriesData.size) {
        setLegend(null);
        return;
      }
      const cd = param.seriesData.get(candleSeries) as CandlestickData | undefined;
      const vd = param.seriesData.get(volumeSeries) as HistogramData | undefined;
      if (cd) {
        setLegend({
          time: String(cd.time),
          open: cd.open,
          high: cd.high,
          low: cd.low,
          close: cd.close,
          vol: vd?.value ?? 0,
        });
      }
    });

    // Resize observer
    const ro = new ResizeObserver(() => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight,
        });
      }
    });
    ro.observe(containerRef.current);

    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, []);

  // ── Load data when bars change ───────────────────────────────────────────
  useEffect(() => {
    if (!candleSeriesRef.current || !volumeSeriesRef.current || !bars?.length) return;

    const candleData: CandlestickData[] = bars.map((b) => ({
      time: (b.timestamp / 1000) as Time,
      open: b.open,
      high: b.high,
      low: b.low,
      close: b.close,
    }));

    const volumeData: HistogramData[] = bars.map((b) => ({
      time: (b.timestamp / 1000) as Time,
      value: b.volume,
      color: b.close >= b.open ? "rgba(0,230,118,0.3)" : "rgba(255,61,87,0.3)",
    }));

    candleSeriesRef.current.setData(candleData);
    volumeSeriesRef.current.setData(volumeData);

    chartRef.current?.timeScale().fitContent();
  }, [bars]);

  return (
    <div className={styles.chartArea}>
      {/* Legend bar */}
      {legend && (
        <div className={styles.legendBar}>
          <span className={styles.legendDot} />
          <span>O <span className={styles.legendVal}>{formatPrice(legend.open)}</span></span>
          <span>H <span className={styles.legendVal}>{formatPrice(legend.high)}</span></span>
          <span>L <span className={styles.legendVal}>{formatPrice(legend.low)}</span></span>
          <span>C <span className={styles.legendVal}>{formatPrice(legend.close)}</span></span>
          <span>Vol <span className={styles.legendVal}>{formatVolume(legend.vol)}</span></span>
        </div>
      )}
      {/* Chart container */}
      <div ref={containerRef} className={styles.chartContainer} />
    </div>
  );
}

// ── Crypto view ───────────────────────────────────────────────────────────────

function CryptoView({ symbol }: { symbol: string }) {
  const data = CRYPTO_DATA[symbol] ?? { price: 0, change24h: 0, high24h: 0, low24h: 0, vol24h: 0 };
  const isUp = data.change24h >= 0;

  return (
    <div className={styles.cryptoCard}>
      <div className={styles.cryptoSymbol}>{symbol}/USDT</div>
      <div className={styles.cryptoPrice}>{formatPrice(data.price)}</div>
      <div className={`${styles.cryptoChange} ${isUp ? styles.up : styles.down}`}>
        {isUp ? "▲" : "▼"} {isUp ? "+" : ""}
        {data.change24h.toFixed(2)}%
      </div>
      <div className={styles.cryptoNotice}>⚡ Simulated data — connect live feed for real prices</div>
      <div className={styles.cryptoStatsGrid}>
        <div className={styles.cryptoStat}>
          <span className={styles.cryptoStatLabel}>24h High</span>
          <span className={styles.cryptoStatValue}>{formatPrice(data.high24h)}</span>
        </div>
        <div className={styles.cryptoStat}>
          <span className={styles.cryptoStatLabel}>24h Low</span>
          <span className={styles.cryptoStatValue}>{formatPrice(data.low24h)}</span>
        </div>
        <div className={styles.cryptoStat}>
          <span className={styles.cryptoStatLabel}>24h Volume</span>
          <span className={styles.cryptoStatValue}>${(data.vol24h / 1e9).toFixed(1)}B</span>
        </div>
      </div>
    </div>
  );
}