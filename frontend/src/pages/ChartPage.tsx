import { useEffect, useRef, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  createChart,
  IChartApi,
  ISeriesApi,
  CandlestickData,
  HistogramData,
  LineData,
  Time,
  CrosshairMode,
} from "lightweight-charts";
import { stockApi, type OHLCVBar } from "../api/stockApi";
import { useAppStore, type TimeInterval, type ChartType } from "../stores/appStore";
import { ChartToolbar } from "../components/ChartToolbar";
import { OrderBook } from "../components/OrderBook";
import { TimeAndSales } from "../components/TimeAndSales";
import { StockInfo } from "../components/StockInfo";
import { Header } from "../components/Header";
import { formatPrice, formatVolume, priceColor } from "../lib/utils";

// ── Chart Series IDs ──────────────────────────────────────────────────────────

interface SeriesSet {
  candle: ISeriesApi<"Candlestick"> | null;
  line: ISeriesApi<"Line"> | null;
  area: ISeriesApi<"Area"> | null;
  bar: ISeriesApi<"Bar"> | null;
  volume: ISeriesApi<"Histogram"> | null;
  ma5: ISeriesApi<"Line"> | null;
  ma10: ISeriesApi<"Line"> | null;
  ma20: ISeriesApi<"Line"> | null;
  ma50: ISeriesApi<"Line"> | null;
}

const CHART_COLORS = {
  bg: "#1f2937",
  text: "#9ca3af",
  grid: "#374151",
  border: "#4b5563",
  up: "#16a34a",
  down: "#dc2626",
  volume: "#3b82f6",
  ma5: "#f59e0b",
  ma10: "#3b82f6",
  ma20: "#8b5cf6",
  ma50: "#ec4899",
};

function calcMA(data: OHLCVBar[], period: number): LineData<Time>[] {
  const result: LineData<Time>[] = [];
  for (let i = period - 1; i < data.length; i++) {
    let sum = 0;
    for (let j = 0; j < period; j++) {
      sum += data[i - j].close;
    }
    result.push({
      time: (data[i].timestamp / 1000) as Time,
      value: sum / period,
    });
  }
  return result;
}

export function ChartPage() {
  const { symbol } = useParams<{ symbol: string }>();
  const navigate = useNavigate();
  const setSelectedSymbol = useAppStore((s) => s.setSelectedSymbol);

  // Sync URL symbol → store so OrderBook, StockInfo, TimeAndSales all work
  useEffect(() => {
    if (symbol) setSelectedSymbol(symbol);
  }, [symbol, setSelectedSymbol]);

  const {
    activeInterval,
    activeChartType,
    activeIndicators,
  } = useAppStore();

  // ── Data ───────────────────────────────────────────────────────────────────
  const { data: intraday = [], isLoading: intradayLoading } = useQuery({
    queryKey: ["ohlcv", symbol, activeInterval],
    queryFn: () => stockApi.getOHLCV(symbol!, activeInterval, 500).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 30_000,
  });

  const { data: daily = [] } = useQuery({
    queryKey: ["history", symbol, 90],
    queryFn: () => stockApi.getHistory(symbol!, 90).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 5 * 60_000,
  });

  // Use intraday for short intervals, daily for longer ones
  const chartData = ["1D", "1W", "1M"].includes(activeInterval) ? daily : intraday;

  // ── Chart refs ──────────────────────────────────────────────────────────────
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<SeriesSet>({
    candle: null, line: null, area: null, bar: null, volume: null,
    ma5: null, ma10: null, ma20: null, ma50: null,
  });

  // ── Init chart ──────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: CHART_COLORS.bg },
        textColor: CHART_COLORS.text,
      },
      grid: {
        vertLines: { color: CHART_COLORS.grid },
        horzLines: { color: CHART_COLORS.grid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: "#6b7280", labelBackgroundColor: "#374151" },
        horzLine: { color: "#6b7280", labelBackgroundColor: "#374151" },
      },
      timeScale: {
        borderColor: CHART_COLORS.border,
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: CHART_COLORS.border,
      },
      width: containerRef.current.clientWidth,
      height: containerRef.current.clientHeight,
    });

    chartRef.current = chart;

    // Volume series (always visible)
    const vol = chart.addHistogramSeries({
      color: CHART_COLORS.volume + "55",
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
    });
    vol.priceScale().applyOptions({
      scaleMargins: { top: 0.85, bottom: 0 },
    });

    // Candlestick series
    const candle = chart.addCandlestickSeries({
      upColor: CHART_COLORS.up,
      downColor: CHART_COLORS.down,
      borderUpColor: CHART_COLORS.up,
      borderDownColor: CHART_COLORS.down,
      wickUpColor: CHART_COLORS.up,
      wickDownColor: CHART_COLORS.down,
    });

    // Line series (hidden initially)
    const line = chart.addLineSeries({
      color: "#60a5fa",
      lineWidth: 2,
      visible: false,
    });

    // Area series (hidden initially)
    const area = chart.addAreaSeries({
      lineColor: "#34d399",
      topColor: "#34d39933",
      bottomColor: "#34d39900",
      visible: false,
    });

    // Bar series (hidden initially)
    const bar = chart.addBarSeries({
      upColor: CHART_COLORS.up,
      downColor: CHART_COLORS.down,
      visible: false,
    });

    // MA series (hidden initially)
    const ma5 = chart.addLineSeries({ color: CHART_COLORS.ma5, lineWidth: 1, visible: false });
    const ma10 = chart.addLineSeries({ color: CHART_COLORS.ma10, lineWidth: 1, visible: false });
    const ma20 = chart.addLineSeries({ color: CHART_COLORS.ma20, lineWidth: 1, visible: false });
    const ma50 = chart.addLineSeries({ color: CHART_COLORS.ma50, lineWidth: 1, visible: false });

    seriesRef.current = { candle, line, area, bar, volume: vol, ma5, ma10, ma20, ma50 };

    const handleResize = () => {
      if (containerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: containerRef.current.clientWidth,
          height: containerRef.current.clientHeight,
        });
      }
    };
    const observer = new ResizeObserver(handleResize);
    observer.observe(containerRef.current);

    return () => {
      observer.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = {
        candle: null, line: null, area: null, bar: null, volume: null,
        ma5: null, ma10: null, ma20: null, ma50: null,
      };
    };
  }, []);

  // ── Update data when chartData or chartType changes ─────────────────────────
  useEffect(() => {
    const s = seriesRef.current;
    if (!chartRef.current || !chartData.length) return;

    const tdata: CandlestickData<Time>[] = chartData.map((d) => ({
      time: (d.timestamp / 1000) as Time,
      open: d.open, high: d.high, low: d.low, close: d.close,
    }));

    const ldata: LineData<Time>[] = chartData.map((d) => ({
      time: (d.timestamp / 1000) as Time,
      value: d.close,
    }));

    const adata: LineData<Time>[] = chartData.map((d) => ({
      time: (d.timestamp / 1000) as Time,
      value: d.close,
    }));

    const bdata = chartData.map((d) => ({
      time: (d.timestamp / 1000) as Time,
      open: d.open, high: d.high, low: d.low, close: d.close,
    }));

    const vdata: HistogramData<Time>[] = chartData.map((d) => ({
      time: (d.timestamp / 1000) as Time,
      value: d.volume,
      color: d.close >= d.open ? CHART_COLORS.up + "55" : CHART_COLORS.down + "55",
    }));

    // Hide all, show active
    if (s.candle) s.candle.applyOptions({ visible: activeChartType === "candlestick" });
    if (s.line) s.line.applyOptions({ visible: activeChartType === "line" });
    if (s.area) s.area.applyOptions({ visible: activeChartType === "area" });
    if (s.bar) s.bar.applyOptions({ visible: activeChartType === "bar" });

    if (activeChartType === "candlestick" && s.candle) {
      s.candle.setData(tdata);
    } else if (activeChartType === "line" && s.line) {
      s.line.setData(ldata);
    } else if (activeChartType === "area" && s.area) {
      s.area.setData(adata);
    } else if (activeChartType === "bar" && s.bar) {
      s.bar.setData(bdata);
    } else if (s.candle) {
      // Default to candlestick if no type matches
      s.candle.setData(tdata);
    }

    // Volume always updated
    if (s.volume) s.volume.setData(vdata);

    // Indicators
    const inds = activeIndicators;
    if (s.ma5) s.ma5.applyOptions({ visible: inds.includes("MA5") });
    if (s.ma10) s.ma10.applyOptions({ visible: inds.includes("MA10") });
    if (s.ma20) s.ma20.applyOptions({ visible: inds.includes("MA20") });
    if (s.ma50) s.ma50.applyOptions({ visible: inds.includes("MA50") });

    if (inds.includes("MA5")) s.ma5!.setData(calcMA(chartData, 5));
    if (inds.includes("MA10")) s.ma10!.setData(calcMA(chartData, 10));
    if (inds.includes("MA20")) s.ma20!.setData(calcMA(chartData, 20));
    if (inds.includes("MA50")) s.ma50!.setData(calcMA(chartData, 50));

    chartRef.current.timeScale().fitContent();
  }, [chartData, activeChartType, activeIndicators]);

  // ── Handlers ────────────────────────────────────────────────────────────────
  const handleIntervalChange = useCallback((_interval: TimeInterval) => {
    // React Query will refetch via the queryKey change
  }, []);

  const handleChartTypeChange = useCallback((_type: ChartType) => {
    // Handled in useEffect via activeChartType
  }, []);

  const handleDrawingToolChange = useCallback((tool: string | null) => {
    if (tool) {
      console.info(`Drawing tool activated: ${tool} — click on chart to draw.`);
    }
  }, []);

  const handleIndicatorToggle = useCallback((_indicator: string) => {
    // Handled via activeIndicators in useEffect
  }, []);

  // ── Latest quote for the header ─────────────────────────────────────────────
  const { data: quote } = useQuery({
    queryKey: ["quote", symbol],
    queryFn: () => stockApi.getQuote(symbol!).then((r) => r.data),
    enabled: !!symbol,
    staleTime: 15_000,
  });

  if (!symbol) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-gray-900 text-white">
        <p>Không tìm thấy mã chứng khoán.</p>
        <button className="ml-4 text-blue-400 underline" onClick={() => navigate("/")}>
          Quay lại
        </button>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen flex-col bg-gray-900 text-gray-100">
      <Header />

      {/* Symbol title bar */}
      <div className="flex items-center gap-4 border-b border-gray-700 bg-gray-800 px-4 py-2">
        <button
          onClick={() => navigate(-1)}
          className="text-gray-400 hover:text-white"
          title="Quay lại"
        >
          ←
        </button>
        <h1 className="text-lg font-bold text-white">{symbol}</h1>
        {quote && (
          <>
            <span className={`text-lg font-bold ${priceColor(quote.change)}`}>
              {formatPrice(quote.last_price)}
            </span>
            <span className={`text-sm ${priceColor(quote.change)}`}>
              {quote.change > 0 ? "+" : ""}{formatPrice(quote.change)} ({quote.ratio_change.toFixed(2)}%)
            </span>
            <span className="ml-4 text-xs text-gray-400">
              KL: {formatVolume(quote.volume)} | Cao: {formatPrice(quote.highest)} | Thấp: {formatPrice(quote.lowest)}
            </span>
          </>
        )}
        {intradayLoading && <span className="ml-2 text-xs text-blue-400">...</span>}
      </div>

      {/* Full toolbar */}
      <ChartToolbar
        chartRef={chartRef}
        onIntervalChange={handleIntervalChange}
        onChartTypeChange={handleChartTypeChange}
        onDrawingToolChange={handleDrawingToolChange}
        onIndicatorToggle={handleIndicatorToggle}
        activeIndicators={activeIndicators}
      />

      {/* Main layout: chart + right panel */}
      <div className="flex flex-1 overflow-hidden">
        {/* Candlestick chart */}
        <div className="flex flex-1 flex-col overflow-hidden">
          <div ref={containerRef} className="flex-1 overflow-hidden" />
          {/* Time & Sales below chart */}
          <div className="h-48 flex-shrink-0 border-t border-gray-700">
            <div className="border-b border-gray-700 px-3 py-1.5 text-xs font-semibold text-gray-400">
              Time & Sales
            </div>
            <TimeAndSales />
          </div>
        </div>

        {/* Right panel */}
        <aside className="flex w-72 flex-shrink-0 flex-col gap-3 overflow-y-auto border-l border-gray-700 bg-gray-850 p-3">
          <StockInfo />
          <OrderBook />
        </aside>
      </div>
    </div>
  );
}
