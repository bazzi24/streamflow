import { useEffect, useRef } from "react";
import { createChart, IChartApi, ISeriesApi, CandlestickData, Time } from "lightweight-charts";
import { useAppStore } from "../stores/appStore";
import { useStockOHLCV } from "../hooks/useStockOHLCV";

export function PriceChart() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const { history, historyLoading } = useStockOHLCV(selectedSymbol, "5m", 30);

  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  // Init chart
  useEffect(() => {
    if (!containerRef.current) return;
    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: "#1f2937" },
        textColor: "#9ca3af",
      },
      grid: {
        vertLines: { color: "#374151" },
        horzLines: { color: "#374151" },
      },
      crosshair: {
        mode: 1,
      },
      timeScale: {
        borderColor: "#4b5563",
        timeVisible: true,
      },
      rightPriceScale: {
        borderColor: "#4b5563",
      },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#16a34a",
      downColor: "#dc2626",
      borderUpColor: "#16a34a",
      borderDownColor: "#dc2626",
      wickUpColor: "#16a34a",
      wickDownColor: "#dc2626",
    });

    const volSeries = chart.addHistogramSeries({
      color: "#3b82f6",
      priceFormat: { type: "volume" },
      priceScaleId: "",
    });
    volSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.85, bottom: 0 },
    });

    chartRef.current = chart;
    seriesRef.current = candleSeries;
    volSeriesRef.current = volSeries;

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener("resize", handleResize);
    handleResize();

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      volSeriesRef.current = null;
    };
  }, []);

  // Update data
  useEffect(() => {
    if (!seriesRef.current || !volSeriesRef.current) return;
    if (!history || history.length === 0) return;

    const candles: CandlestickData<Time>[] = history.map((bar) => ({
      time: (bar.timestamp / 1000) as Time,
      open: bar.open,
      high: bar.high,
      low: bar.low,
      close: bar.close,
    }));
    seriesRef.current.setData(candles);

    const volumes = history.map((bar) => ({
      time: (bar.timestamp / 1000) as Time,
      value: bar.volume,
      color: bar.close >= bar.open ? "rgba(22,163,74,0.5)" : "rgba(220,38,38,0.5)",
    }));
    volSeriesRef.current.setData(volumes);
  }, [history]);

  return (
    <div className="relative h-full w-full">
      {historyLoading && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-800/50">
          <span className="text-sm text-gray-400">Loading chart...</span>
        </div>
      )}
      <div ref={containerRef} className="h-full w-full" />
    </div>
  );
}
