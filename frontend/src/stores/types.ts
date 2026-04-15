// ── Store-shared types ────────────────────────────────────────────────────────

export type MarketSegment = "ALL" | "HOSE" | "HNX" | "VN30" | "HNX30" | "UPCOM" | "ETF" | "DERIVATIVE" | "WARRANT";
export type ChartType = "candlestick" | "line" | "area" | "bar";
export type DrawingTool = "trend" | "horizontal" | "fib" | "vertical" | "channel" | "gann" | "srl";
export type TimeInterval =
  | "1m" | "5m" | "15m" | "30m"
  | "1h" | "2h" | "4h"
  | "1D" | "1W" | "1M";
export type MarketVizMode = "heatmap" | "breath";
export type HeatmapExchange = "ALL" | "HOSE" | "HNX" | "VN30" | "HNX30" | "UPCOM" | "ETF" | "WARRANT";

/** Warrant symbols are >3 characters (e.g. CACB2501, CVNM2503). */
export function isWarrant(symbol: string): boolean {
  return symbol.length > 3;
}

/** ETF symbols follow known patterns (VF, E1,...) */
export const ETF_PREFIXES = new Set(["VF", "E1", "SSIAM", "VOF", "VFA", "VCA", "VNZ", "VIBF"]);
export function isETF(symbol: string): boolean {
  return ETF_PREFIXES.has(symbol.slice(0, 2)) || ETF_PREFIXES.has(symbol.slice(0, 5));
}
