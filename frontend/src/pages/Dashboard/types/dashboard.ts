// ── Shared types for Dashboard ─────────────────────────────────────────────

export interface HeatmapCell {
  symbol: string;
  symbol_name: string;
  exchange: string;
  last_price: number;
  change: number;
  ratio_change: number;
  volume: number;
  sector: string | null;
  /** rgba string computed from ratio_change intensity */
  bgColor: string;
  /** log-scaled cell size */
  cellSize: number;
  /** 0–1 opacity for color intensity */
  intensity: number;
}

export interface BreathIndex {
  index_id: string;
  index_name: string;
  index_value: number;
  change: number;
  ratio_change: number;
  advances: number;
  declines: number;
  nochanges: number;
  /** advances / (declines || 1) */
  adRatio: number;
  /** advances / (advances + declines + nochanges) — 0 to 1 */
  advancePct: number;
  /** McClellan oscillator: (A-D)/(A+D) * 100 */
  mcClellan: number;
}

export interface DashboardLayoutItem {
  i: string; // key
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
  maxW?: number;
  maxH?: number;
}

export type MarketVizMode = "heatmap" | "breath";
export type HeatmapExchange = "ALL" | "HOSE" | "HNX" | "VN30" | "HNX30" | "UPCOM";
