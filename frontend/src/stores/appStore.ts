import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { UserResponse } from "../api/stockApi";
import type {
  MarketSegment,
  ChartType,
  DrawingTool,
  TimeInterval,
  MarketVizMode,
  HeatmapExchange,
} from "./types";

// ── Types re-export ─────────────────────────────────────────────────────────

export type { MarketSegment, ChartType, DrawingTool, TimeInterval };
export type { MarketVizMode, HeatmapExchange };

// ── State ───────────────────────────────────────────────────────────────────

interface AppState {
  // Auth
  user: UserResponse | null;
  token: string | null;
  setAuth: (token: string, user: UserResponse) => void;
  clearAuth: () => void;

  // Watchlist
  watchlist: string[];
  setWatchlist: (symbols: string[]) => void;

  // Selected symbol — THE core synchronized state
  selectedSymbol: string;
  setSelectedSymbol: (symbol: string) => void;

  // Theme
  theme: "light" | "dark";
  toggleTheme: () => void;

  // Market segment (for Home/Markets pages)
  activeSegment: MarketSegment;
  setActiveSegment: (segment: MarketSegment) => void;

  // Chart toolbar state
  activeInterval: TimeInterval;
  setActiveInterval: (interval: TimeInterval) => void;
  activeChartType: ChartType;
  setActiveChartType: (chartType: ChartType) => void;
  activeDrawingTool: DrawingTool | null;
  setActiveDrawingTool: (tool: DrawingTool | null) => void;
  activeIndicators: string[];
  toggleIndicator: (indicator: string) => void;

  // ── NEW Dashboard slices ─────────────────────────────────────────────────

  /** Market visualization mode — heatmap vs breath */
  marketVizMode: MarketVizMode;
  setMarketVizMode: (mode: MarketVizMode) => void;

  /** Exchange filter for heatmap */
  heatmapExchange: HeatmapExchange;
  setHeatmapExchange: (ex: HeatmapExchange) => void;

  /** Watchlist panel collapsed/expanded */
  watchlistExpanded: boolean;
  setWatchlistExpanded: (v: boolean) => void;

  /** Right sidebar active tab */
  rightTab: "stats" | "orderbook";
  setRightTab: (tab: "stats" | "orderbook") => void;

  /** Whether watchlist is in crypto mode */
  watchlistCryptoMode: boolean;
  setWatchlistCryptoMode: (v: boolean) => void;
}

export const useAppStore = create<AppState>()(
  persist(
    (set) => ({
      user: null,
      token: null,
      setAuth: (token, user) => set({ token, user }),
      clearAuth: () => set({ token: null, user: null, watchlist: [] }),

      watchlist: [],
      setWatchlist: (symbols) => set({ watchlist: symbols }),

      selectedSymbol: "VND",
      setSelectedSymbol: (symbol) => set({ selectedSymbol: symbol }),

      theme: "dark",
      toggleTheme: () =>
        set((s) => ({ theme: s.theme === "dark" ? "light" : "dark" })),

      activeSegment: "ALL",
      setActiveSegment: (segment) => set({ activeSegment: segment }),

      activeInterval: "5m",
      setActiveInterval: (interval) => set({ activeInterval: interval }),
      activeChartType: "candlestick",
      setActiveChartType: (chartType) => set({ activeChartType: chartType }),
      activeDrawingTool: null,
      setActiveDrawingTool: (tool) => set({ activeDrawingTool: tool }),
      activeIndicators: [],
      toggleIndicator: (indicator) =>
        set((s) => ({
          activeIndicators: s.activeIndicators.includes(indicator)
            ? s.activeIndicators.filter((i) => i !== indicator)
            : [...s.activeIndicators, indicator],
        })),

      // ── NEW Dashboard slices ──────────────────────────────────────────────
      marketVizMode: "heatmap",
      setMarketVizMode: (mode) => set({ marketVizMode: mode }),

      heatmapExchange: "HOSE",
      setHeatmapExchange: (ex) => set({ heatmapExchange: ex }),

      watchlistExpanded: true,
      setWatchlistExpanded: (v) => set({ watchlistExpanded: v }),

      rightTab: "stats",
      setRightTab: (tab) => set({ rightTab: tab }),

      watchlistCryptoMode: false,
      setWatchlistCryptoMode: (v) => set({ watchlistCryptoMode: v }),
    }),
    {
      name: "streamflow-store",
      partialize: (state) => ({
        token: state.token,
        user: state.user,
        watchlist: state.watchlist,
        selectedSymbol: state.selectedSymbol,
        theme: state.theme,
        activeSegment: state.activeSegment,
        activeInterval: state.activeInterval,
        activeChartType: state.activeChartType,
        activeIndicators: state.activeIndicators,
        marketVizMode: state.marketVizMode,
        heatmapExchange: state.heatmapExchange,
        watchlistExpanded: state.watchlistExpanded,
      }),
    }
  )
);

// ── Crypto detection ───────────────────────────────────────────────────────

export const CRYPTO_SYMBOLS = new Set(["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "DOT"]);

export function isCrypto(symbol: string): boolean {
  return CRYPTO_SYMBOLS.has(symbol.toUpperCase());
}
