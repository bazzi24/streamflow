import { useQuery } from "@tanstack/react-query";
import { useAppStore } from "../../stores/appStore";
import { marketApi } from "../../api/stockApi";
import { formatPrice } from "../../lib/utils";
import { WatchlistPanel } from "./components/WatchlistPanel/WatchlistPanel";
import { MainChart } from "./components/MainChart/MainChart";
import { MarketHeatmap } from "./components/MarketHeatmap/MarketHeatmap";
import { MarketBreath } from "./components/MarketBreath/MarketBreath";
import { StatsPanel } from "./components/StatsPanel/StatsPanel";
import { OrderBookPanel } from "./components/OrderBookPanel/OrderBookPanel";
import { useSynchronizedView } from "./hooks/useSynchronizedView";
import styles from "./DashboardPage.module.css";

// ── Market index chips in header ──────────────────────────────────────────────

function MarketIndexChips() {
  const { data } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    staleTime: 15_000,
  });

  if (!data?.indices) return null;

  return (
    <div className={styles.headerMarketIndices}>
      {data.indices.slice(0, 6).map((idx) => (
        <div key={idx.index_id} className={styles.marketIndexChip}>
          <span className={styles.indexName}>{idx.index_name}</span>
          <span className={styles.indexValue}>{formatPrice(idx.index_value)}</span>
          <span
            className={`${styles.indexChange} ${
              idx.ratio_change >= 0 ? styles.positive : styles.negative
            }`}
          >
            {idx.ratio_change >= 0 ? "▲" : "▼"}{" "}
            {idx.ratio_change >= 0 ? "+" : ""}
            {idx.ratio_change.toFixed(2)}%
          </span>
        </div>
      ))}
    </div>
  );
}

// ── Right sidebar with tabs ────────────────────────────────────────────────────

function RightSidebar() {
  const rightTab = useAppStore((s) => s.rightTab);
  const setRightTab = useAppStore((s) => s.setRightTab);

  return (
    <div className={styles.rightSidebar}>
      <div className={styles.panelTabs}>
        <button
          className={`${styles.panelTab} ${rightTab === "stats" ? styles.active : ""}`}
          onClick={() => setRightTab("stats")}
        >
          Stats
        </button>
        <button
          className={`${styles.panelTab} ${rightTab === "orderbook" ? styles.active : ""}`}
          onClick={() => setRightTab("orderbook")}
        >
          Depth
        </button>
      </div>
      <div className={styles.rightPanelBody}>
        {rightTab === "stats" ? <StatsPanel /> : <OrderBookPanel />}
      </div>
    </div>
  );
}

// ── Dashboard ─────────────────────────────────────────────────────────────────

export function DashboardPage() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const marketVizMode = useAppStore((s) => s.marketVizMode);
  const setMarketVizMode = useAppStore((s) => s.setMarketVizMode);

  // Activate synchronized view — WebSocket subscriptions + cache coordination
  useSynchronizedView();

  return (
    <div className={styles.dashboardRoot}>
      {/* ── Header Bar ────────────────────────────────────────────────────── */}
      <header className={styles.headerBar}>
        {/* Logo */}
        <div className={styles.logo}>
          <svg className={styles.logoIcon} viewBox="0 0 24 24" fill="none">
            <polyline
              points="22 12 18 12 15 21 9 3 6 12 2 12"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          SF StreamFlow
        </div>

        <div className={styles.headerDivider} />

        {/* Live market indices */}
        <MarketIndexChips />

        <div className={styles.headerDivider} />

        {/* Active symbol chip */}
        <div className={styles.selectedSymbolChip}>
          <span className={styles.selectedSymbolLabel}>Charting</span>
          <span className={styles.selectedSymbolValue}>{selectedSymbol}</span>
        </div>

        {/* Heatmap / Breath toggle */}
        <div className={styles.vizToggle}>
          <button
            className={`${styles.vizToggleBtn} ${marketVizMode === "heatmap" ? styles.active : ""}`}
            onClick={() => setMarketVizMode("heatmap")}
          >
            Heatmap
          </button>
          <button
            className={`${styles.vizToggleBtn} ${marketVizMode === "breath" ? styles.active : ""}`}
            onClick={() => setMarketVizMode("breath")}
          >
            Breath
          </button>
        </div>

        {/* Live connection dot */}
        <div className={styles.connectionDot} title="Live connection active" />
      </header>

      {/* ── Main CSS Grid ─────────────────────────────────────────────────── */}
      <div className={styles.mainGrid}>
        {/* Left: Watchlist */}
        <div className={`${styles.panel} ${styles.watchlistPanel}`}>
          <div className={styles.panelHeader}>
            <span className={styles.panelTitle}>Watchlist</span>
          </div>
          <div className={styles.panelBody}>
            <WatchlistPanel />
          </div>
        </div>

        {/* Center: Main Chart */}
        <div className={`${styles.panel} ${styles.chartPanel}`}>
          <div className={styles.panelBody}>
            <MainChart />
          </div>
        </div>

        {/* Center-bottom: Market Viz (Heatmap / Breath) */}
        <div className={`${styles.panel} ${styles.marketVizPanel}`}>
          <div className={styles.panelBody}>
            {marketVizMode === "heatmap" ? (
              <MarketHeatmap />
            ) : (
              <MarketBreath />
            )}
          </div>
        </div>

        {/* Right: Stats / OrderBook */}
        <div className={`${styles.panel} ${styles.rightPanel}`}>
          <RightSidebar />
        </div>
      </div>
    </div>
  );
}
