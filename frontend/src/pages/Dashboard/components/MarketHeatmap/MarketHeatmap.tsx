import { useState, useMemo, useCallback } from "react";
import { useMarketHeatmap } from "../../hooks/useMarketHeatmap";
import { useAppStore } from "../../../../stores/appStore";
import { formatPrice, formatVolume } from "../../../../lib/utils";
import styles from "./MarketHeatmap.module.css";

// ── Types ──────────────────────────────────────────────────────────────────

interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  symbol: string;
  price: number;
  pct: number;
  volume: number;
}

// ── Exchange options ────────────────────────────────────────────────────────

const EXCHANGES = [
  { label: "ALL", value: "ALL" },
  { label: "HOSE", value: "HOSE" },
  { label: "HNX", value: "HNX" },
  { label: "VN30", value: "VN30" },
  { label: "HNX30", value: "HNX30" },
  { label: "UPCOM", value: "UPCOM" },
];

// ── Component ────────────────────────────────────────────────────────────────

export function MarketHeatmap() {
  const { cells, isLoading, totalCount } = useMarketHeatmap();
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const setSelectedSymbol = useAppStore((s) => s.setSelectedSymbol);
  const heatmapExchange = useAppStore((s) => s.heatmapExchange);
  const setHeatmapExchange = useAppStore((s) => s.setHeatmapExchange);

  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false, x: 0, y: 0, symbol: "", price: 0, pct: 0, volume: 0,
  });

  const handleMouseEnter = useCallback(
    (e: React.MouseEvent, cell: (typeof cells)[0]) => {
      setTooltip({
        visible: true,
        x: e.clientX + 12,
        y: e.clientY - 40,
        symbol: cell.symbol,
        price: cell.last_price,
        pct: cell.ratio_change,
        volume: cell.volume,
      });
    },
    []
  );

  const handleMouseLeave = useCallback(() => {
    setTooltip((t) => ({ ...t, visible: false }));
  }, []);

  // Limit display to top 100 by volume for performance
  const displayCells = useMemo(() => {
    return [...cells]
      .sort((a, b) => b.volume - a.volume)
      .slice(0, 120);
  }, [cells]);

  return (
    <div className={styles.heatmapPanel}>
      {/* Controls */}
      <div className={styles.heatmapControls}>
        <span className={styles.heatmapTitle}>Heatmap</span>
        <div className={styles.toolbarDivider} />
        <select
          className={styles.exchangeSelect}
          value={heatmapExchange}
          onChange={(e) => setHeatmapExchange(e.target.value as typeof heatmapExchange)}
        >
          {EXCHANGES.map((ex) => (
            <option key={ex.value} value={ex.value}>{ex.label}</option>
          ))}
        </select>
        <span className={styles.cellCount}>{totalCount} symbols</span>
      </div>

      {/* Grid */}
      <div className={styles.heatmapGrid}>
        {isLoading ? (
          <HeatmapSkeleton />
        ) : displayCells.length === 0 ? (
          <div className={styles.emptyState}>No market data available</div>
        ) : (
          displayCells.map((cell) => {
            const isSelected = selectedSymbol === cell.symbol;
            return (
              <div
                key={cell.symbol}
                className={`${styles.heatmapCell} ${isSelected ? styles.selected : ""}`}
                style={{
                  background: cell.bgColor,
                  minWidth: `${Math.round(cell.cellSize * 0.9)}px`,
                  minHeight: `${Math.round(cell.cellSize * 0.6)}px`,
                }}
                onClick={() => setSelectedSymbol(cell.symbol)}
                onMouseEnter={(e) => handleMouseEnter(e, cell)}
                onMouseLeave={handleMouseLeave}
              >
                <span className={styles.heatmapSymbol}>{cell.symbol}</span>
                <span className={styles.heatmapPct}>
                  {cell.ratio_change >= 0 ? "+" : ""}
                  {cell.ratio_change.toFixed(1)}%
                </span>
              </div>
            );
          })
        )}
      </div>

      {/* Tooltip */}
      {tooltip.visible && (
        <div
          className={styles.heatmapTooltip}
          style={{ left: tooltip.x, top: tooltip.y }}
        >
          <div className={styles.tooltipRow}>
            <span className={styles.tooltipLabel}>Symbol</span>
            <span className={styles.tooltipValue}>{tooltip.symbol}</span>
          </div>
          <div className={styles.tooltipRow}>
            <span className={styles.tooltipLabel}>Price</span>
            <span className={styles.tooltipValue}>{formatPrice(tooltip.price)}</span>
          </div>
          <div className={styles.tooltipRow}>
            <span className={styles.tooltipLabel}>Change</span>
            <span
              className={styles.tooltipValue}
              style={{ color: tooltip.pct >= 0 ? "#00e676" : "#ff3d57" }}
            >
              {tooltip.pct >= 0 ? "+" : ""}
              {tooltip.pct.toFixed(2)}%
            </span>
          </div>
          <div className={styles.tooltipRow}>
            <span className={styles.tooltipLabel}>Volume</span>
            <span className={styles.tooltipValue}>{formatVolume(tooltip.volume)}</span>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Loading skeleton ────────────────────────────────────────────────────────

function HeatmapSkeleton() {
  return (
    <div className={styles.heatmapGrid}>
      {Array.from({ length: 40 }).map((_, i) => (
        <div key={i} className={styles.skeletonCell} />
      ))}
    </div>
  );
}
