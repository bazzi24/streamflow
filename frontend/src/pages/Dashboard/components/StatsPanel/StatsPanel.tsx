import { useQuery } from "@tanstack/react-query";
import { stockApi } from "../../../../api/stockApi";
import { useAppStore } from "../../../../stores/appStore";
import { formatPrice, formatVolume, formatChange } from "../../../../lib/utils";
import styles from "./StatsPanel.module.css";

export function StatsPanel() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const watchlistCryptoMode = useAppStore((s) => s.watchlistCryptoMode);

  const { data: quote, isLoading: quoteLoading } = useQuery({
    queryKey: ["quote", selectedSymbol],
    queryFn: () => stockApi.getQuote(selectedSymbol).then((r) => r.data),
    enabled: !!selectedSymbol && !watchlistCryptoMode,
    staleTime: 15_000,
  });

  const { data: meta, isLoading: metaLoading } = useQuery({
    queryKey: ["symbol", selectedSymbol],
    queryFn: () => stockApi.getSymbol(selectedSymbol).then((r) => r.data),
    enabled: !!selectedSymbol,
    staleTime: 5 * 60_000,
  });

  if (watchlistCryptoMode) {
    return (
      <div className={styles.statsPanel}>
        <div className={styles.panelHeader}>
          <span className={styles.panelTitle}>{selectedSymbol}</span>
        </div>
        <div className={styles.cryptoModeNote}>
          ⚡ Crypto — connect live feed for real-time data
        </div>
      </div>
    );
  }

  if (quoteLoading || metaLoading) return <StatsSkeleton />;
  if (!quote) return <StatsError />;

  const pct = quote.ratio_change;
  const changeDir = pct > 0 ? "up" : pct < 0 ? "down" : "neutral";

  // Position of last_price within ceiling/floor range (0–100%)
  const rangePos = (() => {
    const range = quote.ceiling - quote.floor;
    if (range === 0) return 50;
    return ((quote.last_price - quote.floor) / range) * 100;
  })();

  return (
    <div className={styles.statsPanel}>
      {/* Header */}
      <div className={styles.panelHeader}>
        <span className={styles.panelTitle}>{selectedSymbol}</span>
        {meta && (
          <span className={styles.panelSubtitle}>{meta.symbol_name}</span>
        )}
      </div>

      {/* Price hero */}
      <div className={styles.priceHero}>
        <div className={styles.heroPrice}>{formatPrice(quote.last_price)}</div>
        <div className={`${styles.heroChange} ${styles[changeDir]}`}>
          {formatChange(quote.change)} ({pct >= 0 ? "+" : ""}
          {pct.toFixed(2)}%)
        </div>
      </div>

      {/* Price stats */}
      <div className={styles.statsSection}>
        <div className={styles.statsSectionTitle}>Price</div>
        <div className={styles.statsGrid}>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Ref</span>
            <span className={styles.statValue}>{formatPrice(quote.ref_price)}</span>
          </div>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Ceiling</span>
            <span className={`${styles.statValue} ${styles.accent}`}>
              {formatPrice(quote.ceiling)}
            </span>
          </div>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Value</span>
            <span className={styles.statValue}>{formatVolume(quote.value)}</span>
          </div>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Floor</span>
            <span className={`${styles.statValue} ${styles.down}`}>
              {formatPrice(quote.floor)}
            </span>
          </div>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>High</span>
            <span className={`${styles.statValue} ${styles.up}`}>
              {formatPrice(quote.highest)}
            </span>
          </div>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Low</span>
            <span className={`${styles.statValue} ${styles.down}`}>
              {formatPrice(quote.lowest)}
            </span>
          </div>
        </div>

        {/* Ceiling/Floor position bar */}
        <div className={styles.ceilingFloorBar}>
          <div className={styles.cfBarLabel}>
            <span style={{ color: "#ff3d57" }}>Floor</span>
            <span style={{ color: "#00e676" }}>Ceiling</span>
          </div>
          <div className={styles.cfBarTrack}>
            <div className={styles.cfBarRange}>
              <div className={styles.cfBarFloor} />
              <div className={styles.cfBarCeiling} />
            </div>
            <div
              className={styles.cfBarCurrent}
              style={{
                left: `${Math.max(0, Math.min(100, rangePos))}%`,
                background: pct >= 0 ? "#00e676" : "#ff3d57",
              }}
            />
          </div>
        </div>
      </div>

      {/* Volume stats */}
      <div className={styles.statsSection}>
        <div className={styles.statsSectionTitle}>Volume / Value</div>
        <div className={styles.statsGrid}>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Volume</span>
            <span className={styles.statValue}>{formatVolume(quote.volume)}</span>
          </div>
          <div className={styles.statRow}>
            <span className={styles.statLabel}>Value</span>
            <span className={styles.statValue}>{formatVolume(quote.value)}</span>
          </div>
        </div>
      </div>

      {/* Meta */}
      {meta && (
        <div className={styles.statsSection}>
          <div className={styles.statsSectionTitle}>Info</div>
          <div className={styles.statsGrid}>
            {meta.sector && (
              <div className={styles.statRow}>
                <span className={styles.statLabel}>Sector</span>
                <span className={styles.statValue}>{meta.sector}</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Loading / Error ────────────────────────────────────────────────────────

function StatsSkeleton() {
  return (
    <div className={styles.statsPanel}>
      <div className={styles.panelHeader}>
        <span className={styles.panelTitle}>Stats</span>
      </div>
      <div className={styles.skeleton}>
        <div className={styles.skeletonLine} style={{ width: "60%", height: 20 }} />
        <div className={styles.skeletonLine} style={{ width: "80%", height: 12, marginBottom: 16 }} />
        {[1, 2, 3, 4, 5, 6].map((i) => (
          <div key={i} className={styles.skeletonLine} style={{ width: "90%" }} />
        ))}
      </div>
    </div>
  );
}

function StatsError() {
  return (
    <div className={styles.statsPanel}>
      <div className={styles.panelHeader}>
        <span className={styles.panelTitle}>Stats</span>
      </div>
      <div className={styles.errorState}>
        <span className={styles.errorIcon}>⚠</span>
        <span className={styles.errorMsg}>Failed to load stats</span>
      </div>
    </div>
  );
}
