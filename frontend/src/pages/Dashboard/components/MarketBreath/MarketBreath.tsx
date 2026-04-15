import { useMarketBreath } from "../../hooks/useMarketBreath";
import styles from "./MarketBreath.module.css";

export function MarketBreath() {
  const { indices, isLoading } = useMarketBreath();

  if (isLoading) return <BreathSkeleton />;

  if (!indices.length) {
    return (
      <div className={styles.breathPanel}>
        <div className={styles.breathHeader}>
          <span className={styles.panelTitle}>Market Breadth</span>
        </div>
        <div className={styles.emptyState}>No index data available</div>
      </div>
    );
  }

  return (
    <div className={styles.breathPanel}>
      <div className={styles.breathHeader}>
        <span className={styles.panelTitle}>Market Breadth</span>
      </div>
      <div className={styles.breathList}>
        {indices.map((idx) => {
          const total = idx.advances + idx.declines + idx.nochanges;
          const advPct = total > 0 ? (idx.advances / total) * 100 : 50;

          return (
            <div key={idx.index_id} className={styles.breathRow}>
              <div className={styles.breathRowHeader}>
                <span className={styles.breathIndexName}>{idx.index_name}</span>
                <div className={styles.breathCounts}>
                  <span className={styles.breathAdvances}>
                    {idx.advances}↑
                  </span>
                  <span className={styles.breathSeparator}>/</span>
                  <span className={styles.breathDeclines}>
                    {idx.declines}↓
                  </span>
                </div>
              </div>

              {/* Progress bar */}
              <div className={styles.breathBarContainer}>
                <div
                  className={styles.breathBarAdvance}
                  style={{ width: `${advPct}%` }}
                />
                <div className={styles.breathMidLine} />
              </div>

              {/* A/D ratio row */}
              <div className={styles.adRatioRow}>
                <span className={styles.adRatioLabel}>
                  A/D · {idx.adRatio.toFixed(2)}
                </span>
                <div className={styles.adRatioBar}>
                  <div
                    className={styles.adRatioBarFill}
                    style={{ width: `${Math.min(advPct, 100)}%` }}
                  />
                </div>
              </div>
            </div>
          );
        })}

        {/* McClellan oscillator summary */}
        <div className={styles.mcSection}>
          <div className={styles.mcSectionTitle}>McClellan Oscillator</div>
          {indices.slice(0, 3).map((idx) => (
            <div key={`mc-${idx.index_id}`} className={styles.mcRow}>
              <span className={styles.mcName}>{idx.index_name}</span>
              <span
                className={`${styles.mcValue} ${
                  idx.mcClellan >= 0 ? styles.mcUp : styles.mcDown
                }`}
              >
                {idx.mcClellan >= 0 ? "+" : ""}
                {idx.mcClellan.toFixed(1)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function BreathSkeleton() {
  return (
    <div className={styles.breathPanel}>
      <div className={styles.breathHeader}>
        <span className={styles.panelTitle}>Market Breadth</span>
      </div>
      <div className={styles.breathList}>
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className={styles.breathRow}>
            <div className={styles.skeleton} style={{ width: "60%", height: 10, marginBottom: 6 }} />
            <div className={styles.skeleton} style={{ width: "100%", height: 14, borderRadius: 3 }} />
            <div className={styles.skeleton} style={{ width: "40%", height: 8, marginTop: 4 }} />
          </div>
        ))}
      </div>
    </div>
  );
}
