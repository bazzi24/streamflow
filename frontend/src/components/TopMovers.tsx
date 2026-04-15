import { useQuery } from "@tanstack/react-query";
import { marketApi } from "../api/stockApi";
import type { StockSummary } from "../api/stockApi";
import { formatPrice } from "../lib/utils";
import styles from "./TopMovers.module.css";

export function TopMovers() {
  const { data, isLoading } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    refetchInterval: 30_000,
  });

  const gainers: StockSummary[] = data?.top_gainers ?? [];
  const losers: StockSummary[] = data?.top_losers ?? [];

  function MoverRow({ s, rank }: { s: StockSummary; rank: number }) {
    const up = s.ratio_change >= 0;
    return (
      <div className={`${styles.row} ${up ? "" : styles.down}`}>
        <span className={styles.rank}>{rank}</span>
        <span className={styles.sym}>{s.symbol}</span>
        <span className={styles.price}>
          {formatPrice(s.last_price)}
        </span>
        <span className={`${styles.chg} ${up ? styles.upVal : styles.downVal}`}>
          {up ? "+" : ""}
          {s.ratio_change.toFixed(2)}%
        </span>
      </div>
    );
  }

  return (
    <div className={styles.container}>
      <div className={styles.section}>
        <div className={styles.header}>
          <span className={`${styles.dot} ${styles.upDot}`}>▲</span>
          <span className={styles.title}>Top Gainers</span>
        </div>
        <div className={styles.table}>
          <div className={styles.thead}>
            <span>#</span>
            <span>Symbol</span>
            <span>Price</span>
            <span>Chg%</span>
          </div>
          {isLoading ? (
            <div className={styles.loading}>Loading…</div>
          ) : (
            gainers.slice(0, 8).map((s, i) => (
              <MoverRow key={s.symbol + i} s={s} rank={i + 1} />
            ))
          )}
        </div>
      </div>

      <div className={styles.section}>
        <div className={styles.header}>
          <span className={`${styles.dot} ${styles.downDot}`}>▼</span>
          <span className={styles.title}>Top Losers</span>
        </div>
        <div className={styles.table}>
          <div className={styles.thead}>
            <span>#</span>
            <span>Symbol</span>
            <span>Price</span>
            <span>Chg%</span>
          </div>
          {isLoading ? (
            <div className={styles.loading}>Loading…</div>
          ) : (
            losers.slice(0, 8).map((s, i) => (
              <MoverRow key={s.symbol + i} s={s} rank={i + 1} />
            ))
          )}
        </div>
      </div>
    </div>
  );
}
