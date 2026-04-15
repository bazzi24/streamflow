import { useQuery } from "@tanstack/react-query";
import { marketApi } from "../api/stockApi";
import { formatPrice } from "../lib/utils";
import styles from "./IndexCard.module.css";

export function IndexCard({ indexId, name }: { indexId?: string; name?: string }) {
  const { data } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    refetchInterval: 30_000,
  });

  const index = data?.indices?.find(
    (i: { index_id: string }) => !indexId || i.index_id === indexId
  );

  const isUp = (index?.ratio_change ?? 0) >= 0;

  return (
    <div className={styles.card}>
      <div className={styles.name}>
        {name ?? index?.index_name ?? "Market"}
      </div>
      <div className={styles.value}>
        {index?.index_value != null ? formatPrice(index.index_value) : "—"}
      </div>
      <div className={`${styles.change} ${isUp ? styles.up : styles.down}`}>
        <span className={styles.arrow}>{isUp ? "▲" : "▼"}</span>
        <span className={styles.pct}>
          {index?.ratio_change != null
            ? `${isUp ? "+" : ""}${index.ratio_change.toFixed(2)}%`
            : "—"}
        </span>
        <span className={styles.abs}>
          {index?.change != null
            ? `${isUp ? "+" : ""}${index.change.toFixed(2)}`
            : ""}
        </span>
      </div>
    </div>
  );
}
