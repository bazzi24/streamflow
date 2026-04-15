import { useEffect, useRef, useState } from "react";
import { marketApi } from "../api/stockApi";
import { formatPrice } from "../lib/utils";
import styles from "./IndexTickerTape.module.css";

interface IndexItem {
  index_id: string;
  index_name: string;
  index_value: number;
  change: number;
  ratio_change: number;
  advances?: number;
  declines?: number;
}

export function IndexTickerTape() {
  const [indices, setIndices] = useState<IndexItem[]>([]);
  const tapeRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    marketApi.getOverview().then((r) => {
      setIndices(r.data.indices ?? []);
    });
  }, []);

  return (
    <div className={styles.tape}>
      <div className={styles.track} ref={tapeRef}>
        {[...indices, ...indices].map((idx, i) => {
          const up = idx.ratio_change >= 0;
          return (
            <div key={`${idx.index_id}-${i}`} className={styles.item}>
              <span className={styles.itemName}>{idx.index_name}</span>
              <span className={styles.itemValue}>
                {formatPrice(idx.index_value)}
              </span>
              <span className={`${styles.itemChange} ${up ? styles.up : styles.down}`}>
                {up ? "+" : ""}
                {idx.ratio_change?.toFixed(2)}%
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
