import { useState, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { watchlistApi, type StockSummary } from "../api/stockApi";
import { useAppStore } from "../stores/appStore";
import { formatPrice, formatVolume } from "../lib/utils";
import styles from "./StockGrid.module.css";

type SortKey = keyof StockSummary;
type SortDir = "asc" | "desc";

interface StockGridProps {
  symbols: StockSummary[];
  showExchange?: boolean;
  title?: string;
}

export function StockGrid({ symbols, showExchange = false, title }: StockGridProps) {
  const navigate = useNavigate();
  const token = useAppStore((s) => s.token);
  const queryClient = useQueryClient();
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const { data: watchlist = [] } = useQuery({
    queryKey: ["watchlist"],
    queryFn: () => watchlistApi.getWatchlist().then((r) => r.data.symbols as string[]),
    enabled: !!token,
  });

  const toggleMutation = useMutation({
    mutationFn: async (symbol: string) => {
      const current = watchlist as string[];
      const isFav = current.includes(symbol);
      const updated = isFav
        ? current.filter((s) => s !== symbol)
        : [...current, symbol];
      await watchlistApi.updateWatchlist(updated);
      return { symbol, isFav: !isFav };
    },
    onSuccess: ({ symbol, isFav }) => {
      queryClient.setQueryData(["watchlist"], (old: string[] | undefined) =>
        isFav ? [...(old ?? []), symbol] : (old ?? []).filter((s) => s !== symbol)
      );
    },
  });

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  }

  const sorted = useMemo(() => {
    return [...symbols].sort((a, b) => {
      const av = a[sortKey as keyof StockSummary];
      const bv = b[sortKey as keyof StockSummary];
      if (typeof av === "string" && typeof bv === "string") {
        return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      }
      if (typeof av === "number" && typeof bv === "number") {
        return sortDir === "asc" ? av - bv : bv - av;
      }
      return 0;
    });
  }, [symbols, sortKey, sortDir]);

  function handleRowClick(sym: string) {
    navigate(`/chart/${sym}`);
  }

  const cols: { key: SortKey; label: string; align?: "right" }[] = [
    { key: "symbol", label: "Mã CK" },
    ...(showExchange ? [{ key: "exchange" as SortKey, label: "Sàn" }] : []),
    { key: "ceiling", label: "Trần", align: "right" },
    { key: "ref_price", label: "TC", align: "right" },
    { key: "floor", label: "Sàn", align: "right" },
    { key: "best_bid_vol", label: "KL Mua", align: "right" },
    { key: "best_bid_price", label: "Giá Mua", align: "right" },
    { key: "best_ask_price", label: "Giá Bán", align: "right" },
    { key: "best_ask_vol", label: "KL Bán", align: "right" },
    { key: "matched_price", label: "Khớp Lệnh", align: "right" },
    { key: "last_price", label: "Giá", align: "right" },
    { key: "change", label: "+/-", align: "right" },
    { key: "ratio_change", label: "%", align: "right" },
    { key: "volume", label: "KL", align: "right" },
  ];

  return (
    <div className={styles.container}>
      <div className={styles.toolbar}>
        {title && <h2 className={styles.sectionTitle}>{title}</h2>}
        <span className={styles.count}>{sorted.length} mã</span>
      </div>

      <div className={styles.tableWrap}>
        <table className={styles.table}>
          <thead>
            <tr>
              {cols.map((col) => {
                const isActive = sortKey === col.key;
                return (
                  <th
                    key={col.key}
                    onClick={() => handleSort(col.key)}
                    className={[
                      styles.th,
                      col.align === "right" ? styles.alignRight : "",
                      styles.sortable,
                      isActive ? styles.sortActive : "",
                    ].join(" ")}
                  >
                    {col.label}
                    {isActive && (
                      <span className={styles.sortIcon}>
                        {sortDir === "asc" ? "▲" : "▼"}
                      </span>
                    )}
                  </th>
                );
              })}
              {token && <th className={styles.th} style={{ width: 40 }} />}
            </tr>
          </thead>
          <tbody>
            {sorted.length === 0 && (
              <tr>
                <td colSpan={cols.length + (token ? 1 : 0)} className={styles.empty}>
                  Không có dữ liệu
                </td>
              </tr>
            )}
            {sorted.map((stock) => {
              const isUp = stock.change > 0;
              const isDown = stock.change < 0;
              const isFav = (watchlist as string[]).includes(stock.symbol);

              return (
                <tr key={stock.symbol}>
                  <td className={styles.sym} onClick={() => handleRowClick(stock.symbol)}>
                    {stock.symbol}
                    {showExchange && stock.exchange && (
                      <span className={styles.exch}>{stock.exchange}</span>
                    )}
                  </td>
                  {showExchange && (
                    <td className={styles.cellVal}>{stock.exchange || "—"}</td>
                  )}
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatPrice(stock.ceiling)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatPrice(stock.ref_price)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatPrice(stock.floor)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatVolume(stock.best_bid_vol)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight} ${styles.up}`}>{formatPrice(stock.best_bid_price)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight} ${styles.down}`}>{formatPrice(stock.best_ask_price)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatVolume(stock.best_ask_vol)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatPrice(stock.matched_price)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatPrice(stock.last_price)}</td>
                  <td className={`${styles.cellVal} ${styles.alignRight} ${isUp ? styles.up : isDown ? styles.down : styles.neutral}`}>
                    {formatPrice(stock.change)}
                  </td>
                  <td className={`${styles.cellVal} ${styles.alignRight} ${isUp ? styles.up : isDown ? styles.down : styles.neutral}`}>
                    {stock.ratio_change > 0 ? "+" : ""}{stock.ratio_change.toFixed(2)}%
                  </td>
                  <td className={`${styles.cellVal} ${styles.alignRight}`}>{formatVolume(stock.volume)}</td>
                  {token && (
                    <td onClick={(e) => e.stopPropagation()}>
                      <button
                        className={`${styles.favBtn} ${isFav ? styles.favActive : styles.favInactive}`}
                        onClick={() => toggleMutation.mutate(stock.symbol)}
                        title={isFav ? "Xóa khỏi yêu thích" : "Thêm vào yêu thích"}
                      >
                        {isFav ? "★" : "☆"}
                      </button>
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
