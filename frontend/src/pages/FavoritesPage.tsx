import { useState, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { useQuery, useQueries, useQueryClient } from "@tanstack/react-query";
import { stockApi, watchlistApi, type StockSummary } from "../api/stockApi";
import { useAppStore } from "../stores/appStore";
import { Header } from "../components/Header";
import { formatPrice, formatVolume } from "../lib/utils";
import styles from "./FavoritesPage.module.css";

type SortKey = keyof StockSummary;
type SortDir = "asc" | "desc";

function FavoritesTable() {
  const navigate = useNavigate();
  const token = useAppStore((s) => s.token);
  const queryClient = useQueryClient();
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const { data: watchlist = [], isLoading: wlLoading } = useQuery({
    queryKey: ["watchlist"],
    queryFn: () => watchlistApi.getWatchlist().then((r) => r.data.symbols as string[]),
    enabled: !!token,
  });

  const stockQueries = useQueries({
    queries: watchlist.map((sym: string) => ({
      queryKey: ["quote", sym],
      queryFn: () => stockApi.listStocks().then((r) => r.data.find((s: StockSummary) => s.symbol === sym)),
      enabled: !!token && watchlist.length > 0,
      refetchInterval: 30_000,
    })),
  });

  const stocks: StockSummary[] = stockQueries
    .map((q) => q.data)
    .filter((s): s is StockSummary => !!s);

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  }

  const sorted = useMemo(() => {
    return [...stocks].sort((a, b) => {
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
  }, [stocks, sortKey, sortDir]);

  async function handleRemove(symbol: string) {
    if (!token) return;
    const updated = (watchlist as string[]).filter((s: string) => s !== symbol);
    await watchlistApi.updateWatchlist(updated);
    queryClient.setQueryData(["watchlist"], updated);
  }

  if (!token) {
    return (
      <div className={styles.emptyState}>
        <span className={styles.emptyIcon}>🔒</span>
        <p className={styles.emptyTitle}>Vui lòng đăng nhập</p>
        <p className={styles.emptyDesc}>Đăng nhập để xem danh mục yêu thích của bạn.</p>
      </div>
    );
  }

  if (wlLoading || stockQueries.some((q) => q.isLoading)) {
    return (
      <div className={styles.emptyState}>
        <span className={styles.emptyIcon}>⏳</span>
        <p className={styles.emptyText}>Đang tải...</p>
      </div>
    );
  }

  if (watchlist.length === 0) {
    return (
      <div className={styles.emptyState}>
        <span className={styles.emptyIcon}>⭐</span>
        <p className={styles.emptyTitle}>Chưa có cổ phiếu yêu thích</p>
        <p className={styles.emptyDesc}>Thêm cổ phiếu từ trang chủ hoặc thị trường.</p>
      </div>
    );
  }

  const cols: { key: SortKey; label: string; align?: "right" }[] = [
    { key: "symbol", label: "Mã CK" },
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
            <th className={`${styles.th} ${styles.alignRight}`} style={{ width: 40 }}>✦</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((stock) => {
            const isUp = stock.change > 0;
            const isDown = stock.change < 0;
            return (
              <tr key={stock.symbol} onClick={() => navigate(`/chart/${stock.symbol}`)}>
                <td className={styles.sym}>{stock.symbol}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatPrice(stock.ceiling)}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatPrice(stock.ref_price)}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatPrice(stock.floor)}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatVolume(stock.best_bid_vol)}</td>
                <td className={`${styles.cell} ${styles.alignRight} ${styles.up}`}>{formatPrice(stock.best_bid_price)}</td>
                <td className={`${styles.cell} ${styles.alignRight} ${styles.down}`}>{formatPrice(stock.best_ask_price)}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatVolume(stock.best_ask_vol)}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatPrice(stock.matched_price)}</td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatPrice(stock.last_price)}</td>
                <td className={`${styles.cell} ${styles.alignRight} ${isUp ? styles.up : isDown ? styles.down : styles.neutral}`}>
                  {formatPrice(stock.change)}
                </td>
                <td className={`${styles.cell} ${styles.alignRight} ${isUp ? styles.up : isDown ? styles.down : styles.neutral}`}>
                  {stock.ratio_change > 0 ? "+" : ""}{stock.ratio_change.toFixed(2)}%
                </td>
                <td className={`${styles.cell} ${styles.alignRight}`}>{formatVolume(stock.volume)}</td>
                <td
                  className={`${styles.cell} ${styles.alignRight} ${styles.removeBtn}`}
                  onClick={(e) => { e.stopPropagation(); handleRemove(stock.symbol); }}
                >
                  ★
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function FavoritesPage() {
  return (
    <div className={styles.page}>
      <Header />
      <div className={styles.body}>
        <div className={styles.titleBar}>
          <h1 className={styles.title}>⭐ Yêu thích</h1>
          <span className={styles.subtitle}>Danh sách theo dõi cá nhân</span>
        </div>
        <FavoritesTable />
      </div>
    </div>
  );
}
