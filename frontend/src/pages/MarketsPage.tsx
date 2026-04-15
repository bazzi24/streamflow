import { useParams, useNavigate } from "react-router-dom";
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { stockApi } from "../api/stockApi";
import { StockGrid } from "../components/StockGrid";
import { useAppStore, type MarketSegment } from "../stores/appStore";
import styles from "./MarketsPage.module.css";

const SEGMENTS: { key: MarketSegment; label: string }[] = [
  { key: "ALL", label: "Tất cả" },
  { key: "HOSE", label: "HOSE" },
  { key: "HNX", label: "HNX" },
  { key: "VN30", label: "VN30" },
  { key: "HNX30", label: "HNX30" },
  { key: "UPCOM", label: "UPCOM" },
];

function Header() {
  const navigate = useNavigate();
  const { user, clearAuth, toggleTheme } = useAppStore();
  return (
    <header className={styles.header}>
      <div className={styles.headerLeft}>
        <div className={styles.logoWrap} onClick={() => navigate("/")} style={{ cursor: "pointer" }}>
          <div className={styles.logo}>SF</div>
          <span className={styles.logoText}>StreamFlow</span>
        </div>
        <nav className={styles.nav}>
          <a className={styles.navLink} href="/">Trang chủ</a>
          <a className={styles.navLink} href="/markets">Thị trường</a>
        </nav>
      </div>
      <div className={styles.headerRight}>
        <button className={styles.iconBtn} title="Toggle theme" onClick={toggleTheme}>☀️</button>
        {user ? (
          <>
            <button className={styles.watchlistBtn} onClick={() => navigate("/favorites")}>★ Watchlist</button>
            <div className={styles.userChip}>
              <div className={styles.userAvatar}>{user.username.charAt(0).toUpperCase()}</div>
              <span>{user.username}</span>
              <button className={styles.logoutBtn} onClick={clearAuth}>Logout</button>
            </div>
          </>
        ) : (
          <button className={styles.loginBtn} onClick={() => navigate("/login")}>Sign In</button>
        )}
      </div>
    </header>
  );
}

export function MarketsPage() {
  const navigate = useNavigate();
  const params = useParams();
  const activeSegment = useAppStore((s) => s.activeSegment);
  const setActiveSegment = useAppStore((s) => s.setActiveSegment);
  const [search, setSearch] = useState("");

  const segment = (params.segment?.toUpperCase() as MarketSegment) ?? activeSegment;

  const exchange = segment === "ALL" ? undefined : segment;

  const { data: stocks = [], isLoading } = useQuery({
    queryKey: ["stocks", segment],
    queryFn: () => stockApi.listStocks(exchange).then((r) => r.data),
    refetchInterval: 30_000,
  });

  const filtered = search
    ? stocks.filter((s: { symbol: string }) => s.symbol.toLowerCase().includes(search.toLowerCase()))
    : stocks;

  return (
    <div className={styles.page}>
      <Header />
      <div className={styles.body}>
        {/* Title bar */}
        <div className={styles.titleBar}>
          <h1 className={styles.title}>🏛️ Thị trường</h1>
          {isLoading && <span className={styles.loading}>Đang cập nhật...</span>}
          <input
            type="text"
            placeholder="Tìm mã ck..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className={styles.searchInput}
          />
          <span className={styles.count}>{filtered.length} mã</span>
        </div>

        {/* Segment tabs */}
        <div className={styles.tabBar}>
          {SEGMENTS.map((seg) => (
            <button
              key={seg.key}
              onClick={() => {
                setActiveSegment(seg.key);
                navigate(`/markets/${seg.key.toLowerCase()}`);
              }}
              className={`${styles.tab} ${segment === seg.key ? styles.active : ""}`}
            >
              {seg.label}
            </button>
          ))}
        </div>

        {/* Grid */}
        <div className={styles.gridWrap}>
          <StockGrid
            symbols={filtered}
            showExchange={segment === "ALL"}
            title={SEGMENTS.find((s) => s.key === segment)?.label}
          />
        </div>
      </div>
    </div>
  );
}
