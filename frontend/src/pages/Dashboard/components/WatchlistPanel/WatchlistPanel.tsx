import { useState, useMemo, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { stockApi } from "../../../../api/stockApi";
import { useAppStore } from "../../../../stores/appStore";
import type { StockSummary } from "../../../../api/stockApi";
import { formatPrice } from "../../../../lib/utils";
import styles from "./WatchlistPanel.module.css";

// ── Static watchlist items ───────────────────────────────────────────────────

const STATIC_STOCKS: Array<{ symbol: string; name: string }> = [
  { symbol: "VCB", name: "Vietcombank" },
  { symbol: "MBB", name: "MB Bank" },
  { symbol: "ACB", name: "ACB Bank" },
  { symbol: "FPT", name: "FPT Corp" },
  { symbol: "HPG", name: "Hoa Phat" },
  { symbol: "VND", name: "VNDirect" },
  { symbol: "TCB", name: "Techcombank" },
  { symbol: "VPB", name: "VPBank" },
  { symbol: "CTG", name: "VietinBank" },
  { symbol: "BID", name: "BIDV" },
  { symbol: "VHM", name: "Vinhomes" },
  { symbol: "SSI", name: "SSI Sec" },
  { symbol: "SHB", name: "SHB" },
  { symbol: "TPB", name: "TPBank" },
  { symbol: "STB", name: "Sacombank" },
];

const STATIC_CRYPTO: Array<{ symbol: string; name: string }> = [
  { symbol: "BTC", name: "Bitcoin" },
  { symbol: "ETH", name: "Ethereum" },
  { symbol: "SOL", name: "Solana" },
  { symbol: "BNB", name: "Binance" },
  { symbol: "XRP", name: "Ripple" },
];

// ── Component ────────────────────────────────────────────────────────────────

export function WatchlistPanel() {
  const [search, setSearch] = useState("");
  const [stocksExpanded, setStocksExpanded] = useState(true);
  const [cryptoExpanded, setCryptoExpanded] = useState(true);

  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const setSelectedSymbol = useAppStore((s) => s.setSelectedSymbol);
  const setWatchlistCryptoMode = useAppStore((s) => s.setWatchlistCryptoMode);

  // Fetch all stocks for live prices
  const { data: allStocks } = useQuery({
    queryKey: ["stocks", "ALL"],
    queryFn: () => stockApi.listStocks().then((r) => r.data),
    staleTime: 30_000,
  });

  // Build a price map: symbol → StockSummary
  const priceMap = useMemo(() => {
    const map = new Map<string, StockSummary>();
    allStocks?.forEach((s) => map.set(s.symbol, s));
    return map;
  }, [allStocks]);

  // ── Filter logic ──────────────────────────────────────────────────────────
  const filteredStocks = useMemo(() => {
    if (!search) return STATIC_STOCKS;
    return STATIC_STOCKS.filter(
      (s) =>
        s.symbol.toLowerCase().includes(search.toLowerCase()) ||
        s.name.toLowerCase().includes(search.toLowerCase())
    );
  }, [search]);

  const filteredCrypto = useMemo(() => {
    if (!search) return STATIC_CRYPTO;
    return STATIC_CRYPTO.filter(
      (s) =>
        s.symbol.toLowerCase().includes(search.toLowerCase()) ||
        s.name.toLowerCase().includes(search.toLowerCase())
    );
  }, [search]);

  // ── Render a row ──────────────────────────────────────────────────────────
  const renderRow = useCallback(
    (item: { symbol: string; name: string }, isCrypto = false) => {
      const stock = priceMap.get(item.symbol);
      const price = stock?.last_price ?? (isCrypto ? 0 : 0);
      const pct = stock?.ratio_change ?? 0;

      const isSelected = selectedSymbol === item.symbol;

      return (
        <div
          key={item.symbol}
          className={`${styles.watchlistRow} ${isSelected ? styles.selected : ""}`}
          onClick={() => {
            setSelectedSymbol(item.symbol);
            setWatchlistCryptoMode(isCrypto);
          }}
          title={item.name}
        >
          <div className={styles.watchlistSymbol}>{item.symbol}</div>
          {price > 0 && (
            <>
              <div className={styles.watchlistPrice}>{formatPrice(price)}</div>
              <div
                className={`${styles.watchlistChange} ${
                  pct > 0 ? styles.up : pct < 0 ? styles.down : styles.neutral
                }`}
              >
                {pct > 0 ? "+" : ""}
                {pct.toFixed(2)}%
              </div>
            </>
          )}
        </div>
      );
    },
    [selectedSymbol, priceMap, setSelectedSymbol, setWatchlistCryptoMode]
  );

  return (
    <div className={styles.watchlistPanel}>
      {/* Search */}
      <div className={styles.watchlistSearch}>
        <input
          className={styles.watchlistSearchInput}
          type="text"
          placeholder="Search symbol..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      {/* Stock section */}
      <div className={styles.watchlistSection}>
        <div
          className={styles.watchlistSectionHeader}
          onClick={() => setStocksExpanded((v) => !v)}
        >
          <span>VN Stocks ({filteredStocks.length})</span>
          <span>{stocksExpanded ? "▼" : "▶"}</span>
        </div>
        {stocksExpanded && (
          <div className={styles.watchlistItems}>
            {filteredStocks.map((s) => renderRow(s))}
          </div>
        )}
      </div>

      {/* Crypto section */}
      <div className={styles.watchlistSection}>
        <div
          className={styles.watchlistSectionHeader}
          onClick={() => setCryptoExpanded((v) => !v)}
        >
          <span>Crypto ({filteredCrypto.length})</span>
          <span>{cryptoExpanded ? "▼" : "▶"}</span>
        </div>
        {cryptoExpanded && (
          <div className={styles.watchlistItems}>
            {filteredCrypto.map((s) => renderRow(s, true))}
          </div>
        )}
      </div>
    </div>
  );
}