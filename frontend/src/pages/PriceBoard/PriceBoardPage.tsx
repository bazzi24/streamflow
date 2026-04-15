import { useState, useMemo, useCallback, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { stockApi, marketApi } from "../../api/stockApi";
import { formatPrice, formatVolume, comparePrice, priceColorByCompare } from "../../lib/utils";
import { useStockWebSocket } from "../../hooks/useStockWebSocket";
import type { StockSummary, IndexOverview } from "../../api/stockApi";
import type { WsMessage } from "../../hooks/useStockWebSocket";
import { ChartModal } from "../ChartPageV2";
import styles from "./PriceBoardPage.module.css";




// ── Segment tabs ────────────────────────────────────────────────────────────────

const SEGMENTS = [
  { label: "Danh mục", value: "MY" },
  { label: "VN30",     value: "VN30" },
  { label: "HNX30",    value: "HNX30" },
  { label: "HOSE",     value: "HOSE" },
  { label: "HNX",      value: "HNX" },
  { label: "UPCOM",    value: "UPCOM" },
  { label: "ETF",       value: "ETF" },
  { label: "Phái sinh", value: "DERIVATIVE" },
  { label: "Trái phiếu",   value: "WARRANT" },
];

// ── Table column definitions ────────────────────────────────────────────────────

type SortKey = "symbol" | "change" | "ratio_change" | "volume" | "total_vol";

interface ColDef {
  key: string;
  label: string;
  className: string;
  sortable?: boolean;
  colorFn?: (row: StockSummary) => string;
  headerColor?: string; // CSS class name for sub-header text color
}

function changeColor(row: StockSummary) {
  if (row.change > 0) return styles.cellUp;
  if (row.change < 0) return styles.cellDown;
  return styles.cellNeutral;
}

function pctColorFn(row: StockSummary) {
  if (row.ratio_change > 0) return styles.cellUp;
  if (row.ratio_change < 0) return styles.cellDown;
  return styles.cellNeutral;
}

function highLowColor(value: number, ref: number, ceiling?: number) {
  if (ceiling != null && value >= ceiling) return styles.cellPurple;
  if (value > ref) return styles.cellUp;
  if (value < ref) return styles.cellDown;
  return styles.cellNeutral;
}

/**
 * Maps a bid/ask price to a CSS color value (var(--accent-*)) by comparing
 * against ref_price, ceiling, and floor.
 */
function priceCompareColor(price: number, ref: number, ceiling: number, floor: number): string {
  const result = comparePrice(price, ref, ceiling, floor);
  return priceColorByCompare(result);
}

const COLUMNS: ColDef[] = [
  { key: "symbol",        label: "CK",       className: styles["col-ck"],   sortable: true,                              headerColor: styles.subWhite },
  { key: "ceiling",       label: "Trần",     className: styles["col-tran"], colorFn: () => styles.cellPurple,             headerColor: styles.subWhite },
  { key: "floor",         label: "Sàn",      className: styles["col-san"],  colorFn: () => styles.cellCyan,              headerColor: styles.subWhite },
  { key: "ref_price",     label: "TC",       className: styles["col-tc"],   colorFn: () => styles.cellYellow,            headerColor: styles.subWhite },
  // Bên mua (buy) — dynamic: compare against ref_price
  { key: "bid3",          label: "Giá 3",   className: styles["col-bid3"], colorFn: (r) => priceCompareColor(r.bid_ask_levels[2]?.bid_price ?? 0, r.ref_price, r.ceiling, r.floor), headerColor: styles.subWhite },
  { key: "kl3",           label: "KL 3",    className: styles["col-kl3"],                                              headerColor: styles.subWhite },
  { key: "bid2",          label: "Giá 2",   className: styles["col-bid2"], colorFn: (r) => priceCompareColor(r.bid_ask_levels[1]?.bid_price ?? 0, r.ref_price, r.ceiling, r.floor), headerColor: styles.subWhite },
  { key: "kl2",           label: "KL 2",    className: styles["col-kl2"],                                              headerColor: styles.subWhite },
  { key: "bid1",          label: "Giá 1",   className: styles["col-bid1"], colorFn: (r) => priceCompareColor(r.bid_ask_levels[0]?.bid_price ?? 0, r.ref_price, r.ceiling, r.floor), headerColor: styles.subWhite },
  { key: "kl1",           label: "KL 1",    className: styles["col-kl1"],                                              headerColor: styles.subWhite },
  // Khớp lệnh (matched)
  { key: "matched_price", label: "Giá",     className: styles["col-gia"],                                              headerColor: styles.subWhite },
  { key: "vol",           label: "KL",      className: styles["col-kl"],                                              headerColor: styles.subWhite },
  { key: "change",        label: "+/-",     className: styles["col-change"], sortable: true, colorFn: changeColor,     headerColor: styles.subWhite },
  { key: "ratio_change",  label: "%",       className: styles["col-pct"],   sortable: true, colorFn: pctColorFn,        headerColor: styles.subWhite },
  // Bên bán (sell) — dynamic: compare against ref_price
  { key: "ask1",          label: "Giá 1",   className: styles["col-ask1"], colorFn: (r) => priceCompareColor(r.ask_levels[0]?.ask_price ?? 0, r.ref_price, r.ceiling, r.floor), headerColor: styles.subWhite },
  { key: "akl1",          label: "KL 1",    className: styles["col-akl1"],                                              headerColor: styles.subWhite },
  { key: "ask2",          label: "Giá 2",   className: styles["col-ask2"], colorFn: (r) => priceCompareColor(r.ask_levels[1]?.ask_price ?? 0, r.ref_price, r.ceiling, r.floor), headerColor: styles.subWhite },
  { key: "akl2",          label: "KL 2",    className: styles["col-akl2"],                                              headerColor: styles.subWhite },
  { key: "ask3",          label: "Giá 3",   className: styles["col-ask3"], colorFn: (r) => priceCompareColor(r.ask_levels[2]?.ask_price ?? 0, r.ref_price, r.ceiling, r.floor), headerColor: styles.subWhite },
  { key: "akl3",          label: "KL 3",    className: styles["col-akl3"],                                              headerColor: styles.subWhite },
  // Stats
  { key: "total_vol",     label: "Tổng KL", className: styles["col-tongkl"], sortable: true,                        headerColor: styles.subWhite },
  { key: "highest",       label: "Cao",     className: styles["col-cao"],  colorFn: () => styles.cellUp,              headerColor: styles.subWhite },
  { key: "lowest",        label: "Thấp",   className: styles["col-thap"], colorFn: () => styles.cellDown,            headerColor: styles.subWhite },
  { key: "nn_mua",        label: "NN mua",  className: styles["col-nnmua"],                                            headerColor: styles.subWhite },
  { key: "nn_ban",        label: "NN bán",  className: styles["col-nnban"],                                            headerColor: styles.subWhite },
  { key: "room",          label: "Room",    className: `${styles["col-room"]} ${styles.stickyRight}`,                  headerColor: styles.subWhite },
];

// Pixel widths keyed to COLUMNS order (must match CSS .col-* definitions)
const colWidths: Record<string, string> = {
  symbol:        "66px",
  ceiling:       "62px",
  floor:         "62px",
  ref_price:     "60px",
  bid3:          "62px",
  kl3:           "54px",
  bid2:          "62px",
  kl2:           "54px",
  bid1:          "62px",
  kl1:           "54px",
  matched_price: "62px",
  vol:           "72px",
  change:        "56px",
  ratio_change:  "58px",
  ask1:          "62px",
  akl1:          "54px",
  ask2:          "62px",
  akl2:          "54px",
  ask3:          "62px",
  akl3:          "54px",
  total_vol:     "100px",
  highest:      "62px",
  lowest:       "62px",
  nn_mua:        "64px",
  nn_ban:        "64px",
  room:          "80px",
};

// ── Price Row ─────────────────────────────────────────────────────────────────

interface PriceRowProps {
  row: StockSummary;
  isEven: boolean;
  isSelected: boolean;
  onClick: () => void;
}

function PriceRow({ row, isEven, isSelected, onClick }: PriceRowProps) {
  // bid_ask_levels: [best_bid, 2nd_bid, 3rd_bid] — buy side
  const bidLevels = row.bid_ask_levels ?? [];
  const askLevels = row.ask_levels ?? [];

  const bid3 = bidLevels[2] ?? { bid_price: 0, bid_vol: 0 };
  const bid2 = bidLevels[1] ?? { bid_price: 0, bid_vol: 0 };
  const bid1 = bidLevels[0] ?? { bid_price: 0, bid_vol: 0 };

  const ask1 = askLevels[0] ?? { ask_price: 0, ask_vol: 0 };
  const ask2 = askLevels[1] ?? { ask_price: 0, ask_vol: 0 };
  const ask3 = askLevels[2] ?? { ask_price: 0, ask_vol: 0 };

  const priceUp   = row.ratio_change > 0;
  const priceDown = row.ratio_change < 0;

  const rowClass = [
    styles.priceRow,
    isEven ? styles.evenRow : styles.oddRow,
    isSelected ? styles.selected : "",
    priceUp ? styles.upRow : priceDown ? styles.downRow : styles.neutralRow,
  ].filter(Boolean).join(" ");

  // ── Dynamic cell colors (compare against ref_price) ──
  const matchedPrice = row.matched_price || row.last_price;

  // Static cells: ceiling=purple, floor=blue, TC=yellow
  const ceilingColor = "var(--accent-purple)";
  const floorColor  = "var(--accent-blue)";
  const tcColor      = "var(--accent-yellow)";

  // Buy side — dynamic
  const bid3Color = bid3.bid_price === 0 ? "var(--text-primary)" : priceCompareColor(bid3.bid_price, row.ref_price, row.ceiling, row.floor);
  const bid2Color = bid2.bid_price === 0 ? "var(--text-primary)" : priceCompareColor(bid2.bid_price, row.ref_price, row.ceiling, row.floor);
  const bid1Color = bid1.bid_price === 0 ? "var(--text-primary)" : priceCompareColor(bid1.bid_price, row.ref_price, row.ceiling, row.floor);

  // Sell side — dynamic
  const ask1Color = ask1.ask_price === 0 ? "var(--text-primary)" : priceCompareColor(ask1.ask_price, row.ref_price, row.ceiling, row.floor);
  const ask2Color = ask2.ask_price === 0 ? "var(--text-primary)" : priceCompareColor(ask2.ask_price, row.ref_price, row.ceiling, row.floor);
  const ask3Color = ask3.ask_price === 0 ? "var(--text-primary)" : priceCompareColor(ask3.ask_price, row.ref_price, row.ceiling, row.floor);

  // Matched price — dynamic
  const matchedColor = priceCompareColor(matchedPrice, row.ref_price, row.ceiling, row.floor);

  return (
    <tr className={rowClass} onClick={onClick}>
      {/* Symbol */}
      <td className={`${styles.priceCell} ${styles["col-ck"]} ${styles.stickyCol}`}>
        <span className={styles.symbolCell}>{row.symbol}</span>
        {row.is_warrant && <span className={styles.warrantBadge}>W</span>}
        {row.is_etf && <span className={styles.etfBadge}>E</span>}
      </td>

      {/* Ceiling — purple */}
      <td className={`${styles.priceCell} ${styles["col-tran"]}`} style={{ color: ceilingColor }}>
        {formatPrice(row.ceiling)}
      </td>

      {/* Floor — blue */}
      <td className={`${styles.priceCell} ${styles["col-san"]}`} style={{ color: floorColor }}>
        {formatPrice(row.floor)}
      </td>

      {/* TC (Ref) — yellow */}
      <td className={`${styles.priceCell} ${styles["col-tc"]}`} style={{ color: tcColor }}>
        {formatPrice(row.ref_price)}
      </td>

      {/* ── Bên mua (buy) — Giá 3 → 1 (dynamic) ── */}
      <td className={`${styles.priceCell} ${styles["col-bid3"]}`} style={{ color: bid3Color }}>
        {formatPrice(bid3.bid_price)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-kl3"]}`} style={{ color: bid3Color }}>
        {formatVolume(bid3.bid_vol)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-bid2"]}`} style={{ color: bid2Color }}>
        {formatPrice(bid2.bid_price)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-kl2"]}`} style={{ color: bid2Color }}>
        {formatVolume(bid2.bid_vol)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-bid1"]} ${styles.fontBold}`} style={{ color: bid1Color }}>
        {formatPrice(bid1.bid_price)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-kl1"]}`} style={{ color: bid1Color }}>
        {formatVolume(bid1.bid_vol)}
      </td>

      {/* ── Khớp lệnh (matched) — dynamic color ── */}
      <td className={`${styles.priceCell} ${styles["col-gia"]}`} style={{ color: matchedColor }}>
        {formatPrice(matchedPrice)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-kl"]}`} style={{ color: matchedColor }}>
        {formatVolume((row.last_vol ?? 0))}
      </td>
      <td className={`${styles.priceCell} ${styles["col-change"]} ${changeColor(row)}`}>
        {(row.change > 0 ? "+" : row.change < 0 ? "-" : "")}{formatPrice(Math.abs(row.change))}
      </td>
      <td className={`${styles.priceCell} ${styles["col-pct"]} ${pctColorFn(row)}`}>
        {row.ratio_change > 0 ? "+" : row.ratio_change < 0 ? "" : ""}{row.ratio_change.toFixed(2)}
      </td>

      {/* ── Bên bán (sell) — Giá 1 → 3 (dynamic) ── */}
      <td className={`${styles.priceCell} ${styles["col-ask1"]} ${styles.fontBold}`} style={{ color: ask1Color }}>
        {formatPrice(ask1.ask_price)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-akl1"]}`} style={{ color: ask1Color }}>
        {formatVolume(ask1.ask_vol)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-ask2"]}`} style={{ color: ask2Color }}>
        {formatPrice(ask2.ask_price)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-akl2"]}`} style={{ color: ask2Color }}>
        {formatVolume(ask2.ask_vol)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-ask3"]}`} style={{ color: ask3Color }}>
        {formatPrice(ask3.ask_price)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-akl3"]}`} style={{ color: ask3Color }}>
        {formatVolume(ask3.ask_vol)}
      </td>

      {/* ── Stats ── */}
      <td className={`${styles.priceCell} ${styles["col-tongkl"]} ${styles.cellWhite}`}>
        {formatVolume(row.total_vol)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-cao"]} ${highLowColor(row.highest, row.ref_price, row.ceiling)}`}>
        {formatPrice(row.highest)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-thap"]} ${highLowColor(row.lowest, row.ref_price)}`}>
        {formatPrice(row.lowest)}
      </td>
      <td className={`${styles.priceCell} ${styles["col-nnmua"]} ${styles.cellWhite}`}>
        {row.nn_mua ? formatVolume(row.nn_mua) : "—"}
      </td>
      <td className={`${styles.priceCell} ${styles["col-nnban"]} ${styles.cellWhite}`}>
        {row.nn_ban ? formatVolume(row.nn_ban) : "—"}
      </td>
      <td className={`${styles.priceCell} ${styles["col-room"]} ${styles.cellWhite} ${styles.stickyRight}`}>
        {row.room ? formatVolume(row.room) : "—"}
      </td>
    </tr>
  );
}

// ── Compact Index Card (for market ticker row) ───────────────────────

function TickerCard({ index: idx }: { index: IndexOverview }) {
  const up = idx.ratio_change >= 0;
  const qtty = idx.total_qtty ?? 0;
  const qttyFormatted = qtty > 0
    ? (qtty / 1000).toLocaleString("vi-VN", { maximumFractionDigits: 0 })
    : "—";
  return (
    <div className={`${styles.tickerCard} ${up ? styles.up : styles.down}`}>
      {/* Top: name + value + sparkline */}
      <div className={styles.tickerCardTop}>
        <span className={styles.tickerBadge}>{idx.index_name}</span>
        <span className={styles.tickerValue}>{formatPrice(idx.index_value)}</span>
      </div>
      {/* Sparkline fills middle space */}
      <div className={styles.tickerSparkline}>
        <IndexSparkline value={idx.index_value} change={idx.ratio_change} />
      </div>
      {/* Bottom: % change + volume + breadth */}
      <div className={styles.tickerStats}>
        <span className={`${styles.tickerPct} ${up ? styles.up : styles.down}`}>
          {up ? "▲" : "▼"} {up ? "+" : ""}{idx.ratio_change.toFixed(2)}%
        </span>
        <span className={styles.tickerVol}>{qttyFormatted} CP</span>
        <div className={styles.tickerBreadth}>
          <span className={styles.tickerAdv}>▲ {idx.advances}</span>
          <span className={styles.tickerSep}>/</span>
          <span className={styles.tickerDec}>▼ {idx.declines}</span>
        </div>
      </div>
    </div>
  );
}

// ── Market Breadth Panel ───────────────────────────────────────────

function MarketBreadthPanel({ indices }: { indices: IndexOverview[] }) {
  const breadthIndices = ["VNINDEX", "VN30", "HNXINDEX", "HNX30"].map((id) =>
    indices.find((i) => i.index_id === id)
  ).filter(Boolean) as IndexOverview[];
  return (
    <div className={styles.marketBreadth}>
      <div className={styles.breadthHeader}>T/G Tăng/Giảm</div>
      {breadthIndices.map((idx) => (
        <div key={idx.index_id} className={styles.breadthRow}>
          <span className={styles.breadthSymbol}>{idx.index_name}</span>
          <div className={styles.breadthValues}>
            <span className={styles.breadthUp}>{idx.advances}</span>
            <span className={styles.breadthSep}>-</span>
            <span className={styles.breadthDown}>{idx.declines}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Nav Clock ─────────────────────────────────────────────────────────

function NavClock() {
  const [time, setTime] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(id);
  }, []);
  const timeStr = time.toLocaleTimeString("vi-VN", { hour12: false });
  return (
    <div className={styles.navClock}>
      <span className={styles.navClockTime}>{timeStr}</span>
      <div className={styles.navClockDot} />
    </div>
  );
}

// ── Sparkline for index cards (intraday 9h–15h) ────────────────────────
function IndexSparkline({ change }: { value: number; change: number }) {
  const color = change >= 0 ? "#00e676" : "#ff3d57";
  const h = 28;
  const w = 120;
  const points = [20, 30, 24, 34, 18, 28, 22, 12, 26, 20, 15, 24];
  const max = Math.max(...points);
  const min = Math.min(...points);
  const range = max - min || 1;
  const scaled = points.map((v, i) => {
    const x = (i / (points.length - 1)) * w;
    const y = h - ((v - min) / range) * h;
    return `${x},${y}`;
  });
  const pathD = `M ${scaled.join(" L ")}`;

  return (
    <svg width="100%" height={h} viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      <path d={pathD} fill="none" stroke={color} strokeWidth="1.5" opacity="0.9" />
      <path d={`${pathD} L ${w},${h} L 0,${h} Z`} fill={change >= 0 ? "rgba(0,230,118,0.08)" : "rgba(255,61,87,0.08)"} />
    </svg>
  );
}


// ── Main Price Board ──────────────────────────────────────────────────────────

export function PriceBoardPage() {
  const [activeSegment, setActiveSegment] = useState<string>("HOSE");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("symbol");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [chartSymbol, setChartSymbol] = useState<string | null>(null);

  const selectedExchange =
    activeSegment === "MY"        ? undefined :
    activeSegment === "VN30"     ? "VN30" :
    activeSegment === "HNX30"    ? "HNX30" :
    activeSegment === "ETF"       ? undefined :
    activeSegment === "DERIVATIVE" ? undefined :
    activeSegment === "WARRANT"  ? undefined :
    activeSegment === "UPCOM"   ? "UPCOM" :
    activeSegment === "HOSE" || activeSegment === "HNX" ? activeSegment : undefined;

  const selectedSegment: string | undefined =
    activeSegment === "ETF" ? "ETF" :
    activeSegment === "WARRANT" ? "WARRANT" : undefined;

  // ── Fetch stocks ────────────────────────────────────────────────────────────
  const { data: stocks = [], isLoading } = useQuery({
    queryKey: ["stocks", selectedExchange ?? "ALL", selectedSegment],
    queryFn: () => stockApi.listStocks(selectedExchange, selectedSegment).then((r) => r.data),
    staleTime: 15_000,
    refetchInterval: 15_000,
  });

  // ── Fetch market overview (indices) ────────────────────────────────────────
  const { data: overview } = useQuery({
    queryKey: ["market-overview"],
    queryFn: () => marketApi.getOverview().then((r) => r.data),
    staleTime: 15_000,
  });

  // ── WebSocket live updates ──────────────────────────────────────────────────
  const handleWsMessage = useCallback(
    (_msg: WsMessage) => {
      // Live updates handled via React Query cache merge
    },
    []
  );

  useStockWebSocket({ market: true, onMessage: handleWsMessage });

  // ── Sort & filter ────────────────────────────────────────────────────────────
  const displayedStocks = useMemo(() => {
    let list = [...stocks];

    // Search filter (client-side, fast)
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      list = list.filter(
        (s) =>
          s.symbol.toLowerCase().includes(q) ||
          (s.symbol_name ?? "").toLowerCase().includes(q)
      );
    }

    // Sort
    list.sort((a, b) => {
      let result: number;
      if (sortKey === "symbol") {
        result = a.symbol.localeCompare(b.symbol);
      } else if (sortKey === "change") {
        result = a.change - b.change;
      } else if (sortKey === "ratio_change") {
        result = a.ratio_change - b.ratio_change;
      } else {
        result = a.total_vol - b.total_vol;
      }
      return sortDir === "asc" ? result : -result;
    });

    return list;
  }, [stocks, search, sortKey, sortDir]);

  // ── Sort handler ──────────────────────────────────────────────────────────────
  const handleSort = useCallback((key: SortKey) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return prev;
      }
      setSortDir("asc");
      return key;
    });
  }, []);

  // ── Row click → open chart modal ────────────────────────────────────────────
  const handleRowClick = useCallback(
    (symbol: string) => {
      setChartSymbol(symbol);
    },
    []
  );

  const indices = overview?.indices ?? [];

  return (
    <div className={styles.boardRoot}>
      {/* ── SSI Header ────────────────────────────────────────────────────── */}
      <div className={styles.topHeader}>
        {/* Row 1: Top utility bar */}
        <div className={styles.topBar}>
          <div className={styles.logoSection}>
            <img
              src="/logo_streamflow.png"
              alt="StreamFlow"
              className={styles.logoImg}
            />
            <div>
              <div className={styles.logoText}>StreamFlow</div>
              <div className={styles.logoSub}>Real-time</div>
            </div>
          </div>

          {/* Scrolling marquee */}
          <div className={styles.marquee}>
            <div className={styles.marqueeInner}>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                Lãi suất qua đêm: 4.5%
              </span>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                USD/VND: 25,450
              </span>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                Phiên giao dịch: ATO
              </span>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                Thị trường: HOSE đang mở cửa
              </span>
              {/* Duplicate for seamless loop */}
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                Lãi suất qua đêm: 4.5%
              </span>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                USD/VND: 25,450
              </span>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                Phiên giao dịch: ATO
              </span>
              <span className={styles.marqueeItem}>
                <span className={styles.marqueeDot} />
                Thị trường: HOSE đang mở cửa
              </span>
            </div>
          </div>

          {/* Top bar icon buttons */}
          <div className={styles.topBarIcons}>
            <div className={styles.topBarIcon} title="Hỗ trợ">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="12" cy="12" r="10" />
                <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3M12 17h.01" />
              </svg>
            </div>
            <div className={styles.topBarIcon} title="Tìm kiếm">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="11" cy="11" r="8" />
                <path d="M21 21l-4.35-4.35" />
              </svg>
            </div>
            <div className={styles.topBarIcon} title="Thông báo">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9M13.73 21a2 2 0 0 1-3.46 0" />
              </svg>
            </div>
          </div>
        </div>

        {/* Row 2: Market ticker */}
        <div className={styles.marketTicker}>
          <div className={styles.tickerCards}>
            {["VNINDEX", "VN30", "HNXIndex", "HNX30"].map((id) => {
              const idx = indices.find((i) => i.index_id === id);
              return idx ? (
                <TickerCard key={idx.index_id} index={idx} />
              ) : null;
            })}
          </div>
          <MarketBreadthPanel indices={indices} />
        </div>

        {/* Row 3: Navigation tabs */}
        <div className={styles.navBar}>
          <div className={styles.navSearch}>
            <svg className={styles.navSearchIcon} viewBox="0 0 24 24" fill="none">
              <circle cx="11" cy="11" r="6" stroke="currentColor" strokeWidth="2" />
              <path d="M21 21l-4.35-4.35" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
            </svg>
            <input
              className={styles.navSearchInput}
              type="text"
              placeholder="Tìm kiếm CK"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>

          <div className={styles.navTabs}>
            {SEGMENTS.map((seg) => (
              <button
                key={seg.value}
                className={`${styles.navTab} ${activeSegment === seg.value ? styles.active : ""}`}
                onClick={() => setActiveSegment(seg.value)}
              >
                {seg.label}
              </button>
            ))}
          </div>

          <NavClock />
        </div>
      </div>

      {/* ── Table (multi-level grouped header) ───────────────────────────────── */}
      <div className={styles.tableWrapper}>
        <table className={styles.dataTable}>
          <colgroup>
            {COLUMNS.map((col) => (
              <col
                key={col.key}
                className={col.className}
                width={parseInt(colWidths[col.key])}
              />
            ))}
          </colgroup>

          {/* ── Row 1: Group header — merged rowSpan={2} for single cols ── */}
          {/* ── Table (multi-level grouped header) ───────────────────────────────── */}
          <thead>
            <tr className={styles.tableGroupRow}>
              {/* Các cột đơn dùng rowSpan=2 */}
              <th rowSpan={2} className={`${styles.thGroup} ${styles["col-ck"]}`}>CK</th>
              <th rowSpan={2} className={`${styles.thGroup}`}>Trần</th>
              <th rowSpan={2} className={`${styles.thGroup}`}>Sàn</th>
              <th rowSpan={2} className={`${styles.thGroup} ${styles["col-tc"]}`}>TC</th>
              
              {/* Nhóm Bên mua */}
              <th colSpan={6} className={`${styles.thGroup} ${styles.thGroupMuted}`}>Bên mua</th>
              
              {/* Nhóm Khớp lệnh */}
              <th colSpan={4} className={`${styles.thGroup} ${styles.thGroupMuted}`}>Khớp lệnh</th>
              
              {/* Nhóm Bên bán */}
              <th colSpan={6} className={`${styles.thGroup} ${styles.thGroupMuted}`}>Bên bán</th>
              
              {/* Các cột thống kê dùng rowSpan=2 */}
              <th rowSpan={2} className={`${styles.thGroup} ${styles["col-tongkl"]}`}>Tổng KL</th>
              <th rowSpan={2} className={`${styles.thGroup}`}>Cao</th>
              <th rowSpan={2} className={`${styles.thGroup}`}>Thấp</th>
              
              {/* Nhóm ĐTNN */}
              <th colSpan={3} className={`${styles.thGroup} ${styles.thGroupMuted}`}>ĐTNN</th>
          </tr>

          <tr className={styles.tableSubRow}>
              {COLUMNS.map((col) => {
                  
                  const isParentCol = [
                      "symbol", "ceiling", "floor", "ref_price", 
                      "total_vol", "highest", "lowest"
                  ].includes(col.key);

                  if (isParentCol) return null;

                  return (
                      <th
                          key={col.key}
                          className={`${styles.thSub} ${col.className} ${col.sortable ? styles.sortable : ""} ${col.headerColor ?? ""}`}
                          onClick={col.sortable ? () => handleSort(col.key as SortKey) : undefined}
                      >
                          {col.label}
                          {col.sortable && sortKey === col.key && (
                              <span className={styles.sortIcon}>
                                  {sortDir === "asc" ? " ▲" : " ▼"}
                              </span>
                          )}
                      </th>
                  );
              })}
          </tr>
          </thead>

          {/* ── Table body ── */}
          <tbody className={styles.tableBody}>
            {isLoading ? (
              Array.from({ length: 20 }).map((_, i) => (
                <tr key={i} className={styles.loadingRow}>
                  <td colSpan={COLUMNS.length} className={styles.loadingCell}>
                    Loading...
                  </td>
                </tr>
              ))
            ) : displayedStocks.length === 0 ? (
              <tr className={styles.emptyRow}>
                <td colSpan={COLUMNS.length} className={styles.emptyCell}>
                  Không có dữ liệu
                </td>
              </tr>
            ) : (
              displayedStocks.map((stock, idx) => (
                <PriceRow
                  key={stock.symbol}
                  row={stock}
                  isEven={idx % 2 === 0}
                  isSelected={chartSymbol === stock.symbol}
                  onClick={() => handleRowClick(stock.symbol)}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* ── Chart Detail Modal ──────────────────────────────────────────── */}
      {chartSymbol && (
        <ChartModal symbol={chartSymbol} onClose={() => setChartSymbol(null)} />
      )}
    </div>
  );
}
