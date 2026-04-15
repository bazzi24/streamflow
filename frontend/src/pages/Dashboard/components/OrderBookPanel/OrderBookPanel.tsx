import { useQuery, useQueryClient } from "@tanstack/react-query";
import { stockApi } from "../../../../api/stockApi";
import { useAppStore } from "../../../../stores/appStore";
import { useStockWebSocket, type WsMessage } from "../../../../hooks/useStockWebSocket";
import type { OrderBook } from "../../../../api/stockApi";
import { formatPrice, formatVolume } from "../../../../lib/utils";
import styles from "./OrderBookPanel.module.css";

export function OrderBookPanel() {
  const selectedSymbol = useAppStore((s) => s.selectedSymbol);
  const watchlistCryptoMode = useAppStore((s) => s.watchlistCryptoMode);
  const token = useAppStore((s) => s.token);
  const queryClient = useQueryClient();

  const { data: book, isLoading } = useQuery({
    queryKey: ["orderbook", selectedSymbol],
    queryFn: () => stockApi.getOrderBook(selectedSymbol).then((r) => r.data),
    enabled: !!selectedSymbol && !watchlistCryptoMode,
    staleTime: 10_000,
    refetchInterval: 10_000,
  });

  // WebSocket live updates
  useStockWebSocket({
    symbol: selectedSymbol,
    token,
    onMessage: (msg: WsMessage) => {
      if (msg.type !== "orderbook_update") return;
      const ob = msg as unknown as OrderBook;
      if (ob.symbol === selectedSymbol) {
        queryClient.setQueryData(["orderbook", selectedSymbol], ob);
      }
    },
  });

  if (watchlistCryptoMode) {
    return (
      <div className={styles.orderbookPanel}>
        <div className={styles.panelHeader}>
          <span className={styles.panelTitle}>Order Book</span>
        </div>
        <div className={styles.emptyState}>Crypto — live depth unavailable</div>
      </div>
    );
  }

  if (isLoading) return <OrderBookSkeleton />;

  const bids = book?.bids ?? [];
  const asks = book?.asks ?? [];
  const bestBid = bids[0];
  const bestAsk = asks[0];
  const spread = bestBid && bestAsk ? bestAsk.price - bestBid.price : null;

  const maxVol = Math.max(
    ...bids.map((b) => b.volume),
    ...asks.map((a) => a.volume),
    1
  );

  return (
    <div className={styles.orderbookPanel}>
      <div className={styles.panelHeader}>
        <span className={styles.panelTitle}>Order Book</span>
        <span className={styles.panelSymbol}>{selectedSymbol}</span>
      </div>

      {/* Header labels */}
      <div className={styles.levelHeader}>
        <span className={styles.sideLabel}>Bid Vol</span>
        <span className={styles.priceLabel}>Price</span>
        <span className={styles.sideLabel}>Ask Vol</span>
      </div>

      {/* Levels */}
      <div className={styles.orderbookLevels}>
        {/* Asks (reversed — lowest ask at bottom) */}
        <div className={styles.asksSection}>
          {[...asks].reverse().slice(0, 5).map((ask, i) => (
            <LevelRow
              key={`ask-${i}`}
              side="ask"
              price={ask.price}
              volume={ask.volume}
              maxVol={maxVol}
            />
          ))}
        </div>

        {/* Spread */}
        {spread != null && (
          <div className={styles.spreadRow}>
            <span className={styles.spreadLabel}>Spread</span>
            <span className={styles.spreadValue}>{formatPrice(spread)}</span>
          </div>
        )}

        {/* Bids */}
        <div className={styles.bidsSection}>
          {bids.slice(0, 5).map((bid, i) => (
            <LevelRow
              key={`bid-${i}`}
              side="bid"
              price={bid.price}
              volume={bid.volume}
              maxVol={maxVol}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

// ── Level Row ────────────────────────────────────────────────────────────────

interface LevelRowProps {
  side: "bid" | "ask";
  price: number;
  volume: number;
  maxVol: number;
}

function LevelRow({ side, price, volume, maxVol }: LevelRowProps) {
  const depthPct = (volume / maxVol) * 100;

  return (
    <div className={`${styles.levelRow} ${styles[side]}`}>
      {/* Depth bar background */}
      <div
        className={styles.depthBar}
        style={{
          width: `${depthPct}%`,
          background: side === "bid" ? "rgba(0,230,118,0.12)" : "rgba(255,61,87,0.12)",
        }}
      />
      <span className={`${styles.levelSide} ${styles[side]}`}>
        {side === "bid" ? formatVolume(volume) : ""}
      </span>
      <span className={styles.levelPrice}>{formatPrice(price)}</span>
      <span className={`${styles.levelSide} ${styles[side]}`}>
        {side === "ask" ? formatVolume(volume) : ""}
      </span>
    </div>
  );
}

function OrderBookSkeleton() {
  return (
    <div className={styles.orderbookPanel}>
      <div className={styles.panelHeader}>
        <span className={styles.panelTitle}>Order Book</span>
      </div>
      <div className={styles.skeleton}>
        {Array.from({ length: 10 }).map((_, i) => (
          <div key={i} className={styles.skeletonLine} />
        ))}
      </div>
    </div>
  );
}
