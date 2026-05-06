import client from "./client";

// ── Types ──────────────────────────────────────────────────────────────────

export interface StockQuote {
  symbol: string;
  last_price: number;
  change: number;
  ratio_change: number;
  volume: number;
  value: number;
  highest: number;
  lowest: number;
  ref_price: number;
  ceiling: number;
  floor: number;
  time: string;
}

export interface OrderBookLevel {
  price: number;
  volume: number;
}

export interface OrderBook {
  symbol: string;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  time: string;
}

export interface OHLCVBar {
  timestamp: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface SymbolMeta {
  symbol: string;
  symbol_name: string;
  sector: string | null;
}

export interface BidAskLevel {
  bid_price: number;
  bid_vol: number;
  ask_price: number;
  ask_vol: number;
}

export interface StockSummary {
  symbol: string;
  symbol_name: string;
  exchange: string;
  last_price: number;
  change: number;
  ratio_change: number;
  volume: number;
  last_vol: number;
  total_vol: number;
  value: number;
  ceiling: number;
  floor: number;
  ref_price: number;
  best_bid_price: number;
  best_bid_vol: number;
  best_ask_price: number;
  best_ask_vol: number;
  bid_ask_levels: BidAskLevel[];  // top-3 levels: [best, 2nd, 3rd]
  ask_levels: BidAskLevel[];       // sell side top-3 (deduplicated from bid_ask_levels)
  matched_price: number;
  time: string;
  highest: number;
  lowest: number;
  nn_mua?: number;
  nn_ban?: number;
  room?: number;
  is_warrant?: boolean;
  is_etf?: boolean;
}

export interface PaginatedStocksResponse {
  items: StockSummary[];
  total: number;
  limit: number;
  offset: number;
}

export interface IndexOverview {
  index_id: string;
  index_name: string;
  index_value: number;
  change: number;
  ratio_change: number;
  advances: number;
  declines: number;
  nochanges?: number;
  total_qtty?: number;
  total_value?: number;
  time: string;
}

export interface MarketOverview {
  indices: IndexOverview[];
  top_gainers: StockSummary[];
  top_losers: StockSummary[];
}

export interface TradeMatch {
  trading_date: string;
  time: string;
  symbol: string;
  price: number;
  volume: number;
  side: "buy" | "sell";
  price_change: number | null;
}

// ── Auth ───────────────────────────────────────────────────────────────────

export interface LoginRequest {
  email: string;
  password: string;
}

export interface UserResponse {
  id: number;
  email: string;
  username: string;
  is_active: boolean;
}

export interface TokenResponse {
  access_token: string;
  token_type: string;
  user: UserResponse;
}

export const authApi = {
  login: (data: LoginRequest) => client.post<TokenResponse>("/auth/login", data),
  register: (data: LoginRequest & { username: string }) =>
    client.post<TokenResponse>("/auth/register", data),
  getMe: () => client.get<UserResponse>("/users/me"),
};

// ── Stocks ────────────────────────────────────────────────────────────────

export const stockApi = {
  listStocks: (exchange?: string, segment?: string, limit = 100, offset = 0) =>
    client.get<PaginatedStocksResponse>("/stocks", {
      params: { exchange: exchange ?? undefined, segment: segment ?? undefined, limit, offset }
    }),
  getSymbol: (symbol: string) => client.get<SymbolMeta>(`/stocks/${symbol}`),
  getQuote: (symbol: string) => client.get<StockQuote>(`/stocks/${symbol}/quote`),
  getOrderBook: (symbol: string) => client.get<OrderBook>(`/stocks/${symbol}/orderbook`),
  getOHLCV: (symbol: string, interval = "5m", limit = 200) =>
    client.get<OHLCVBar[]>(`/stocks/${symbol}/ohlcv`, { params: { interval, limit } }),
  getHistory: (symbol: string, days = 30) =>
    client.get<OHLCVBar[]>(`/stocks/${symbol}/history`, { params: { days } }),
  getTradeMatches: (symbol: string, date?: string) =>
    client.get<TradeMatch[]>(`/stocks/${symbol}/trade-matches`, { params: { date: date ?? undefined } }),
};

// ── Market ────────────────────────────────────────────────────────────────

export const marketApi = {
  getOverview: () => client.get<MarketOverview>("/market/overview"),
};

// ── Watchlist ─────────────────────────────────────────────────────────────

export const watchlistApi = {
  getWatchlist: () => client.get<{ symbols: string[] }>("/users/me/watchlist"),
  updateWatchlist: (symbols: string[]) =>
    client.put<{ symbols: string[] }>("/users/me/watchlist", { symbols }),
};

