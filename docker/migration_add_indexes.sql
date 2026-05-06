-- ============================================
-- StreamFlow - Performance Indexes Migration
-- Date: 2025-05-06
-- Purpose: Add indexes to optimize list_latest_quotes query
-- ============================================

-- Indexes for data.data_trade
-- Supports window function: ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY id DESC)
-- and exchange filtering: WHERE exchange = ?
CREATE INDEX IF NOT EXISTS idx_data_trade_symbol_id_desc ON data.data_trade (symbol, id DESC);
CREATE INDEX IF NOT EXISTS idx_data_trade_exchange_id_desc ON data.data_trade (`exchange`, id DESC);

-- Indexes for data.data_quote
-- Supports correlated subquery: MAX(id) WHERE symbol_id = ranked_trade.symbol
CREATE INDEX IF NOT EXISTS idx_data_quote_symbol_id_desc ON data.data_quote (symbol_id, id DESC);

-- Indexes for data.foreign_room
-- Supports correlated subquery: MAX(id) WHERE symbol = ranked_trade.symbol
CREATE INDEX IF NOT EXISTS idx_foreign_room_symbol_id_desc ON data.foreign_room (symbol, id DESC);

-- Index for data.indexcomponent
-- Optimizes VN30/HNX30 constituent lookup
CREATE INDEX IF NOT EXISTS idx_indexcomponent_index_id_effective_date ON data.indexcomponent (index_id, effective_date, symbol);
