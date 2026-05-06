-- ============================================
-- StreamFlow — Database Initialization
-- Target: 2 databases — data (raw + charts) and warehouse (star-schema DW)
-- Single source of truth: this file only.
-- ============================================

-- ============================================
-- SECURITY: App user (non-root)
-- IMPORTANT: The password below MUST match DB_PASSWORD in .env.
-- In production, replace both with a strong random password.
-- ============================================
CREATE USER IF NOT EXISTS 'streamflow_app'@'%'
  IDENTIFIED BY 'change_strong_password';
GRANT
  SELECT, INSERT, UPDATE, DELETE
ON `data`.* TO 'streamflow_app'@'%';
GRANT
  SELECT, INSERT, UPDATE, DELETE
ON `warehouse`.* TO 'streamflow_app'@'%';
FLUSH PRIVILEGES;

-- ============================================
-- DATABASE: data  (raw + reference + charts)
-- ============================================
CREATE DATABASE IF NOT EXISTS `data`
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- ============================================
-- DATABASE: warehouse  (star-schema DW)
-- ============================================
CREATE DATABASE IF NOT EXISTS `warehouse`
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;


-- ============================================================
--  SCHEMA: data.market  — Exchange / index metadata
-- ============================================================
CREATE TABLE IF NOT EXISTS `data`.`exchange` (
    exchange_key   INT          NOT NULL AUTO_INCREMENT,
    exchange_name  VARCHAR(100) NOT NULL,
    PRIMARY KEY (exchange_key),
    UNIQUE KEY uk_exchange_name (exchange_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed exchange records (needed for FK constraints and loader script)
INSERT IGNORE INTO `data`.`exchange` (exchange_name) VALUES
    ('HOSE'),
    ('HNX'),
    ('UPCOM');

CREATE TABLE IF NOT EXISTS `data`.`indexlist` (
    index_id       VARCHAR(50)  NOT NULL,
    index_name     VARCHAR(100) NOT NULL DEFAULT '',
    exchange_key   INT          NOT NULL,
    PRIMARY KEY (index_id),
    INDEX idx_exchange (exchange_key),
    CONSTRAINT fk_indexlist_exchange
        FOREIGN KEY (exchange_key) REFERENCES `data`.`exchange`(exchange_key)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- indexcomponent: stores VN30, HNX30 (and future) constituent symbols.
-- Populated at runtime by dataSSI/load_index_components.py.
-- Run: docker compose run --rm load-index python dataSSI/load_index_components.py
CREATE TABLE IF NOT EXISTS `data`.`indexcomponent` (
    index_id       VARCHAR(50)  NOT NULL,
    symbol         VARCHAR(20)  NOT NULL,
    exchange_key   INT          NOT NULL,
    weight         DECIMAL(10,4) DEFAULT NULL,
    effective_date DATE         NOT NULL,
    PRIMARY KEY (index_id, symbol, effective_date),
    INDEX idx_exchange (exchange_key),
    INDEX idx_index_id (index_id),
    INDEX idx_index_id_effective_date (index_id, effective_date, symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`dailyindex` (
    index_id       VARCHAR(50)  NOT NULL,
    trading_date   DATE         NOT NULL,
    close_value    DECIMAL(20,4),
    `change`       DECIMAL(20,4),
    ratio_change   DECIMAL(10,4),
    total_qtty     BIGINT,
    total_value    DECIMAL(20,4),
    advances       INT,
    declines       INT,
    PRIMARY KEY (index_id, trading_date),
    INDEX idx_date (trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
--  SCHEMA: data.corporation  — Symbol master + reference
-- ============================================================
CREATE TABLE IF NOT EXISTS `data`.`sector` (
    sector_id   VARCHAR(50)  NOT NULL PRIMARY KEY,
    sector_name VARCHAR(255) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`corporation` (
    symbol_id      VARCHAR(20)  NOT NULL PRIMARY KEY,
    symbol_name    VARCHAR(255) NOT NULL DEFAULT '',
    symbol_en_name VARCHAR(255) NOT NULL DEFAULT '',
    sector_id      VARCHAR(50)  DEFAULT NULL,
    exchange_key   INT           DEFAULT NULL,
    UNIQUE KEY uk_symbol (symbol_id),
    INDEX idx_sector (sector_id),
    INDEX idx_exchange (exchange_key),
    CONSTRAINT fk_corp_sector
        FOREIGN KEY (sector_id) REFERENCES `data`.`sector`(sector_id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_corp_exchange
        FOREIGN KEY (exchange_key) REFERENCES `data`.`exchange`(exchange_key)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`corporation_detail` (
    symbol_id         VARCHAR(20)  NOT NULL PRIMARY KEY,
    listing_date      DATE,
    par_value         DECIMAL(20,4),
    lot_size          INT,
    issuedshares      BIGINT,
    listedshares      BIGINT,
    address           VARCHAR(500),
    telephone         VARCHAR(100),
    fax               VARCHAR(100),
    website           VARCHAR(255),
    foreign_max_room  BIGINT,
    stock_type        VARCHAR(50),
    INDEX idx_listing_date (listing_date),
    CONSTRAINT fk_corpdetail_corp
        FOREIGN KEY (symbol_id) REFERENCES `data`.`corporation`(symbol_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
--  SCHEMA: data.streaming  — Raw tick data (Kafka consumers)
--  No PK — optimised for high-throughput INSERT only.
--  Retention: 30 days for data_trade / data_quote / foreign_room.
--  Retention: 90 days for index_data.
-- ============================================================

CREATE TABLE IF NOT EXISTS `data`.`ml_feature_data` (
    id                BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    symbol            VARCHAR(20)  NOT NULL,
    trading_date      DATE         NOT NULL,
    time_key          INT,
    last_price        DECIMAL(20,4),
    avg_price         DECIMAL(20,4),
    `change`          DECIMAL(20,4),
    ratio_change      DECIMAL(20,4),
    highest           DECIMAL(20,4),
    lowest            DECIMAL(20,4),
    last_vol          BIGINT,
    ceiling           DECIMAL(20,4),
    `floor`           DECIMAL(20,4),
    sector            VARCHAR(255),
    tradingdate_key   INT,
    symbol_key        INT,
    created_at        DATETIME     DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_symbol_date (symbol, trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS `data`.`data_trade` (
    id                 BIGINT      NOT NULL AUTO_INCREMENT,
    rtype              VARCHAR(50),
    trading_date       DATE,
    time               VARCHAR(20),
    isin               VARCHAR(20),
    symbol             VARCHAR(20),
    ceiling            DECIMAL(20,4),
    `floor`            DECIMAL(20,4),
    ref_price          DECIMAL(20,4),
    avg_price          DECIMAL(20,4),
    prior_val          DECIMAL(20,4),
    last_price         DECIMAL(20,4),
    last_vol           BIGINT,
    total_val          DECIMAL(20,4),
    total_vol          BIGINT,
    market_id          VARCHAR(50),
    exchange           VARCHAR(50),
    trading_session    VARCHAR(50),
    trading_status     VARCHAR(50),
    `change`           DECIMAL(20,4),
    ratio_change       DECIMAL(20,4),
    est_matched_price  DECIMAL(20,4),
    highest            DECIMAL(20,4),
    lowest             DECIMAL(20,4),
    side               VARCHAR(10),
    PRIMARY KEY (id),
    INDEX idx_symbol_date (symbol, trading_date),
    INDEX idx_symbol_id_desc (symbol, id DESC),
    INDEX idx_exchange_id_desc (exchange, id DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`data_quote` (
    id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    trading_date    DATE,
    time            VARCHAR(20),
    exchange        VARCHAR(50),
    symbol_id       VARCHAR(20),
    rtype           VARCHAR(50),
    trading_session VARCHAR(50),
    ask_price1      DECIMAL(20,4), ask_vol1  BIGINT,
    ask_price2      DECIMAL(20,4), ask_vol2  BIGINT,
    ask_price3      DECIMAL(20,4), ask_vol3  BIGINT,
    ask_price4      DECIMAL(20,4), ask_vol4  BIGINT,
    ask_price5      DECIMAL(20,4), ask_vol5  BIGINT,
    ask_price6      DECIMAL(20,4), ask_vol6  BIGINT,
    ask_price7      DECIMAL(20,4), ask_vol7  BIGINT,
    ask_price8      DECIMAL(20,4), ask_vol8  BIGINT,
    ask_price9      DECIMAL(20,4), ask_vol9  BIGINT,
    ask_price10     DECIMAL(20,4), ask_vol10 BIGINT,
    bid_price1      DECIMAL(20,4), bid_vol1  BIGINT,
    bid_price2      DECIMAL(20,4), bid_vol2  BIGINT,
    bid_price3      DECIMAL(20,4), bid_vol3  BIGINT,
    bid_price4      DECIMAL(20,4), bid_vol4  BIGINT,
    bid_price5      DECIMAL(20,4), bid_vol5  BIGINT,
    bid_price6      DECIMAL(20,4), bid_vol6  BIGINT,
    bid_price7      DECIMAL(20,4), bid_vol7  BIGINT,
    bid_price8      DECIMAL(20,4), bid_vol8  BIGINT,
    bid_price9      DECIMAL(20,4), bid_vol9  BIGINT,
    bid_price10     DECIMAL(20,4), bid_vol10 BIGINT,
    INDEX idx_symbol_date (symbol_id, trading_date),
    INDEX idx_symbol_id_desc (symbol_id, id DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`data_quote_archive` (
    id              BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    trading_date    DATE,
    time            VARCHAR(20),
    exchange        VARCHAR(50),
    symbol_id       VARCHAR(20),
    rtype           VARCHAR(50),
    trading_session VARCHAR(50),
    ask_price1      DECIMAL(20,4), ask_vol1  BIGINT,
    ask_price2      DECIMAL(20,4), ask_vol2  BIGINT,
    ask_price3      DECIMAL(20,4), ask_vol3  BIGINT,
    ask_price4      DECIMAL(20,4), ask_vol4  BIGINT,
    ask_price5      DECIMAL(20,4), ask_vol5  BIGINT,
    ask_price6      DECIMAL(20,4), ask_vol6  BIGINT,
    ask_price7      DECIMAL(20,4), ask_vol7  BIGINT,
    ask_price8      DECIMAL(20,4), ask_vol8  BIGINT,
    ask_price9      DECIMAL(20,4), ask_vol9  BIGINT,
    ask_price10     DECIMAL(20,4), ask_vol10 BIGINT,
    bid_price1      DECIMAL(20,4), bid_vol1  BIGINT,
    bid_price2      DECIMAL(20,4), bid_vol2  BIGINT,
    bid_price3      DECIMAL(20,4), bid_vol3  BIGINT,
    bid_price4      DECIMAL(20,4), bid_vol4  BIGINT,
    bid_price5      DECIMAL(20,4), bid_vol5  BIGINT,
    bid_price6      DECIMAL(20,4), bid_vol6  BIGINT,
    bid_price7      DECIMAL(20,4), bid_vol7  BIGINT,
    bid_price8      DECIMAL(20,4), bid_vol8  BIGINT,
    bid_price9      DECIMAL(20,4), bid_vol9  BIGINT,
    bid_price10     DECIMAL(20,4), bid_vol10 BIGINT,
    INDEX idx_symbol_date (symbol_id, trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`index_data` (
    id                  BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    index_id            VARCHAR(50),
    index_value         DECIMAL(20,4),
    prior_index_value   DECIMAL(20,4),
    trading_date        DATE,
    time                VARCHAR(20),
    total_trade         BIGINT,
    total_qtty          BIGINT,
    total_value         DECIMAL(20,4),
    index_name          VARCHAR(100),
    advances            INT,
    nochanges           INT,
    declines            INT,
    ceilings            INT,
    floors              INT,
    `change`            DECIMAL(20,4),
    ratio_change        DECIMAL(20,4),
    total_qtty_pt       BIGINT,
    total_value_pt     DECIMAL(20,4),
    exchange            VARCHAR(50),
    all_qtty            BIGINT,
    all_value           DECIMAL(20,4),
    index_type          VARCHAR(50),
    trading_session     VARCHAR(50),
    market_id           VARCHAR(50),
    rtype               VARCHAR(50),
    total_qtty_od       BIGINT,
    total_value_od     DECIMAL(20,4),
    INDEX idx_index_date (index_id, trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`foreign_room` (
    id           BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    rtype        VARCHAR(50),
    trading_date DATE,
    time         VARCHAR(20),
    isin         VARCHAR(20),
    symbol       VARCHAR(20),
    total_room   BIGINT,
    current_room BIGINT,
    buy_vol      BIGINT,
    sell_vol     BIGINT,
    buy_val      DECIMAL(20,4),
    sell_val     DECIMAL(20,4),
    market_id    VARCHAR(50),
    exchange     VARCHAR(50),
    INDEX idx_symbol_date (symbol, trading_date),
    INDEX idx_symbol_id_desc (symbol, id DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`securities_status` (
    id                  BIGINT      NOT NULL AUTO_INCREMENT PRIMARY KEY,
    rtype               VARCHAR(50),
    market_id           VARCHAR(50),
    trading_date        DATE,
    time                VARCHAR(20),
    symbol_id           VARCHAR(20),
    trading_session     VARCHAR(50),
    trading_status      VARCHAR(50),
    exchange            VARCHAR(50),
    trading_ol_session  VARCHAR(50),
    INDEX idx_symbol_date (symbol_id, trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS `data`.`trade_match_archive` (
    -- One row per matched trade (buy-aggressor tick from market_data_trade)
    -- Aggregated per symbol per trading date.
    id           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    trading_date DATE         NOT NULL,
    `time`       VARCHAR(20)  NOT NULL,
    symbol       VARCHAR(20)  NOT NULL,
    price        DECIMAL(20,4) NOT NULL,
    volume       BIGINT       NOT NULL DEFAULT 0,
    -- 'buy'  = buy-initiated (Side='BU'), matched against a seller
    -- 'sell' = sell-initiated (Side='SD'), matched against a buyer
    side         VARCHAR(10)   NOT NULL,      -- 'buy' | 'sell'
    price_change DECIMAL(20,4) DEFAULT NULL,   -- change from previous match (can be negative)
    created_at   DATETIME      DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, trading_date),
    INDEX idx_time (trading_date, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
--  SCHEMA: data.candlestick  — Pre-computed OHLC candles
--  Written by CandlestickConsumer (Kafka consumer, no Flink).
--  1m = source of truth; larger timeframes derived at query time.
-- ============================================================
CREATE TABLE IF NOT EXISTS `data`.`candlestick_1m` (
    symbol        VARCHAR(20)  NOT NULL,
    time_start    DATETIME     NOT NULL,
    trading_date  DATE         NOT NULL DEFAULT '2000-01-01',
    `time`        VARCHAR(20)  NOT NULL DEFAULT '00:00:00',
    open          DECIMAL(20,4),
    high          DECIMAL(20,4),
    low           DECIMAL(20,4),
    close         DECIMAL(20,4),
    volume        BIGINT,
    PRIMARY KEY (symbol, time_start),
    INDEX idx_symbol_time (symbol, time_start DESC),
    INDEX idx_trading_date (trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `data`.`candlestick_1d` (
    symbol        VARCHAR(20)  NOT NULL,
    trading_date  DATE         NOT NULL,
    open          DECIMAL(20,4),
    high          DECIMAL(20,4),
    low           DECIMAL(20,4),
    close         DECIMAL(20,4),
    volume        BIGINT,
    nn_mua        BIGINT,
    nn_ban        BIGINT,
    room          BIGINT,
    PRIMARY KEY (symbol, trading_date),
    INDEX idx_symbol_date (symbol, trading_date DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
--  SCHEMA: warehouse.dim  — Dimension tables
-- ============================================================
-- `date` and `time` are reserved MySQL keywords — backtick-quoted.
CREATE TABLE IF NOT EXISTS `warehouse`.`date` (
    tradingdate_key  INT      NOT NULL,
    tradingdate      DATE,
    Year             INT,
    Quarter          INT,
    Month            INT,
    Day              INT,
    Weekday          INT,
    PRIMARY KEY (tradingdate_key),
    UNIQUE KEY uk_tradingdate (tradingdate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`time` (
    time_key      INT    NOT NULL,
    time_hh_mm_ss TIME,
    Hour          INT,
    Minute        INT,
    Second        INT,
    PRIMARY KEY (time_key),
    UNIQUE KEY uk_time (time_hh_mm_ss)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`symbol` (
    symbol_key     INT          NOT NULL AUTO_INCREMENT,
    symbol         VARCHAR(20)  NOT NULL,
    symbol_name    VARCHAR(255) NOT NULL DEFAULT '',
    symbol_en_name VARCHAR(255) NOT NULL DEFAULT '',
    sector         VARCHAR(255) NOT NULL DEFAULT '',
    sector_id      VARCHAR(50)  DEFAULT NULL,
    exchange_key   INT          DEFAULT NULL,
    PRIMARY KEY (symbol_key),
    UNIQUE KEY uk_symbol (symbol),
    INDEX idx_sector (sector_id),
    INDEX idx_exchange (exchange_key),
    CONSTRAINT fk_dim_symbol_sector
        FOREIGN KEY (sector_id) REFERENCES `data`.`sector`(sector_id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_dim_symbol_exchange
        FOREIGN KEY (exchange_key) REFERENCES `data`.`exchange`(exchange_key)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`market_index` (
    index_key      INT          NOT NULL AUTO_INCREMENT,
    index_name     VARCHAR(100) NOT NULL,
    exchange_key    INT          DEFAULT NULL,
    PRIMARY KEY (index_key),
    UNIQUE KEY uk_index_name (index_name),
    INDEX idx_exchange (exchange_key),
    CONSTRAINT fk_dim_market_index_exchange
        FOREIGN KEY (exchange_key) REFERENCES `data`.`exchange`(exchange_key)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`exchange` (
    exchange_key  INT          NOT NULL AUTO_INCREMENT,
    exchange_name VARCHAR(100) NOT NULL,
    PRIMARY KEY (exchange_key),
    UNIQUE KEY uk_exchange_name (exchange_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`tradingsession` (
    trading_session_key INT         NOT NULL AUTO_INCREMENT,
    trading_session     VARCHAR(50)  NOT NULL,
    PRIMARY KEY (trading_session_key),
    UNIQUE KEY uk_trading_session (trading_session)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ============================================================
--  SCHEMA: warehouse.fact  — Fact tables
--  Composite PK = 5 surrogate keys (tradingdate_key, time_key,
--  symbol_key, exchange_key, trading_session_key).
--  Indexes added for efficient slicing by symbol+date and date+exchange.
-- ============================================================
CREATE TABLE IF NOT EXISTS `warehouse`.`stockorderbook` (
    tradingdate_key     INT   NOT NULL,
    time_key            INT   NOT NULL,
    symbol_key          INT   NOT NULL,
    exchange_key        INT   NOT NULL,
    trading_session_key INT   NOT NULL,
    ask_price1          DECIMAL(20,4), ask_vol1  BIGINT,
    ask_price2          DECIMAL(20,4), ask_vol2  BIGINT,
    ask_price3          DECIMAL(20,4), ask_vol3  BIGINT,
    bid_price1          DECIMAL(20,4), bid_vol1  BIGINT,
    bid_price2          DECIMAL(20,4), bid_vol2  BIGINT,
    bid_price3          DECIMAL(20,4), bid_vol3  BIGINT,
    PRIMARY KEY (tradingdate_key, time_key, symbol_key, exchange_key, trading_session_key),
    INDEX idx_symbol_date (symbol_key, tradingdate_key),
    INDEX idx_date_exchange (tradingdate_key, exchange_key),
    CONSTRAINT fk_fact_ob_date
        FOREIGN KEY (tradingdate_key) REFERENCES `warehouse`.`date`(tradingdate_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_ob_time
        FOREIGN KEY (time_key) REFERENCES `warehouse`.`time`(time_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_ob_symbol
        FOREIGN KEY (symbol_key) REFERENCES `warehouse`.`symbol`(symbol_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_ob_exchange
        FOREIGN KEY (exchange_key) REFERENCES `warehouse`.`exchange`(exchange_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_ob_session
        FOREIGN KEY (trading_session_key) REFERENCES `warehouse`.`tradingsession`(trading_session_key)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`stocktrade` (
    tradingdate_key     INT   NOT NULL,
    time_key            INT   NOT NULL,
    symbol_key          INT   NOT NULL,
    exchange_key        INT   NOT NULL,
    trading_session_key INT   NOT NULL,
    last_price          DECIMAL(20,4),
    avg_price           DECIMAL(20,4),
    ceiling             DECIMAL(20,4),
    `floor`             DECIMAL(20,4),
    ref_price           DECIMAL(20,4),
    prio_val            DECIMAL(20,4),
    last_vol            BIGINT,
    total_val           DECIMAL(20,4),
    total_vol           BIGINT,
    `change`            DECIMAL(20,4),
    ratio_change        DECIMAL(20,4),
    highest             DECIMAL(20,4),
    lowest              DECIMAL(20,4),
    PRIMARY KEY (tradingdate_key, time_key, symbol_key, exchange_key, trading_session_key),
    INDEX idx_symbol_date (symbol_key, tradingdate_key),
    INDEX idx_date_exchange (tradingdate_key, exchange_key),
    CONSTRAINT fk_fact_st_date
        FOREIGN KEY (tradingdate_key) REFERENCES `warehouse`.`date`(tradingdate_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_st_time
        FOREIGN KEY (time_key) REFERENCES `warehouse`.`time`(time_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_st_symbol
        FOREIGN KEY (symbol_key) REFERENCES `warehouse`.`symbol`(symbol_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_st_exchange
        FOREIGN KEY (exchange_key) REFERENCES `warehouse`.`exchange`(exchange_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_st_session
        FOREIGN KEY (trading_session_key) REFERENCES `warehouse`.`tradingsession`(trading_session_key)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `warehouse`.`marketindex` (
    tradingdate_key     INT   NOT NULL,
    time_key            INT   NOT NULL,
    index_key           INT   NOT NULL,
    exchange_key        INT   NOT NULL,
    trading_session_key INT   NOT NULL,
    index_value         DECIMAL(20,4),
    prio_index_value    DECIMAL(20,4),
    `change`            DECIMAL(20,4),
    ratio_change        DECIMAL(20,4),
    total_qtty          BIGINT,
    total_value         DECIMAL(20,4),
    total_qtty_pt       BIGINT,
    total_value_pt     DECIMAL(20,4),
    advances            INT,
    nochanges           INT,
    declines            INT,
    ceilings            INT,
    floors              INT,
    PRIMARY KEY (tradingdate_key, time_key, index_key, exchange_key, trading_session_key),
    INDEX idx_index_date (index_key, tradingdate_key),
    INDEX idx_date_exchange (tradingdate_key, exchange_key),
    CONSTRAINT fk_fact_mi_date
        FOREIGN KEY (tradingdate_key) REFERENCES `warehouse`.`date`(tradingdate_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_mi_time
        FOREIGN KEY (time_key) REFERENCES `warehouse`.`time`(time_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_mi_index
        FOREIGN KEY (index_key) REFERENCES `warehouse`.`market_index`(index_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_mi_exchange
        FOREIGN KEY (exchange_key) REFERENCES `warehouse`.`exchange`(exchange_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_fact_mi_session
        FOREIGN KEY (trading_session_key) REFERENCES `warehouse`.`tradingsession`(trading_session_key)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
