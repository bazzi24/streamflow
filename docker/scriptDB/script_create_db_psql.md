CREATE TABLE corporation.sector (
	sector_id SERIAL PRIMARY KEY,
	sector_name VARCHAR(255) UNIQUE,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE market.market (
	market_id SERIAL PRIMARY KEY,
	market_name VARCHAR(255) UNIQUE,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE corporation.corporation (
	symbol_id CHAR(10) PRIMARY KEY,
	sector_id INT REFERENCES corporation.sector(sector_id) ON DELETE CASCADE,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	symbol_name VARCHAR(255),
	symbol_en_name VARCHAR(255),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE corporation.corporation_detail (
	symbol_detail_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	address VARCHAR(255),
	web_url CHAR(100),
	stock_type CHAR(25),
	listing_date DATE,
	delisting_date DATE,
	par_value INT,
	lot_size INT,
	total_listed_volume INT,
	total_out_standing_volume INT,
	charter_capital INT,
	foreign_max_room INT,
	foreign_current_room INT,
	status CHAR(10)
);

CREATE TABLE corporation.ceo (
	ceo_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	ceo_name VARCHAR(255),
	ceo_volume INT,
	ceo_percent INT
);

CREATE TABLE corporation.majorShareHolder (
	majorShareHolder_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	major VARCHAR(255)
);

	
CREATE TABLE corporation.report (
	report_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	report_name VARCHAR(255),
	report_date DATE
);

CREATE TABLE corporation.financeReport (
	financeReport_id SERIAL PRIMARY KEY,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	EPS INT,
	ROE INT,
	PE INT,
	PB INT
);

CREATE TABLE corporation.news (
	news_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	title VARCHAR(255),
	source TEXT
);

CREATE TABLE market.indexList (
	indexList_id CHAR(25) PRIMARY KEY,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	index_name VARCHAR(255),
	index_en_name VARCHAR(255)
);

CREATE TABLE market.indexComponent (
	indexComponent_id SERIAL PRIMARY KEY,
	indexList_id CHAR(25) REFERENCES market.indexList(indexList_id) ON DELETE CASCADE,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	weight INT,
	date DATE
);

CREATE TABLE ohlc.intradayOHLC (
	intradayOHLC_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	trading_date DATE,
	time TIME,
	open_price INT,
	high_price INT,
	low_price INT,
	close_price INT,
	volume BIGINT,
	value BIGINT
);

CREATE TABLE ohlc.dailyOHLC (
	dailyOHLC_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	open_price INT,
	high_price INT,
	low_price INT,
	close_price INT,
	avg_price INT,
	volume BIGINT,
	value BIGINT,
	adjust_factor INT
);

CREATE TABLE market.dailyIndex (
	dailyIndex_id SERIAL PRIMARY KEY,
	indexList_id CHAR(25) REFERENCES market.indexList(indexList_id) ON DELETE CASCADE,
	trading_date DATE,
	open_index INT,
	high_index INT,
	low_index INT,
	close_index INT,
	change INT,
	change_percent INT,
	volume BIGINT,
	value BIGINT
);

CREATE TABLE ohlc.dailyStockPrice (
	dailyStockPrice_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	trading_date DATE,
	open_price INT,
	high_price INT,
	low_price INT,
	close_price INT,
	ref_price INT,
	floor_price INT,
	ceiling_price INT,
	volume BIGINT,
	value BIGINT
);


### STREAMING ###
CREATE TABLE streaming.f (
	f_id SERIAL PRIMARY KEY,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	status CHAR(10)
);

CREATE TABLE streaming.x (
	x_id SERIAL PRIMARY KEY,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	trading_date DATE,
	ref_price INT,
	floor_price INT,
	ceiling_price INT,
	open_price INT,
	close_price INT,
	high_price INT,
	low_price INT,
	last_price INT,
	total_volume BIGINT,
	total_value BIGINT,
	bid_price_1 INT,
	bid_volume_1 INT,
	bid_price_2 INT,
	bid_volume_2 INT,
	bid_price_3 INT,
	bid_volume_3 INT,
	ask_price_1 INT,
	ask_volume_1 INT,
	ask_price_2 INT,
	ask_volume_2 INT,
	ask_price_3 INT,
	ask_volume_3 INT
);

CREATE TABLE streaming.r (
	r_id SERIAL PRIMARY KEY,
	market_id INT REFERENCES market.market(market_id) ON DELETE CASCADE,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	foreign_max_room BIGINT,
	foreign_current_room BIGINT,
	foreign_available_room BIGINT,
	update_time TIME
);

CREATE TABLE streaming.mi (
	mi_id SERIAL PRIMARY KEY,
	indexList_id CHAR(25) REFERENCES market.indexList(indexList_id) ON DELETE CASCADE,
	trading_date DATE,
	time TIME,
	current_index INT,
	change INT,
	change_percent INT,
	volume BIGINT,
	value BIGINT,
	total_symbols_up INT,
	total_symbols_down INT,
	total_symbols_no_change INT
);

CREATE TABLE streaming.b (
	b_id SERIAL PRIMARY KEY,
	symbol_id CHAR(10) REFERENCES corporation.corporation(symbol_id) ON DELETE CASCADE,
	trading_date DATE,
	interval INT,
	open_price INT,
	high_price INT,
	low_price INT,
	close_price INT,
	volume BIGINT,
	value BIGINT
);
