create table dim.date (
	tradingdate_key int primary key,
	tradingdate date,
	year int,
	quarter int,
	month int,
	day int,
	weekday int
);

create table dim.time (
	time_key int primary key,
	time_hh_mm_ss time,
	hour int,
	minute int,
	second int
);

create table dim.index (
	index_key serial primary key,
	index_name varchar(255)
);

create table dim.exchange(
	exchange_key serial primary key,
	exchange_name varchar(255)
);

create table dim.symbol (
	symbol_key serial primary key,
	symbol char(50),
	symbol_name varchar(255),
	symbol_en_name varchar(255),
	sector varchar(255)
);

create table dim.tradingsession (
	trading_session_key serial primary key,
	trading_session char(50)
);

create table fact.marketindex (
	index_fact_key serial primary key,
	tradingdate_key int references dim.date(tradingdate_key),
	time_key int references dim.time(time_key),
	index_key int references dim.index(index_key),
	exchange_key int references dim.exchange(exchange_key),
	trading_session_key int references dim.tradingsession(trading_session_key),
	index_value numeric(18,2),
	prio_index_value numeric(18,2),
	change numeric(18,2),
	ratio_change double precision,
	total_qtty numeric(18,2),
	total_value numeric(18,2),
	total_qtty_pt numeric(18,2),
	total_value_pt numeric(18,2),
	advances numeric(18,2),
	nochanges int,
	declines int,
	ceilings int,
	floors int
);

create table fact.stocktrade (
	trade_key serial primary key,
	tradingdate_key int references dim.date(tradingdate_key),
	time_key int references dim.time(time_key),
	symbol_key int references dim.symbol(symbol_key),
	exchange_key int references dim.exchange(exchange_key),
	trading_session_key int references dim.tradingsession(trading_session_key),
	last_price numeric(18,2), 
	avg_price numeric(18,2),
	ceiling numeric(18,2),
	floor numeric(18,2),
	change numeric(18,2),
	ref_price numeric(18,2),
	prio_val numeric(18,2),
	last_vol numeric(18,2),
	total_val numeric(18,2),
	total_vol numeric(18,2),
	ratio_change numeric(18,2),
	highest numeric(18,2),
	lowest numeric(18,2)
);

create table fact.stockorderbook (
	orderbook_key serial primary key,
	tradingdate_key int references dim.date(tradingdate_key),
	time_key int references dim.time(time_key),
	symbol_key int references dim.symbol(symbol_key),
	exchange_key int references dim.exchange(exchange_key),
	trading_session_key int references dim.tradingsession(trading_session_key),
	ask_price1 numeric(18,2),
	ask_vol1 numeric(18,2),
	ask_price2 numeric(18,2),
	ask_vol2 numeric(18,2),
	ask_price3 numeric(18,2),
	ask_vol3 numeric(18,2),
	bid_price1 numeric(18,2),
	bid_vol1 numeric(18,2),
	bid_price2 numeric(18,2),
	bid_vol2 numeric(18,2),
	bid_price3 numeric(18,2),
	bid_vol3 numeric(18,2)
);

