import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, md5, concat_ws, round

load_dotenv()

MYSQL_JAR = os.getenv("MYSQL_JAR", "/streamflow/lib/mysql-connector-j-8.0.33.jar")


def fact_quote():
    spark = SparkSession.builder \
        .appName("ETL_Fact_Quote") \
        .master(os.getenv("SPARK_MASTER_URL", "local[*]")) \
        .config("spark.jars", MYSQL_JAR) \
        .config("spark.driver.memory", "6g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "2000") \
        .getOrCreate()

    raw_db_url = os.getenv("RAW_DB_URL")
    raw_db_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER"),
    }

    dw_db_url = os.getenv("DW_DB_URL")
    dw_db_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER"),
    }

    date_dw_df     = spark.read.jdbc(url=dw_db_url, table="dim.`date`",         properties=dw_db_properties)
    time_dw_df     = spark.read.jdbc(url=dw_db_url, table="dim.`time`",          properties=dw_db_properties)
    symbol_dw_df   = spark.read.jdbc(url=dw_db_url, table="dim.symbol",           properties=dw_db_properties)
    exchange_dw_df = spark.read.jdbc(url=dw_db_url, table="dim.exchange",       properties=dw_db_properties)
    session_dw_df  = spark.read.jdbc(url=dw_db_url, table="dim.tradingsession",  properties=dw_db_properties)

    # MySQL syntax: DATE() cast and DATE_SUB() instead of PostgreSQL ::date and INTERVAL '2 day'
    queryquote = """
    (
        SELECT *
        FROM data.data_quote
        WHERE DATE(trading_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 2 DAY)
    ) AS recent_quotes
    """

    quote_raw_df = spark.read.jdbc(url=raw_db_url,
                                  table=queryquote,
                                  properties=raw_db_properties)

    fact_quote = quote_raw_df.alias("q") \
        .join(date_dw_df.alias("d"),    trim(col("q.trading_date")) == trim(col("d.tradingdate")),           "left") \
        .join(time_dw_df.alias("t"),    col("q.time")              == col("t.time_hh_mm_ss"),              "left") \
        .join(symbol_dw_df.alias("s"),   trim(col("q.symbol_id"))   == trim(col("s.symbol"))) \
        .join(exchange_dw_df.alias("e"), trim(col("q.exchange"))   == trim(col("e.exchange_name")),         "left") \
        .join(session_dw_df.alias("se"),  trim(col("q.trading_session"))  == trim(col("se.trading_session")), "left")

    # Round all price columns to 4 dp so the dedup hash is stable
    price_cols = [
        "ask_price1", "ask_price2", "ask_price3",
        "bid_price1", "bid_price2", "bid_price3",
    ]
    rounded_quote = fact_quote
    for c in price_cols:
        rounded_quote = rounded_quote.withColumn(c, round(col(c), 4))

    # Select + dedup hash covers all bid/ask price+vol columns
    fact_quote_df = rounded_quote.select(
        col("d.tradingdate_key").alias("tradingdate_key"),
        col("t.time_key").alias("time_key"),
        col("s.symbol_key").alias("symbol_key"),
        col("e.exchange_key").alias("exchange_key"),
        col("se.trading_session_key").alias("trading_session_key"),
        col("q.ask_price1").alias("ask_price1"),
        col("q.ask_vol1").alias("ask_vol1"),
        col("q.ask_price2").alias("ask_price2"),
        col("q.ask_vol2").alias("ask_vol2"),
        col("q.ask_price3").alias("ask_price3"),
        col("q.ask_vol3").alias("ask_vol3"),
        col("q.bid_price1").alias("bid_price1"),
        col("q.bid_vol1").alias("bid_vol1"),
        col("q.bid_price2").alias("bid_price2"),
        col("q.bid_vol2").alias("bid_vol2"),
        col("q.bid_price3").alias("bid_price3"),
        col("q.bid_vol3").alias("bid_vol3"),
    )

    # Composite hash of all bid/ask columns — catches any price/vol change
    dedup_cols = [
        "tradingdate_key", "time_key", "symbol_key", "exchange_key", "trading_session_key",
        "ask_price1", "ask_vol1", "ask_price2", "ask_vol2", "ask_price3", "ask_vol3",
        "bid_price1", "bid_vol1", "bid_price2", "bid_vol2", "bid_price3", "bid_vol3",
    ]
    fact_quote_df = fact_quote_df.withColumn(
        "dedup_key",
        md5(concat_ws("||", *[col(c).cast("string") for c in dedup_cols]))
    ).dropDuplicates(["dedup_key"])

    date_list = [row[0] for row in fact_quote_df.select("tradingdate_key").distinct().collect()]
    print(f"Will process {len(date_list)} date(s): {date_list}")

    for date_key in date_list:
        print(f"Processing on day {date_key} ...")

        df_day = fact_quote_df.filter(col("tradingdate_key") == date_key)

        # Match on the composite hash so any bid/ask column change is caught
        query = f"""
        (
            SELECT MD5(CONCAT_WS('||',
                tradingdate_key, time_key, symbol_key, exchange_key, trading_session_key,
                ask_price1, ask_vol1, ask_price2, ask_vol2, ask_price3, ask_vol3,
                bid_price1, bid_vol1, bid_price2, bid_vol2, bid_price3, bid_vol3
            )) AS dedup_key
            FROM warehouse.fact.stockorderbook
            WHERE tradingdate_key = {date_key}
        ) AS existing
        """

        existing_df = spark.read.jdbc(url=dw_db_url,
                                     table=query,
                                     properties=dw_db_properties)

        new_df = df_day.join(existing_df,
                             on="dedup_key",
                             how="left_anti")

        count_new = new_df.count()
        print(f"Day {date_key}: {count_new} new records")

        if count_new > 0:
            new_df.drop("dedup_key") \
                .repartition(2, "symbol_key") \
                .write \
                .mode("append") \
                .option("batchsize", 500) \
                .option("isolationLevel", "NONE") \
                .jdbc(url=dw_db_url,
                      table="fact.stockorderbook",
                      properties=dw_db_properties)

        new_df.unpersist(blocking=True)
        df_day.unpersist(blocking=True)
        existing_df.unpersist(blocking=True)
        spark.catalog.clearCache()

    spark.stop()
