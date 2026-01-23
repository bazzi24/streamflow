from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, md5, concat_ws, round
from dotenv import load_dotenv
import os

load_dotenv()

def fact_quote():
    spark = SparkSession.builder \
        .appName("ETL_Fact_Quote") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .config("spark.driver.memory", "6g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "2000") \
        .getOrCreate()
        
    raw_db_url = os.getenv("RAW_DB_URL")
    raw_db_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER")
    }

    dw_db_url = os.getenv("DW_DB_URL")
    dw_db_properties = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER")
    }

    date_dw_df = spark.read.jdbc(url=dw_db_url, 
                                 table="dim.date", 
                                 properties=dw_db_properties)
    
    time_dw_df = spark.read.jdbc(url=dw_db_url, 
                                 table="dim.time", 
                                 properties=dw_db_properties)
    
    symbol_dw_df = spark.read.jdbc(url=dw_db_url, 
                                   table="dim.symbol", 
                                   properties=dw_db_properties)
    
    exchange_dw_df = spark.read.jdbc(url=dw_db_url, 
                                     table="dim.exchange", 
                                     properties=dw_db_properties)
    
    session_dw_df = spark.read.jdbc(url=dw_db_url, 
                                    table="dim.tradingsession", 
                                    properties=dw_db_properties)
    
    queryquote = """
    (
            SELECT *
            FROM streaming.data_quote
            WHERE trading_date::date >= (CURRENT_DATE - INTERVAL '2 day')
    ) AS recent_quotes        
    """

    quote_raw_df = spark.read.jdbc(url=raw_db_url, 
                                   table=queryquote, 
                                   properties=raw_db_properties)

    fact_quote = quote_raw_df.alias("q") \
        .join(date_dw_df.alias("d"), trim(col("q.trading_date")) == trim(col("d.tradingdate")), "left") \
        .join(time_dw_df.alias("t"), col("q.time") == col("t.time_hh_mm_ss"), "left") \
        .join(symbol_dw_df.alias("s"), trim(col("q.symbol_id")) == trim(col("s.symbol"))) \
        .join(exchange_dw_df.alias("e"), trim(col("q.exchange")) == trim(col("e.exchange_name")), "left") \
        .join(session_dw_df.alias("se"), trim(col("q.trading_session")) == trim(col("se.trading_session")), "left")
        
    fact_quote_df = fact_quote.select(
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
        col("q.bid_vol3").alias("bid_vol3")
    ).dropDuplicates(["tradingdate_key", 
                      "time_key", 
                      "symbol_key", 
                      "ask_price1", 
                      "ask_vol1"])

    date_list = [row[0] for row in fact_quote_df.select("tradingdate_key").distinct().collect()]

    print("They will be processed in order ", len(date_list), "date: ", date_list)
    
    fact_quote_df = fact_quote_df.withColumn("ask_price1", round(col("ask_price1"), 4))
    
    fact_quote_df = fact_quote_df.withColumn("hask_key", md5(concat_ws("||", *fact_quote_df.columns)))
    
    fact_quote_df = fact_quote_df.dropDuplicates(["hask_key"])

    for date_key in date_list:
        print(f"Processing on day {date_key} ...")

        df_day = fact_quote_df.filter(col("tradingdate_key") == date_key)

        
        query = f"""
                (
                    SELECT md5(concat_ws('||', tradingdate_key, time_key, symbol_key, ask_price1, ask_vol1)) AS hask_key,
                        tradingdate_key,
                        time_key,
                        symbol_key,
                        ask_price1,
                        ask_vol1
                    FROM fact.stockorderbook
                    WHERE tradingdate_key = {date_key}
                ) AS existing
                """
                
        existing_df = spark.read.jdbc(url=dw_db_url, 
                                      table=query, 
                                      properties=dw_db_properties)

        new_df = df_day.join(existing_df, 
                             on=["tradingdate_key", "time_key", "symbol_key"
                                ,"ask_price1", "ask_vol1"], 
                             how="left_anti")

        count_new = new_df.count()

        if count_new > 0:
            new_df.drop("hask_key") \
                .repartition(2, "symbol_key") \
                .write \
                .mode("append") \
                .option("batchsize", 500) \
                .option("isolationLevel", "NONE") \
                .jdbc(url=dw_db_url, table="fact.stockorderbook", properties=dw_db_properties)

        # Free up RAM after each loop.
        new_df.unpersist(blocking=True)
        df_day.unpersist(blocking=True)
        existing_df.unpersist(blocking=True)
        spark.catalog.clearCache()
        
        
    
    spark.stop()

