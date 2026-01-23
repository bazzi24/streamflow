from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from dotenv import load_dotenv
import os

load_dotenv()


def fact_trade():
    spark = SparkSession.builder \
        .appName("ETL_Fact_Trade") \
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

    
    querytrade = """
    (
            SELECT *
            FROM streaming.data_trade
            WHERE trading_date::date >= (CURRENT_DATE - INTERVAL '5 day')
    ) AS recent_trades        
    """

    trade_raw_df = spark.read.jdbc(url=raw_db_url, 
                                   table=querytrade, 
                                   properties=raw_db_properties)

    fact_trade = trade_raw_df.alias("tr") \
        .join(date_dw_df.alias("d"), col("tr.trading_date") == col("d.tradingdate"), "left") \
        .join(time_dw_df.alias("t"), col("tr.time") == col("t.time_hh_mm_ss"), "left") \
        .join(symbol_dw_df.alias("s"), trim(col("tr.symbol")) == trim(col("s.symbol"))) \
        .join(exchange_dw_df.alias("e"), trim(col("tr.exchange")) == trim(col("e.exchange_name")), "left") \
        .join(session_dw_df.alias("se"), trim(col("tr.trading_session")) == trim(col("se.trading_session")), "left")
    fact_trade_df = fact_trade.select (
        col("d.tradingdate_key").alias("tradingdate_key"),
        col("t.time_key").alias("time_key"),
        col("s.symbol_key").alias("symbol_key"),
        col("e.exchange_key").alias("exchange_key"),
        col("se.trading_session_key").alias("trading_session_key"),
        col("tr.last_price").alias("last_price"),
        col("tr.avg_price").alias("avg_price"),
        col("tr.ceiling").alias("ceiling"),
        col("tr.floor").alias("floor"),
        col("tr.ref_price").alias("ref_price"),
        col("tr.prior_val").alias("prio_val"),
        col("tr.last_vol").alias("last_vol"),
        col("tr.total_val").alias("total_val"),
        col("tr.change").alias("change"),
        col("tr.ratio_change").alias("ratio_change"),
        col("tr.highest").alias("highest"),
        col("tr.lowest").alias("lowest")
    ).dropDuplicates(["tradingdate_key", "time_key", "symbol_key", "last_vol", "total_val"])
    
    date_list = [row[0] for row in fact_trade_df.select("tradingdate_key").distinct().collect()]
    
    
    for date_key in date_list:
        print(f"Processing on day {date_key} . . .")
        
        df_day = fact_trade_df.filter(col("tradingdate_key") == date_key)
        
        query = f"""
                    (SELECT tradingdate_key, time_key, symbol_key, last_price, 
                            avg_price, last_vol, change FROM fact.stocktrade 
                    WHERE tradingdate_key = {date_key}) as existing
                    """
        
        existing_trade_df = spark.read.jdbc(url=dw_db_url, 
                                            table=query, 
                                            properties=dw_db_properties)

        new_trade_df = fact_trade_df.join(existing_trade_df, 
                                          on=["tradingdate_key", "time_key", "symbol_key"
                                                ,"last_price", "avg_price", "last_vol",
                                                "change"],
                                            how="left_anti")

        count_new = new_trade_df.count()
        print(f"Day {date_key}: have {count_new} records")
        
        if count_new > 0:
            new_trade_df.repartition(2, "symbol_key") \
                .write \
                .mode("append") \
                .option("batchsize", 500) \
                .option("isolationLevel", "NONE") \
                .jdbc(url=dw_db_url, 
                      table="fact.stocktrade", 
                      properties=dw_db_properties)
            

    spark.stop()

