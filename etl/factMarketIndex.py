from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from dotenv import load_dotenv
import os

load_dotenv()

def fact_marketindex():
    spark = SparkSession.builder \
        .appName("ETL_Fact_Market_Index") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
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
    
    index_dw_df = spark.read.jdbc(url=dw_db_url, 
                                  table="dim.index", 
                                  properties=dw_db_properties)
    
    exchange_dw_df = spark.read.jdbc(url=dw_db_url, 
                                     table="dim.exchange", 
                                     properties=dw_db_properties)
    
    session_dw_df = spark.read.jdbc(url=dw_db_url, 
                                    table="dim.tradingsession", 
                                    properties=dw_db_properties)


    index_raw_df = spark.read.jdbc(url=raw_db_url, 
                                   table="streaming.index_data", 
                                   properties=raw_db_properties)

    fact_index = index_raw_df.alias("ir") \
        .join(date_dw_df.alias("d"), col("ir.trading_date") == col("d.tradingdate"), "left") \
        .join(time_dw_df.alias("t"), col("ir.time") == col("t.time_hh_mm_ss"), "left") \
        .join(index_dw_df.alias("i"), col("ir.index_name") == col("i.index_name"), "left") \
        .join(exchange_dw_df.alias("e"), col("ir.exchange") == col("e.exchange_name"), "left") \
        .join(session_dw_df.alias("s"), col("ir.trading_session") == col("s.trading_session"), "left")
        
        
    fact_indexmarket_df = fact_index.select(
        col("d.tradingdate_key").alias("tradingdate_key"),
        col("t.time_key").alias("time_key"),
        col("i.index_key").alias("index_key"),
        col("ir.index_value").alias("index_value"),
        col("ir.prior_index_value").alias("prio_index_value"),
        col("ir.change").alias("change"),
        col("ir.ratio_change").alias("ratio_change"),
        col("ir.total_qtty").alias("total_qtty"),
        col("ir.total_value").alias("total_value"),
        col("ir.total_qtty_pt").alias("total_qtty_pt"),
        col("ir.total_value_pt").alias("total_value_pt"),
        col("ir.advances").alias("advances"),
        col("ir.nochanges").alias("nochanges"),
        col("ir.declines").alias("declines"),
        col("ir.ceilings").alias("ceilings"),
        col("ir.floors").alias("floors"),
        col("e.exchange_key").alias("exchange_key"),
        col("s.trading_session_key").alias("trading_session_key")
    ).distinct()

    #fact_indexmarket_df.show()

    existing_index_df = spark.read.jdbc(url=dw_db_url, 
                                        table="fact.marketindex", 
                                        properties=dw_db_properties
                                        ).select("tradingdate_key", 
                                                 "time_key", 
                                                 "index_key")

    new_index_df = fact_indexmarket_df.join(existing_index_df, 
                                            on=["tradingdate_key", "time_key", "index_key"], 
                                            how="left_anti")

    #new_index_df.filter(col("ratio_change") == float("inf")).count()
    
    new_index_df.write.jdbc(url=dw_db_url, 
                            table="fact.marketindex", 
                            mode="append", 
                            properties=dw_db_properties)
    
    #print("So luong market index moi: ", new_index_df.count())

    spark.stop()

