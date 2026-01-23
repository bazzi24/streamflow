from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os

load_dotenv()


def dim_symbol():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Symbol") \
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

    symbol_raw_df = spark.read.jdbc(url=raw_db_url, 
                                    table="corporation.corporation", 
                                    properties=raw_db_properties)
    
    sector_raw_df = spark.read.jdbc(url=raw_db_url, 
                                    table="corporation.sector", 
                                    properties=raw_db_properties)

    symbol_raw_df_alias = symbol_raw_df.alias("sym")
    sector_raw_df_alias = sector_raw_df.alias("sec")

    raw_df = symbol_raw_df_alias.join(
        sector_raw_df_alias,
        on="sector_id",
        how="left"
    ).select(
        col("sym.symbol_id"),
        col("sym.symbol_name"),
        col("sym.symbol_en_name"),
        col("sec.sector_name")
    ).distinct()

    df = raw_df.select (
        col("symbol_id").alias("symbol"),
        col("symbol_name").alias("symbol_name"),
        col("symbol_en_name").alias("symbol_en_name"),
        col("sector_name").alias("sector")
    )



    existing_dim_symbol_df = spark.read.jdbc(url=dw_db_url, 
                                             table="dim.symbol", 
                                             properties=dw_db_properties
                                             ).select("symbol")

    new_dim_symbol_df = df.join(existing_dim_symbol_df, 
                                on="symbol", 
                                how="left_anti")
    
    new_dim_symbol_df.write.jdbc(url=dw_db_url, 
                                 table="dim.symbol", 
                                 mode="append", 
                                 properties=dw_db_properties)

    spark.stop()
