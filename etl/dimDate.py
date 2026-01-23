from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, quarter, dayofweek, date_format, col
from dotenv import load_dotenv
import os

load_dotenv()

def dim_date():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Date") \
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

    raw_df = spark.read.jdbc(url=raw_db_url, 
                             table="streaming.data_trade", 
                             properties=raw_db_properties)
    
    df = raw_df.withColumnRenamed("trading_date", "tradingdate")
    
    dim_date_df = df.select("tradingdate") \
        .distinct() \
        .withColumn("tradingdate_key", date_format(col("tradingdate"), "yyyyMMdd").cast("int")) \
        .withColumn("Year", year(col("tradingdate"))) \
        .withColumn("Quarter", quarter(col("tradingdate"))) \
        .withColumn("Month", month(col("tradingdate"))) \
        .withColumn("Day", dayofmonth(col("tradingdate"))) \
        .withColumn("Weekday", dayofweek(col("tradingdate")))
        
    existing_dim_date_df = spark.read.jdbc(url=dw_db_url, 
                                           table="dim.date", 
                                           properties=dw_db_properties
                                           ).select("tradingdate_key")

    new_dim_date_df = dim_date_df.join(existing_dim_date_df, 
                                       on="tradingdate_key", 
                                       how="left_anti")
    
    new_dim_date_df.write.jdbc(url=dw_db_url, 
                               table="dim.date", 
                               mode="append", 
                               properties=dw_db_properties)
    spark.stop()