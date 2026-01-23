from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os

load_dotenv()



def dim_session():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Session") \
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

    dim_session_df = raw_df.select("trading_session") \
        .distinct() \
        .withColumn("trading_session", col("trading_session"))
        
    existing_dim_session_df = spark.read.jdbc(url=dw_db_url, 
                                              table="dim.tradingsession", 
                                              properties=dw_db_properties
                                              ).select("trading_session")

    new_dim_session_df = dim_session_df.join(existing_dim_session_df, 
                                             on="trading_session", 
                                             how="left_anti")
    
    new_dim_session_df.write.jdbc(url=dw_db_url, 
                                  table="dim.tradingsession", 
                                  mode="append", 
                                  properties=dw_db_properties)
    

    spark.stop()