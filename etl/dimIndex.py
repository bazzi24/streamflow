from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os


def dim_index():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Index") \
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
                             table="streaming.index_data", 
                             properties=raw_db_properties)

    dim_index_df = raw_df.select("index_name") \
        .distinct() \
        .withColumn("index_name", col("index_name"))

    existing_dim_index_df = spark.read.jdbc(url=dw_db_url, 
                                            table="dim.index", 
                                            properties=dw_db_properties
                                            ).select("index_name")

    new_dim_index_df = dim_index_df.join(existing_dim_index_df, 
                                        on="index_name", 
                                        how="left_anti")
    
    new_dim_index_df.write.jdbc(url=dw_db_url, 
                                table="dim.index", 
                                mode="append", 
                                properties=dw_db_properties)

    spark.stop()