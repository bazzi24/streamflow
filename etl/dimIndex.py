import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

MYSQL_JAR = os.getenv("MYSQL_JAR", "/streamflow/lib/mysql-connector-j-8.0.33.jar")


def dim_index():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Index") \
        .master(os.getenv("SPARK_MASTER_URL", "local[*]")) \
        .config("spark.jars", MYSQL_JAR) \
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

    raw_df = spark.read.jdbc(url=raw_db_url,
                             table="data.index_data",
                             properties=raw_db_properties)

    dim_index_df = raw_df.select("index_name").distinct()

    # Table renamed from dim.index to dim.market_index (index is a reserved keyword in MySQL)
    existing_dim_index_df = spark.read.jdbc(url=dw_db_url,
                                            table="warehouse.dim.market_index",
                                            properties=dw_db_properties
                                            ).select("index_name")

    new_dim_index_df = dim_index_df.join(existing_dim_index_df,
                                         on="index_name",
                                         how="left_anti")

    new_dim_index_df.write.jdbc(url=dw_db_url,
                                table="dim.market_index",
                                mode="append",
                                properties=dw_db_properties)

    spark.stop()
