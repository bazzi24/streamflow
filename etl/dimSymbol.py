import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

MYSQL_JAR = os.getenv("MYSQL_JAR", "/streamflow/lib/mysql-connector-j-8.0.33.jar")


def dim_symbol():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Symbol") \
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

    # corporation.* tables are in a separate MySQL database but accessible via cross-db queries
    symbol_raw_df = spark.read.jdbc(url=raw_db_url,
                                    table="data.corporation.corporation",
                                    properties=raw_db_properties)

    sector_raw_df = spark.read.jdbc(url=raw_db_url,
                                    table="data.corporation.sector",
                                    properties=raw_db_properties)

    raw_df = symbol_raw_df.alias("sym").join(
        sector_raw_df.alias("sec"),
        on="sector_id",
        how="left"
    ).select(
        col("sym.symbol_id"),
        col("sym.symbol_name"),
        col("sym.symbol_en_name"),
        col("sec.sector_name"),
    ).distinct()

    dim_symbol_df = raw_df.select(
        col("symbol_id").alias("symbol"),
        col("symbol_name").alias("symbol_name"),
        col("symbol_en_name").alias("symbol_en_name"),
        col("sector_name").alias("sector"),
    )

    existing_dim_symbol_df = spark.read.jdbc(url=dw_db_url,
                                             table="warehouse.dim.symbol",
                                             properties=dw_db_properties
                                             ).select("symbol")

    new_dim_symbol_df = dim_symbol_df.join(existing_dim_symbol_df,
                                on="symbol",
                                how="left_anti")

    new_dim_symbol_df.write.jdbc(url=dw_db_url,
                                 table="dim.symbol",
                                 mode="append",
                                 properties=dw_db_properties)

    spark.stop()
