import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

load_dotenv()

MYSQL_JAR = os.getenv("MYSQL_JAR", "/streamflow/lib/mysql-connector-j-8.0.33.jar")


def dim_exchange():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Exchange") \
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

    df = raw_df.withColumnRenamed("exchange", "exchange_name")

    dim_exchange_df = df.select("exchange_name").distinct()

    existing_dim_exchange_df = spark.read.jdbc(url=dw_db_url,
                                               table="warehouse.dim.exchange",
                                               properties=dw_db_properties
                                               ).select("exchange_name")

    new_dim_exchange_df = dim_exchange_df.join(existing_dim_exchange_df,
                                               on="exchange_name",
                                               how="left_anti")

    new_dim_exchange_df.write.jdbc(url=dw_db_url,
                                   table="dim.exchange",
                                   mode="append",
                                   properties=dw_db_properties)

    spark.stop()
