import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, minute, second, col, date_format, to_timestamp

load_dotenv()

MYSQL_JAR = os.getenv("MYSQL_JAR", "/streamflow/lib/mysql-connector-j-8.0.33.jar")


def dim_time():
    spark = SparkSession.builder \
        .appName("ETL_Dim_Time") \
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
                             table="data.data_quote",
                             properties=raw_db_properties)

    df = raw_df.withColumnRenamed("time", "time_hh_mm_ss")

    dim_time_df = df.select("time_hh_mm_ss") \
        .distinct() \
        .withColumn("time_hh_mm_ss", to_timestamp(col("time_hh_mm_ss"), "HH:mm:ss")) \
        .withColumn("time_key", date_format(col("time_hh_mm_ss"), "HHmmss").cast("int")) \
        .withColumn("Hour", hour(col("time_hh_mm_ss"))) \
        .withColumn("Minute", minute(col("time_hh_mm_ss"))) \
        .withColumn("Second", second(col("time_hh_mm_ss")))

    existing_dim_time_df = spark.read.jdbc(url=dw_db_url,
                                           table="warehouse.dim.`time`",
                                           properties=dw_db_properties
                                           ).withColumn("time_key",
                                                        date_format(col("time_hh_mm_ss"),
                                                                    "HHmmss").cast("int"))

    new_dim_time_df = dim_time_df.join(existing_dim_time_df,
                                       on="time_key",
                                       how="left_anti")

    new_dim_time_df.write.jdbc(url=dw_db_url,
                               table="dim.`time`",
                               mode="append",
                               properties=dw_db_properties)

    spark.stop()
