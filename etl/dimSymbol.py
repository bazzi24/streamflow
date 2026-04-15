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

    # Step 1: Pull only symbols that appear in recent trading data (past 60 days).
    # This avoids scanning the full corporation table — new symbols show up in trade
    # data before they're formally registered in the corporation table.
    from_day = "DATE_SUB(CURRENT_DATE, INTERVAL 60 DAY)"
    recent_symbols_q = (
        f"(SELECT DISTINCT symbol FROM data.data_trade "
        f"WHERE trading_date >= {from_day}) AS recent_symbols"
    )
    recent_symbols_df = spark.read.jdbc(
        url=raw_db_url,
        table=recent_symbols_q,
        properties=raw_db_properties,
    )

    # Step 2: Load corporation and sector rows — filter to recently-active symbols
    # only so we don't read the entire table.
    symbol_raw_df = spark.read.jdbc(
        url=raw_db_url,
        table="data.corporation.corporation",
        properties=raw_db_properties,
    )
    sector_raw_df = spark.read.jdbc(
        url=raw_db_url,
        table="data.corporation.sector",
        properties=raw_db_properties,
    )

    # Join corporation → sector, then filter to only symbols that traded recently
    raw_df = (
        symbol_raw_df.alias("sym")
        .join(sector_raw_df.alias("sec"), on="sector_id", how="left")
        .join(recent_symbols_df.alias("rs"), col("sym.symbol_id") == col("rs.symbol"), how="inner")
        .select(
            col("sym.symbol_id"),
            col("sym.symbol_name"),
            col("sym.symbol_en_name"),
            col("sec.sector_name"),
        )
        .distinct()
    )

    dim_symbol_df = raw_df.select(
        col("symbol_id").alias("symbol"),
        col("symbol_name").alias("symbol_name"),
        col("symbol_en_name").alias("symbol_en_name"),
        col("sector_name").alias("sector"),
    )

    existing_dim_symbol_df = spark.read.jdbc(
        url=dw_db_url,
        table="warehouse.dim.symbol",
        properties=dw_db_properties,
    ).select("symbol")

    new_dim_symbol_df = dim_symbol_df.join(
        existing_dim_symbol_df,
        on="symbol",
        how="left_anti",
    )

    new_dim_symbol_df.write.jdbc(
        url=dw_db_url,
        table="dim.symbol",
        mode="append",
        properties=dw_db_properties,
    )

    spark.stop()
