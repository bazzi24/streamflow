from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, trim, desc, row_number

def feature():
    
    spark = (
        SparkSession.builder
        .appName("ETL_Fact_Trade")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23")
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
        .config("spark.sql.debug.maxToStringFields", "2000")
        .getOrCreate()
    )

    
    dw_db_url = "jdbc:postgresql://localhost:5432/data_warehouse_ssi"
    dw_db_properties = {
        "user": "bazzi",
        "password": "bazzi123",
        "driver": "org.postgresql.Driver"
    }

    ml_db_url = "jdbc:postgresql://localhost:5432/stock_ml"
    ml_db_properties = {
        "user": "bazzi",
        "password": "bazzi123",
        "driver": "org.postgresql.Driver"
    }

    
    fact_trade_df = spark.read.jdbc(url=dw_db_url, table="fact.stocktrade", properties=dw_db_properties)
    dim_symbol_df = spark.read.jdbc(url=dw_db_url, table="dim.symbol", properties=dw_db_properties)
    dim_date_df = spark.read.jdbc(url=dw_db_url, table="dim.date", properties=dw_db_properties)
    dim_time_df = spark.read.jdbc(url=dw_db_url, table="dim.time", properties=dw_db_properties)

    

  
    joined_df = (
        fact_trade_df.alias("ft")
        .join(dim_symbol_df.alias("ds"), col("ft.symbol_key") == col("ds.symbol_key"), "left")
        .join(dim_date_df.alias("dd"), col("ft.tradingdate_key") == col("dd.tradingdate_key"), "left")
        .join(dim_time_df.alias("tt"), col("ft.time_key") == col("tt.time_key"), "left")
        .select(
            trim(col("ds.symbol")).alias("symbol"),
            col("dd.tradingdate"),
            col("tt.time_hh_mm_ss"),
            col("ft.last_price"),
            col("ft.avg_price"),
            col("ft.ref_price"),
            col("ft.total_val"),
            col("ft.change"),
            col("ft.ratio_change"),
            col("ft.highest"),
            col("ft.lowest"),
        )
        .filter(col("tradingdate").isNotNull())
    )

    print("joined_df columns:", joined_df.columns)
    joined_df.printSchema()

    
    joined_df = joined_df.withColumn("time_hh_mm_ss", col("time_hh_mm_ss").cast("string"))
    joined_df = joined_df.withColumn("time_hh_mm_ss", col("time_hh_mm_ss").cast("timestamp"))

    
    window_spec = Window.partitionBy("symbol", "tradingdate").orderBy(col("time_hh_mm_ss").desc())

    daily_last_tick_df = (
        joined_df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    ).cache()


    
    existing_feature_df = (
        spark.read.jdbc(url=ml_db_url, table="ml_data.feature_data", properties=ml_db_properties)
        .select("tradingdate", "symbol")
        .dropDuplicates()
    )

    
    new_feature_df = daily_last_tick_df.join(
        existing_feature_df, on=["tradingdate", "symbol"], how="left_anti"
    ).cache()

    print("Số lượng feature mới:", new_feature_df.count())

    if new_feature_df.count() > 0:
        
        columns_to_write = [
            "symbol",
            "tradingdate",
            "last_price",
            "avg_price",
            "ref_price",
            "total_val",
            "change",
            "ratio_change",
            "highest",
            "lowest"
        ]
        
        final_feature_df = new_feature_df.select(*columns_to_write)

        
        final_feature_df.write.jdbc(
            url=ml_db_url,
            table="ml_data.feature_data",
            mode="append",
            properties=ml_db_properties
        )

        print("Dữ liệu mới đã được ghi vào ml_data.feature_data thành công")
    else:
        print("Không có dữ liệu mới để ghi")

    spark.stop()

if __name__ == "__main__":
    feature()
