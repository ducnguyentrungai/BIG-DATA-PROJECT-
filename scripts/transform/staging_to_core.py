from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, weekofyear, lag, round, log, exp, avg, stddev, sum as spark_sum
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import os
import pandas as pd

def transform_staging_to_core(staging_path: str, core_output_path: str):
    spark = SparkSession.builder.appName("StagingToCoreStockETL").getOrCreate()
    try:
        if not os.path.exists(staging_path):
            raise FileNotFoundError(f"❌ Không tìm thấy file staging: {staging_path}")
        df = spark.read.parquet(staging_path)

        df = df.withColumn("Date", col("Date").cast("date"))
        df = df.withColumn("Open", col("Open").cast(DoubleType())) \
               .withColumn("High", col("High").cast(DoubleType())) \
               .withColumn("Low", col("Low").cast(DoubleType())) \
               .withColumn("Close", col("Close").cast(DoubleType())) \
               .withColumn("Adj Close", col("Close").cast(DoubleType())) \
               .withColumn("Volume", col("Volume").cast(DoubleType()))

        df_cleaned = df.dropDuplicates(["Date"]) \
            .filter(col("Date").isNotNull()) \
            .filter((col("Open") >= 0) & (col("Close") >= 0) &
                    (col("High") >= 0) & (col("Low") >= 0) &
                    (col("Adj Close") >= 0) & (col("Volume") >= 0))

        window_lag = Window.orderBy("Date")
        window_rolling = Window.orderBy("Date").rowsBetween(-19, 0)

        df_cleaned = df_cleaned \
            .withColumn("Prev_Close", lag("Adj Close").over(window_lag)) \
            .withColumn("Gap", col("Open") - col("Prev_Close")) \
            .withColumn("Return", round((col("Adj Close") / col("Prev_Close") - 1) * 100, 4)) \
            .withColumn("LogReturn", log(col("Adj Close") / col("Prev_Close"))) \
            .withColumn("CumLogReturn", spark_sum("LogReturn").over(window_lag)) \
            .withColumn("CumulativeReturn", round(exp(col("CumLogReturn")) - 1, 6)) \
            .withColumn("VWAP", spark_sum(col("Adj Close") * col("Volume")).over(window_rolling) /
                                spark_sum("Volume").over(window_rolling)) \
            .withColumn("MA20", avg("Adj Close").over(window_rolling)) \
            .withColumn("STD20", stddev("Adj Close").over(window_rolling)) \
            .withColumn("UpperBand", col("MA20") + 2 * col("STD20")) \
            .withColumn("LowerBand", col("MA20") - 2 * col("STD20")) \
            .withColumn("GapPercent", round(col("Gap") / col("Prev_Close") * 100, 4)) \
            .withColumn("Year", year(col("Date"))) \
            .withColumn("Month", month(col("Date"))) \
            .withColumn("Day", dayofmonth(col("Date"))) \
            .withColumn("Week", weekofyear(col("Date")))

        df_cleaned.write.mode("overwrite").parquet(core_output_path)
        return df_cleaned

    except Exception as e:
        print(f"❌ Lỗi Core: {e}")
        return None
    finally:
        spark.stop()

if __name__ == "__main__":
    # df = transform_staging_to_core(staging_path='/home/trungduc/airflow/warehouse/staging/aapl.parquet',
    #                           core_output_path='/home/trungduc/airflow/warehouse/core/test.parquet')
   df = pd.read_parquet('/home/trungduc/airflow/warehouse/core/aapl_core.parquet')
   print(df.head(5))