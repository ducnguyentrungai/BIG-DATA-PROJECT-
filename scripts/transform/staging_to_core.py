# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, year, month, dayofmonth, weekofyear, round
# from pyspark.sql.types import DoubleType
# import os

# def transform_staging_to_core(staging_path: str, core_output_path: str):
#     spark = SparkSession.builder.appName("StagingToCoreStockETL").getOrCreate()

#     try:
#         if not os.path.exists(staging_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy file staging: {staging_path}")
#         print("📥 Đang đọc dữ liệu staging...")

#         df = spark.read.parquet(staging_path)
#         df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

#         print("📜 Schema staging:")
#         df.printSchema()
#         original_count = df.count()
#         print(f"🔢 Dữ liệu gốc: {original_count} dòng")

#         # Chuẩn hóa kiểu dữ liệu
#         df = df \
#             .withColumn("Open", col("Open").cast(DoubleType())) \
#             .withColumn("High", col("High").cast(DoubleType())) \
#             .withColumn("Low", col("Low").cast(DoubleType())) \
#             .withColumn("Close", col("Close").cast(DoubleType())) \
#             .withColumn("Volume", col("Volume").cast(DoubleType()))

#         # Làm sạch mềm (chỉ loại bỏ dòng có Date null và dữ liệu âm)
#         df_cleaned = df \
#             .filter(col("Date").isNotNull()) \
#             .filter((col("Open") >= 0) & (col("Close") >= 0) &
#                     (col("High") >= 0) & (col("Low") >= 0) & (col("Volume") >= 0)) \
#             .dropDuplicates(["Date"])

#         cleaned_count = df_cleaned.count()
#         print(f"✅ Dữ liệu sau làm sạch: {cleaned_count} dòng (mất {original_count - cleaned_count})")

#         # Bổ sung phân tích thời gian
#         df_cleaned = df_cleaned \
#             .withColumn("Year", year(col("Date"))) \
#             .withColumn("Month", month(col("Date"))) \
#             .withColumn("Day", dayofmonth(col("Date"))) \
#             .withColumn("Week", weekofyear(col("Date")))

#         # Tính biến động giá
#         df_cleaned = df_cleaned \
#             .withColumn("Change", round(col("Close") - col("Open"), 6)) \
#             .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4))

#         print("Kiểm tra 5 hàng đầu\n", df_cleaned.show(5))
#         df_cleaned.write.mode("overwrite").parquet(core_output_path)
#         print(f"💾 Đã ghi dữ liệu Core tại: {core_output_path}")
#         return df_cleaned

#     except Exception as e:
#         print(f"❌ Lỗi Staging → Core: {e}")
#         return None
#     finally:
#         spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, weekofyear, lag, round, log, exp, avg, stddev, sum as spark_sum
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import os

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

