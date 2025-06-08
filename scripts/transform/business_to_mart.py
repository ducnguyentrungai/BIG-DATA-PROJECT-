from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum as spark_sum, max as spark_max, min as spark_min,
    year, month
)
import os

# def transform_business_to_mart(business_path: str, mart_output_path: str):
#     spark = SparkSession.builder.appName("BusinessToMartStockETL").getOrCreate()
#     try:
#         if not os.path.exists(business_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy dữ liệu Business tại: {business_path}")
#         df = spark.read.parquet(business_path)

#         # Đảm bảo đủ cột
#         required_cols = ["Adj Close", "VWAP", "MA20", "Return", "CumulativeReturn", "GapPercent", "Volume"]
#         missing = [c for c in required_cols if c not in df.columns]
#         if missing:
#             raise ValueError(f"❌ Thiếu các cột bắt buộc: {missing}")

#         df = df.withColumn("Year", year(col("Date"))) \
#                .withColumn("Month", month(col("Date")))

#         df_monthly = df.groupBy("Year", "Month").agg(
#             avg("Adj Close").alias("Avg_Adj_Close"),
#             avg("VWAP").alias("Average_VWAP"),
#             avg("MA20").alias("Average_MA20"),
#             avg("Return").alias("Average_Return"),
#             avg("GapPercent").alias("Average_GapPercent"),
#             avg("CumulativeReturn").alias("Average_CumulativeReturn"),
#             spark_sum("Volume").alias("Total_Volume"),
#             spark_max("Date").alias("Last_Date")
#         )

#         df_monthly.write.mode("overwrite").parquet(mart_output_path)
#         print(f"✅ Đã ghi dữ liệu Mart vào: {mart_output_path}")
#         return df_monthly

#     except Exception as e:
#         print(f"❌ Lỗi Business → Mart: {e}")
#     finally:
#         spark.stop()

def transform_business_to_mart(business_path: str, mart_output_path: str):
    spark = SparkSession.builder.appName("BusinessToMartStockETL").getOrCreate()
    try:
        if not os.path.exists(business_path):
            raise FileNotFoundError(f"❌ Không tìm thấy dữ liệu Business tại: {business_path}")
        df = spark.read.parquet(business_path)

        # Đảm bảo đủ cột
        required_cols = [
            "Adj Close", "VWAP", "MA20", "Return", "CumulativeReturn",
            "GapPercent", "Volume", "UpperBand", "LowerBand", "STD20"
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"❌ Thiếu các cột bắt buộc: {missing}")

        # Thêm Year, Month nếu chưa có
        df = df.withColumn("Year", year(col("Date"))) \
               .withColumn("Month", month(col("Date")))

        # Tổng hợp dữ liệu theo tháng (đầy đủ đặc trưng hơn)
        df_monthly = df.groupBy("Year", "Month").agg(
            avg("Adj Close").alias("Avg_Adj_Close"),
            avg("VWAP").alias("Average_VWAP"),
            avg("MA20").alias("Average_MA20"),
            avg("STD20").alias("Average_STD20"),
            avg("Return").alias("Average_Return"),
            avg("GapPercent").alias("Average_GapPercent"),
            avg("CumulativeReturn").alias("Average_CumulativeReturn"),
            spark_sum("Volume").alias("Total_Volume"),
            spark_max("UpperBand").alias("Max_UpperBand"),
            spark_min("LowerBand").alias("Min_LowerBand"),
            spark_max("Date").alias("Last_Date")
        )

        df_monthly.write.mode("overwrite").parquet(mart_output_path)
        print(f"✅ Đã ghi dữ liệu Mart vào: {mart_output_path}")
        return df_monthly

    except Exception as e:
        print(f"❌ Lỗi Business → Mart: {e}")
    finally:
        spark.stop()
