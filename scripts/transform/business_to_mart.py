# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, round, year, month, dayofmonth, weekofyear, avg, sum, lag, when, lit
# from pyspark.sql.window import Window
# import os

# def transform_business_to_mart(business_path: str, mart_output_path: str):
#     """
#     Chuyển dữ liệu từ tầng Business sang tầng Mart:
#     - Tổng hợp theo tháng
#     - Tính trung bình giá Open, High, Low, Close
#     - Tính trung bình Change, ChangePercent, GapPercent
#     - Tính tổng khối lượng giao dịch (Total_Volume)
#     """

#     spark = SparkSession.builder.appName("BusinessToMartStockETL").getOrCreate()

#     try:
#         abs_business = os.path.abspath(business_path)
#         abs_mart = os.path.abspath(mart_output_path)

#         if not os.path.exists(abs_business):
#             raise FileNotFoundError(f"❌ Không tìm thấy dữ liệu Business tại: {abs_business}")
#         print(f"📥 Đang đọc dữ liệu Business từ: {abs_business}")

#         df = spark.read.parquet(abs_business)
#         original_count = df.count()
#         print(f"🔢 Số dòng ban đầu: {original_count}")

#         if original_count == 0:
#             raise ValueError("❌ Dữ liệu Business rỗng.")

#         # Kiểm tra schema bắt buộc
#         required_columns = {"Date", "Open", "High", "Low", "Close", "Volume"}
#         missing = required_columns - set(df.columns)
#         if missing:
#             raise ValueError(f"❌ Thiếu các cột bắt buộc trong Business: {missing}")

#         # Làm sạch dữ liệu
#         df = df.dropna(subset=["Date", "Open", "High", "Low", "Close", "Volume"])
#         df = df.filter(
#             (col("Open") >= 0) &
#             (col("High") >= 0) &
#             (col("Low") >= 0) &
#             (col("Close") >= 0) &
#             (col("Volume") >= 0)
#         )

#         # Tính thêm các cột chỉ số biến động
#         window_spec = Window.orderBy("Date")
#         df = df.withColumn("Prev_Close", lag("Close").over(window_spec)) \
#                .withColumn("Change", round(col("Close") - col("Open"), 6)) \
#                .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4)) \
#                .withColumn("GapPercent", when(col("Prev_Close").isNotNull(),
#                                               round((col("Close") - col("Prev_Close")) / col("Prev_Close") * 100, 4))
#                            .otherwise(lit(None))) \
#                .withColumn("Year", year(col("Date"))) \
#                .withColumn("Month", month(col("Date"))) \
#                .withColumn("Day", dayofmonth(col("Date"))) \
#                .withColumn("Week", weekofyear(col("Date")))

#         cleaned_count = df.count()
#         print(f"✅ Sau làm sạch: {cleaned_count} dòng (mất {original_count - cleaned_count} dòng)")

#         # Tổng hợp theo tháng
#         df_monthly = df.groupBy("Year", "Month").agg(
#             avg("Open").alias("Average_Open"),
#             avg("High").alias("Average_High"),
#             avg("Low").alias("Average_Low"),
#             avg("Close").alias("Average_Close"),
#             sum("Volume").alias("Total_Volume"),
#             avg("Change").alias("Average_Change"),
#             avg("ChangePercent").alias("Average_ChangePercent"),
#             avg("GapPercent").alias("Average_GapPercent")
#         ).orderBy("Year", "Month")

#         print("📊 Dữ liệu Mart (Monthly Summary):")
#         df_monthly.show(5)

#         # Ghi xuống tầng Mart
#         df_monthly.write.mode("overwrite").parquet(abs_mart)
#         print(f"💾 Đã ghi dữ liệu Mart vào: {abs_mart}")

#     except Exception as e:
#         print(f"❌ Lỗi Business → Mart: {e}")

#     finally:
#         spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, avg, sum as spark_sum, max as spark_max
)
import os

def transform_business_to_mart(business_path: str, mart_output_path: str):
    spark = SparkSession.builder.appName("BusinessToMartStockETL").getOrCreate()
    try:
        if not os.path.exists(business_path):
            raise FileNotFoundError(f"❌ Không tìm thấy dữ liệu Business tại: {business_path}")
        df = spark.read.parquet(business_path)

        # Đảm bảo đủ cột
        required_cols = ["Adj Close", "VWAP", "MA20", "Return", "CumulativeReturn", "GapPercent", "Volume"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"❌ Thiếu các cột bắt buộc: {missing}")

        df = df.withColumn("Year", year(col("Date"))) \
               .withColumn("Month", month(col("Date")))

        df_monthly = df.groupBy("Year", "Month").agg(
            avg("Adj Close").alias("Avg_Adj_Close"),
            avg("VWAP").alias("Average_VWAP"),
            avg("MA20").alias("Average_MA20"),
            avg("Return").alias("Average_Return"),
            avg("GapPercent").alias("Average_GapPercent"),
            avg("CumulativeReturn").alias("Average_CumulativeReturn"),
            spark_sum("Volume").alias("Total_Volume"),
            spark_max("Date").alias("Last_Date")
        )

        df_monthly.write.mode("overwrite").parquet(mart_output_path)
        print(f"✅ Đã ghi dữ liệu Mart vào: {mart_output_path}")
        return df_monthly

    except Exception as e:
        print(f"❌ Lỗi Business → Mart: {e}")
    finally:
        spark.stop()
