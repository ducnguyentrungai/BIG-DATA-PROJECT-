from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, sum
import os

# def transform_business_to_mart(business_path: str, mart_output_path: str):
#     """
#     Chuyển dữ liệu từ tầng Business sang tầng Mart:
#     - Tổng hợp theo tháng
#     - Tính trung bình giá Open, High, Low, Close
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

#         cleaned_count = df.count()
#         print(f"✅ Sau làm sạch: {cleaned_count} dòng (mất {original_count - cleaned_count} dòng)")

#         # Thêm cột Year/Month nếu chưa có
#         if "Year" not in df.columns or "Month" not in df.columns:
#             df = df \
#                 .withColumn("Year", year(col("Date"))) \
#                 .withColumn("Month", month(col("Date")))

#         # Tổng hợp theo tháng
#         df_monthly = df.groupBy("Year", "Month").agg(
#             avg("Open").alias("Average_Open"),
#             avg("High").alias("Average_High"),
#             avg("Low").alias("Average_Low"),
#             avg("Close").alias("Average_Close"),
#             sum("Volume").alias("Total_Volume")
#         ).orderBy("Year", "Month")

#         print("📊 Dữ liệu Mart (Monthly Summary):")
#         df_monthly.show(10)

#         # Ghi xuống tầng Mart
#         df_monthly.write.mode("overwrite").parquet(abs_mart)
#         print(f"💾 Đã ghi dữ liệu Mart vào: {abs_mart}")

#     except Exception as e:
#         print(f"❌ Lỗi Business → Mart: {e}")

#     finally:
#         spark.stop()


def transform_business_to_mart(business_path: str, mart_output_path: str):
    """
    Chuyển dữ liệu từ tầng Business sang tầng Mart:
    - Tổng hợp theo tháng
    - Tính trung bình giá đóng cửa (Average_Close)
    - Tính tổng khối lượng giao dịch (Total_Volume)
    """

    spark = SparkSession.builder.appName("BusinessToMartStockETL").getOrCreate()

    try:
        abs_business = os.path.abspath(business_path)
        abs_mart = os.path.abspath(mart_output_path)

        if not os.path.exists(abs_business):
            raise FileNotFoundError(f"❌ Không tìm thấy dữ liệu Business tại: {abs_business}")
        print(f"📥 Đang đọc dữ liệu Business từ: {abs_business}")

        df = spark.read.parquet(abs_business)
        original_count = df.count()
        print(f"🔢 Số dòng ban đầu: {original_count}")

        if original_count == 0:
            raise ValueError("❌ Dữ liệu Business rỗng.")

        # Kiểm tra schema bắt buộc
        required_columns = {"Date", "Close", "Volume"}
        missing = required_columns - set(df.columns)
        if missing:
            raise ValueError(f"❌ Thiếu các cột bắt buộc trong Business: {missing}")

        # Loại bỏ dòng thiếu dữ liệu quan trọng
        df = df.dropna(subset=["Date", "Close", "Volume"])
        df = df.filter((col("Close") >= 0) & (col("Volume") >= 0))

        cleaned_count = df.count()
        print(f"✅ Sau làm sạch: {cleaned_count} dòng (mất {original_count - cleaned_count} dòng)")

        # Thêm cột Year/Month nếu chưa có (tránh overwrite nếu đã có sẵn)
        if "Year" not in df.columns or "Month" not in df.columns:
            df = df \
                .withColumn("Year", year(col("Date"))) \
                .withColumn("Month", month(col("Date")))

        # Tổng hợp theo tháng
        df_monthly = df.groupBy("Year", "Month").agg(
            avg("Close").alias("Average_Close"),
            sum("Volume").alias("Total_Volume")
        ).orderBy("Year", "Month")

        print("📊 Dữ liệu Mart (Monthly Summary):")
        df_monthly.show(10)

        # Ghi xuống tầng Mart
        df_monthly.write.mode("overwrite").parquet(abs_mart)
        print(f"💾 Đã ghi dữ liệu Mart vào: {abs_mart}")

    except Exception as e:
        print(f"❌ Lỗi Business → Mart: {e}")

    finally:
        spark.stop()
