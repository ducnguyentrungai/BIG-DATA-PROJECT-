# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date
# from pyspark.sql.types import DoubleType
# import os
# def transform_staging_to_core(staging_path: str, core_output_path: str):
#     """
#     Đọc dữ liệu từ tầng Staging (Parquet), làm sạch và chuẩn hóa, ghi vào tầng Core.

#     Args:
#         staging_path (str): Đường dẫn tới file Parquet ở tầng staging.
#         core_output_path (str): Đường dẫn để ghi dữ liệu đã chuẩn hóa sang core.
#     """
#     # Khởi tạo SparkSession
#     spark = SparkSession.builder \
#         .appName("StagingToCoreStockETL") \
#         .getOrCreate()

#     try:
#         if not os.path.exists(staging_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy file staging: {staging_path}")
#         print("✅ File staging tồn tại, bắt đầu xử lý...")

#         # Đọc dữ liệu từ Staging
#         df = spark.read.parquet(staging_path)

#         # Làm sạch và chuẩn hóa dữ liệu
#         df_cleaned = df \
#             .withColumn("Date", to_date(col("Date"))) \
#             .withColumn("Open", col("Open").cast(DoubleType())) \
#             .withColumn("High", col("High").cast(DoubleType())) \
#             .withColumn("Low", col("Low").cast(DoubleType())) \
#             .withColumn("Close", col("Close").cast(DoubleType())) \
#             .withColumn("Volume", col("Volume").cast(DoubleType())) \
#             .dropna(subset=["Date", "Open", "Close"]) \
#             .dropDuplicates(["Date"]) \
#             .filter((col("Open") >= 0) & (col("Close") >= 0))

#         # Ghi dữ liệu sang Core
#         df_cleaned.write.mode("overwrite").parquet(core_output_path)
#         print(f"✅ Core data written to: {core_output_path}")

#         return df_cleaned  # Trả ra DataFrame nếu cần dùng tiếp

#     except Exception as e:
#         print(f"❌ Lỗi khi xử lý staging → core: {e}")

#     finally:
#         spark.stop()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, year, month, dayofmonth, weekofyear, round
# from pyspark.sql.types import DoubleType
# import os

# def transform_staging_to_core(staging_path: str, core_output_path: str):
#     """
#     Chuyển đổi dữ liệu từ tầng Staging → Core:
#     - Làm sạch dữ liệu, chuẩn hóa kiểu
#     - Bổ sung cột phân tích thời gian và chỉ số tăng giảm giá
#     - Ghi ra file Parquet dùng cho phân tích Superset

#     Args:
#         staging_path (str): Đường dẫn file parquet staging.
#         core_output_path (str): Đường dẫn để ghi parquet đã chuẩn hóa.

#     Returns:
#         DataFrame hoặc None nếu lỗi
#     """
#     spark = SparkSession.builder.appName("StagingToCoreStockETL").getOrCreate()

#     try:
#         if not os.path.exists(staging_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy file staging: {staging_path}")
#         print("📥 Đang đọc dữ liệu staging...")

#         # Đọc parquet
#         df = spark.read.parquet(staging_path)

#         # In schema staging
#         print("📜 Schema staging:")
#         df.printSchema()

#         # Thử count (có thể gây lỗi nếu file corrupt)
#         try:
#             row_count = df.count()
#             print(f"🔎 Dữ liệu gốc có {row_count} dòng")
#         except Exception as err:
#             print(f"⚠️ Không thể đếm dòng staging: {err}")
#             df.show(5)

#         # Làm sạch và chuẩn hóa
#         df_cleaned = df \
#             .withColumn("Date", to_date(col("Date"))) \
#             .withColumn("Open", col("Open").cast(DoubleType())) \
#             .withColumn("High", col("High").cast(DoubleType())) \
#             .withColumn("Low", col("Low").cast(DoubleType())) \
#             .withColumn("Close", col("Close").cast(DoubleType())) \
#             .withColumn("Volume", col("Volume").cast(DoubleType())) \
#             .dropna(subset=["Date", "Open", "Close"]) \
#             .dropDuplicates(["Date"]) \
#             .filter((col("Open") >= 0) & (col("Close") >= 0))

#         # Bổ sung cột phục vụ phân tích thời gian
#         df_cleaned = df_cleaned \
#             .withColumn("Year", year(col("Date"))) \
#             .withColumn("Month", month(col("Date"))) \
#             .withColumn("Day", dayofmonth(col("Date"))) \
#             .withColumn("Week", weekofyear(col("Date")))

#         # Tính toán biến động giá
#         df_cleaned = df_cleaned \
#             .withColumn("Change", round(col("Close") - col("Open"), 6)) \
#             .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4))

#         # In schema sau khi xử lý
#         print("📑 Schema sau xử lý:")
#         df_cleaned.printSchema()

#         # Kiểm tra số dòng sau xử lý
#         try:
#             cleaned_count = df_cleaned.count()
#             print(f"✅ Dữ liệu sạch có {cleaned_count} dòng")
#         except Exception as count_err:
#             print(f"⚠️ Không thể count dữ liệu sạch: {count_err}")
#             df_cleaned.show(5)

#         # Ghi parquet xuống tầng Core
#         df_cleaned.write.mode("overwrite").parquet(core_output_path)
#         print(f"💾 Đã lưu Core data tại: {core_output_path}")

#         return df_cleaned

#     except Exception as e:
#         print(f"❌ Lỗi khi xử lý Staging → Core: {e}")
#         return None

#     finally:
#         spark.stop()


# if __name__ == "__main__":
#     # ✅ Thiết lập đường dẫn tuyệt đối đến staging và core
#     base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
#     symbol = "AAPL"

#     staging_path = os.path.join(base_dir, f"warehouse/staging/{symbol.lower()}.parquet")
#     core_path = os.path.join(base_dir, f"warehouse/core/{symbol.lower()}_core.parquet")

#     print(f"🧪 Bắt đầu kiểm tra Staging → Core cho symbol: {symbol}")
#     print(f"📂 Staging path: {staging_path}")
#     print(f"📂 Core output path: {core_path}")

#     result = transform_staging_to_core(staging_path, core_path)

#     if result is not None:
#         print("✅ [DONE] Hàm transform_staging_to_core chạy thành công.")
#     else:
#         print("❌ [FAILED] Có lỗi xảy ra khi xử lý.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, weekofyear, round
from pyspark.sql.types import DoubleType
import os

def transform_staging_to_core(staging_path: str, core_output_path: str):
    spark = SparkSession.builder.appName("StagingToCoreStockETL").getOrCreate()

    try:
        if not os.path.exists(staging_path):
            raise FileNotFoundError(f"❌ Không tìm thấy file staging: {staging_path}")
        print("📥 Đang đọc dữ liệu staging...")

        df = spark.read.parquet(staging_path)
        df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

        print("📜 Schema staging:")
        df.printSchema()
        original_count = df.count()
        print(f"🔢 Dữ liệu gốc: {original_count} dòng")

        # Chuẩn hóa kiểu dữ liệu
        df = df \
            .withColumn("Open", col("Open").cast(DoubleType())) \
            .withColumn("High", col("High").cast(DoubleType())) \
            .withColumn("Low", col("Low").cast(DoubleType())) \
            .withColumn("Close", col("Close").cast(DoubleType())) \
            .withColumn("Volume", col("Volume").cast(DoubleType()))

        # Làm sạch mềm (chỉ loại bỏ dòng có Date null và dữ liệu âm)
        df_cleaned = df \
            .filter(col("Date").isNotNull()) \
            .filter((col("Open") >= 0) & (col("Close") >= 0) &
                    (col("High") >= 0) & (col("Low") >= 0) & (col("Volume") >= 0)) \
            .dropDuplicates(["Date"])

        cleaned_count = df_cleaned.count()
        print(f"✅ Dữ liệu sau làm sạch: {cleaned_count} dòng (mất {original_count - cleaned_count})")

        # Bổ sung phân tích thời gian
        df_cleaned = df_cleaned \
            .withColumn("Year", year(col("Date"))) \
            .withColumn("Month", month(col("Date"))) \
            .withColumn("Day", dayofmonth(col("Date"))) \
            .withColumn("Week", weekofyear(col("Date")))

        # Tính biến động giá
        df_cleaned = df_cleaned \
            .withColumn("Change", round(col("Close") - col("Open"), 6)) \
            .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4))

        df_cleaned.write.mode("overwrite").parquet(core_output_path)
        print(f"💾 Đã ghi dữ liệu Core tại: {core_output_path}")
        return df_cleaned

    except Exception as e:
        print(f"❌ Lỗi Staging → Core: {e}")
        return None
    finally:
        spark.stop()
