from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType
import os

def transform_staging_to_core(staging_path: str, core_output_path: str):
    """
    Đọc dữ liệu từ tầng Staging (Parquet), làm sạch và chuẩn hóa, ghi vào tầng Core.

    Args:
        staging_path (str): Đường dẫn tới file Parquet ở tầng staging.
        core_output_path (str): Đường dẫn để ghi dữ liệu đã chuẩn hóa sang core.
    """
    # Khởi tạo SparkSession
    spark = SparkSession.builder \
        .appName("StagingToCoreStockETL") \
        .getOrCreate()

    try:
        if not os.path.exists(staging_path):
            raise FileNotFoundError(f"❌ Không tìm thấy file staging: {staging_path}")
        print("✅ File staging tồn tại, bắt đầu xử lý...")

        # Đọc dữ liệu từ Staging
        df = spark.read.parquet(staging_path)

        # Làm sạch và chuẩn hóa dữ liệu
        df_cleaned = df \
            .withColumn("Date", to_date(col("Date"))) \
            .withColumn("Open", col("Open").cast(DoubleType())) \
            .withColumn("High", col("High").cast(DoubleType())) \
            .withColumn("Low", col("Low").cast(DoubleType())) \
            .withColumn("Close", col("Close").cast(DoubleType())) \
            .withColumn("Volume", col("Volume").cast(DoubleType())) \
            .dropna(subset=["Date", "Open", "Close"]) \
            .dropDuplicates(["Date"]) \
            .filter((col("Open") >= 0) & (col("Close") >= 0))

        # Ghi dữ liệu sang Core
        df_cleaned.write.mode("overwrite").parquet(core_output_path)
        print(f"✅ Core data written to: {core_output_path}")

        return df_cleaned  # Trả ra DataFrame nếu cần dùng tiếp

    except Exception as e:
        print(f"❌ Lỗi khi xử lý staging → core: {e}")

    finally:
        spark.stop()
