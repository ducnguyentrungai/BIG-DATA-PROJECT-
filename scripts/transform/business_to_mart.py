from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, avg, sum
import os

def transform_business_to_mart(business_path: str, mart_output_path: str):

    spark = SparkSession.builder \
        .appName("BusinessToMartStockETL") \
        .getOrCreate()

    try:
        abs_business = os.path.abspath(business_path)
        abs_mart = os.path.abspath(mart_output_path)
        print(f"📥 Đọc business từ: {abs_business}")

        if not os.path.exists(abs_business):
            raise FileNotFoundError(f"❌ Không tìm thấy path: {abs_business}")

        df = spark.read.parquet(abs_business)
        print(f"🔢 Business row count: {df.count()}")
        df.printSchema()
        df.show(5)

        # 👉 Chuẩn hóa lại tên cột nếu cần
        columns = df.columns
        if "last(Close)" in columns:
            df = df.withColumnRenamed("last(Close)", "Close")
        if "sum(Volume)" in columns:
            df = df.withColumnRenamed("sum(Volume)", "Volume")
        if "first(Open)" in columns:
            df = df.withColumnRenamed("first(Open)", "Open")
        if "max(High)" in columns:
            df = df.withColumnRenamed("max(High)", "High")
        if "min(Low)" in columns:
            df = df.withColumnRenamed("min(Low)", "Low")

        # 👉 Bắt đầu transform theo tháng
        df_monthly = df \
            .withColumn("Month", month(col("Date"))) \
            .withColumn("Year", year(col("Date"))) \
            .groupBy("Year", "Month") \
            .agg(
                avg("Close").alias("Average_Close"),
                sum("Volume").alias("Total_Volume")
            ).orderBy("Year", "Month")

        print("📊 Preview mart data:")
        df_monthly.show(5)

        print(f"💾 Ghi mart vào: {abs_mart}")
        df_monthly.write.mode("overwrite").parquet(abs_mart)
        print("✅ Ghi mart thành công!")

    except Exception as e:
        print(f"❌ Lỗi khi tạo Mart: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    business_path = "warehouse/business/stock_aapl_business.parquet"
    mart_output_path = "warehouse/mart/stock_aapl_monthly_summary.parquet"
    
    transform_business_to_mart(
        os.path.join(base_dir, business_path),
        os.path.join(base_dir, mart_output_path)
    )
