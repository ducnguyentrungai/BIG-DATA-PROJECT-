from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import os

def transform_core_to_business(core_path: str, business_path: str):
    spark = SparkSession.builder \
        .appName("CoreToBusinessStockETL") \
        .getOrCreate()
    
    try:
        if not os.path.exists(core_path):
            raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
        
        print(f"📥 Đọc dữ liệu từ core: {core_path}")
        df_core = spark.read.parquet(core_path)

        if df_core.count() == 0:
            raise ValueError("❌ Dữ liệu core rỗng.")

        # Chuyển đổi ngày nếu cần
        df_core = df_core.withColumn("Date", to_date(col("Date")))

        # Tổng hợp theo ngày
        df_business = df_core.groupBy("Date").agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        ).orderBy("Date")

        # Ghi xuống Business
        df_business.write.mode("overwrite").parquet(business_path)
        print(f"✅ Business data written to: {business_path}")

        return df_business

    except Exception as e:
        print(f"❌ Lỗi trong Core → Business: {e}")
    finally:
        spark.stop()
