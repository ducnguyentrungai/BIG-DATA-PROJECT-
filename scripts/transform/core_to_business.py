# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date
# import os

# def transform_core_to_business(core_path: str, business_path: str):
#     spark = SparkSession.builder \
#         .appName("CoreToBusinessStockETL") \
#         .getOrCreate()
    
#     try:
#         if not os.path.exists(core_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
        
#         print(f"📥 Đọc dữ liệu từ core: {core_path}")
#         df_core = spark.read.parquet(core_path)

#         if df_core.count() == 0:
#             raise ValueError("❌ Dữ liệu core rỗng.")

#         # Chuyển đổi ngày nếu cần
#         df_core = df_core.withColumn("Date", to_date(col("Date")))

#         # Tổng hợp theo ngày
#         df_business = df_core.groupBy("Date").agg(
#             {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
#         ).orderBy("Date")

#         # Ghi xuống Business
#         df_business.write.mode("overwrite").parquet(business_path)
#         print(f"✅ Business data written to: {business_path}")

#         return df_business

#     except Exception as e:
#         print(f"❌ Lỗi trong Core → Business: {e}")
#     finally:
#         spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round, lag
from pyspark.sql.window import Window
import os

# def transform_core_to_business(core_path: str, business_path: str):
#     """
#     Chuyển dữ liệu từ tầng Core sang tầng Business để phục vụ phân tích BI:
#     - Đảm bảo dữ liệu sạch, đầy đủ
#     - Bổ sung các chỉ số phân tích như thay đổi giá, phần trăm thay đổi

#     Args:
#         core_path (str): Đường dẫn Parquet tầng Core.
#         business_path (str): Đường dẫn ghi dữ liệu tầng Business.
#     """
#     # Tạo SparkSession
#     spark = SparkSession.builder \
#         .appName("CoreToBusinessStockETL") \
#         .getOrCreate()

#     try:
#         # Kiểm tra tồn tại
#         if not os.path.exists(core_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
#         print(f"📥 Đọc dữ liệu từ Core: {core_path}")

#         # Đọc dữ liệu parquet từ core
#         df_core = spark.read.parquet(core_path)

#         if df_core.count() == 0:
#             raise ValueError("❌ Dữ liệu core rỗng.")

#         # Chuyển đổi Date sang chuẩn datetime nếu cần
#         df = df_core.withColumn("Date", to_date(col("Date")))

#         # Lọc những dòng null (nếu còn sót lại)
#         df = df.dropna(subset=["Date", "Open", "Close"])

#         # Sắp xếp theo thời gian để tính toán chỉ số
#         df = df.orderBy("Date")

#         # Sử dụng window function để lấy giá hôm trước
#         window_spec = Window.orderBy("Date")
#         df = df.withColumn("Prev_Close", lag("Close").over(window_spec))

#         # Tính các chỉ số phân tích
#         df = df \
#             .withColumn("Change", round(col("Close") - col("Open"), 6)) \
#             .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4)) \
#             .withColumn("GapPercent", round((col("Close") - col("Prev_Close")) / col("Prev_Close") * 100, 4))

#         # In schema và preview
#         print("📊 Schema business:")
#         df.printSchema()
#         df.show(5)

#         # Ghi xuống tầng business
#         df.write.mode("overwrite").parquet(business_path)
#         print(f"✅ Đã ghi dữ liệu business vào: {business_path}")

#         return df

#     except Exception as e:
#         print(f"❌ Lỗi khi xử lý Core → Business: {e}")
#     finally:
#         spark.stop()


from pyspark.sql.window import Window
from pyspark.sql.functions import lag, when, lit, max as spark_max
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, weekofyear, round

def transform_core_to_business(core_path: str, business_path: str):
    spark = SparkSession.builder.appName("CoreToBusinessStockETL").getOrCreate()

    try:
        if not os.path.exists(core_path):
            raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
        print(f"📥 Đọc dữ liệu từ Core: {core_path}")

        df_core = spark.read.parquet(core_path)
        original_count = df_core.count()
        print(f"📊 Dòng Core: {original_count}")

        df = df_core.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

        # Lọc giữ lại dòng hợp lệ nhưng không loại bỏ quá mạnh
        df = df.filter(col("Date").isNotNull() &
                       (col("Open") >= 0) & (col("Close") >= 0))

        df = df.orderBy("Date")
        window_spec = Window.orderBy("Date")
        df = df.withColumn("Prev_Close", lag("Close").over(window_spec))

        df = df \
            .withColumn("Change", round(col("Close") - col("Open"), 6)) \
            .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4)) \
            .withColumn("GapPercent", when(col("Prev_Close").isNotNull(),
                                           round((col("Close") - col("Prev_Close")) / col("Prev_Close") * 100, 4))
                        .otherwise(lit(None))) \
            .withColumn("Year", year(col("Date"))) \
            .withColumn("Month", month(col("Date"))) \
            .withColumn("Day", dayofmonth(col("Date"))) \
            .withColumn("Week", weekofyear(col("Date")))

        final_count = df.count()
        print(f"✅ Dữ liệu Business: {final_count} dòng (mất {original_count - final_count} dòng nếu có)")

        print("📆 Khoảng thời gian dữ liệu:")
        df.select(spark_max("Date").alias("Max_Date")).show()

        df.write.mode("overwrite").parquet(business_path)
        print(f"✅ Đã ghi dữ liệu Business vào: {business_path}")
        return df

    except Exception as e:
        print(f"❌ Lỗi Core → Business: {e}")
    finally:
        spark.stop()
