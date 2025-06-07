# from pyspark.sql import SparkSession

# import os

# from pyspark.sql.window import Window
# from pyspark.sql.functions import lag, when, lit, max as spark_max
# from pyspark.sql.functions import col, to_date, year, month, dayofmonth, weekofyear, round

# def transform_core_to_business(core_path: str, business_path: str):
#     spark = SparkSession.builder.appName("CoreToBusinessStockETL").getOrCreate()

#     try:
#         if not os.path.exists(core_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
#         print(f"📥 Đọc dữ liệu từ Core: {core_path}")

#         df_core = spark.read.parquet(core_path)
#         original_count = df_core.count()
#         print(f"📊 Dòng Core: {original_count}")

#         df = df_core.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

#         # Lọc giữ lại dòng hợp lệ nhưng không loại bỏ quá mạnh
#         df = df.filter(col("Date").isNotNull() &
#                        (col("Open") >= 0) & (col("Close") >= 0))

#         df = df.orderBy("Date")
#         window_spec = Window.orderBy("Date")
#         df = df.withColumn("Prev_Close", lag("Close").over(window_spec))

#         df = df \
#             .withColumn("Change", round(col("Close") - col("Open"), 6)) \
#             .withColumn("ChangePercent", round((col("Close") - col("Open")) / col("Open") * 100, 4)) \
#             .withColumn("GapPercent", when(col("Prev_Close").isNotNull(),
#                                            round((col("Close") - col("Prev_Close")) / col("Prev_Close") * 100, 4))
#                         .otherwise(lit(None))) \
#             .withColumn("Year", year(col("Date"))) \
#             .withColumn("Month", month(col("Date"))) \
#             .withColumn("Day", dayofmonth(col("Date"))) \
#             .withColumn("Week", weekofyear(col("Date")))

#         final_count = df.count()
#         print(f"✅ Dữ liệu Business: {final_count} dòng (mất {original_count - final_count} dòng nếu có)")

#         print("📆 Khoảng thời gian dữ liệu:")
#         df.select(spark_max("Date").alias("Max_Date")).show()
        
#         df.write.mode("overwrite").parquet(business_path)
#         print(f"✅ Đã ghi dữ liệu Business vào: {business_path}")
#         return df

#     except Exception as e:
#         print(f"❌ Lỗi Core → Business: {e}")
#     finally:
#         spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, weekofyear,
    lag, when, lit, round, max as spark_max
)
from pyspark.sql.window import Window
import os

def transform_core_to_business(core_path: str, business_path: str):
    spark = SparkSession.builder.appName("CoreToBusinessStockETL").getOrCreate()

    try:
        if not os.path.exists(core_path):
            raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
        print(f"📥 Đọc dữ liệu từ Core: {core_path}")

        df = spark.read.parquet(core_path)
        original_count = df.count()
        print(f"📊 Dòng Core: {original_count}")

        if original_count == 0:
            raise ValueError("❌ Dữ liệu Core rỗng.")

        # Làm sạch nhẹ – đảm bảo Date và Open/Close tồn tại
        df = df.filter(
            col("Date").isNotNull() &
            col("Open").isNotNull() &
            col("Close").isNotNull()
        )

        # Chuyển về thứ tự thời gian
        df = df.orderBy("Date")

        # Tính Prev_Adj_Close nếu thiếu và cột Adj Close có tồn tại
        if "Adj Close" in df.columns and "Prev_Adj_Close" not in df.columns:
            df = df.withColumn("Prev_Adj_Close", lag("Adj Close").over(Window.orderBy("Date")))

        # Tính thêm GapPercent nếu chưa có (dựa trên Prev_Adj_Close)
        if "GapPercent" not in df.columns and "Prev_Adj_Close" in df.columns:
            df = df.withColumn("GapPercent", when(
                col("Prev_Adj_Close").isNotNull(),
                round((col("Open") - col("Prev_Adj_Close")) / col("Prev_Adj_Close") * 100, 4)
            ).otherwise(lit(None)))

        # Bổ sung các trường thời gian nếu thiếu
        for time_col, func in {
            "Year": year,
            "Month": month,
            "Day": dayofmonth,
            "Week": weekofyear
        }.items():
            if time_col not in df.columns:
                df = df.withColumn(time_col, func(col("Date")))

        # Hiển thị kết quả
        final_count = df.count()
        print(f"✅ Dữ liệu Business: {final_count} dòng (không mất chỉ số từ Core)")

        df.select(spark_max("Date").alias("Max_Date")).show()

        # Ghi dữ liệu sang tầng Business
        df.write.mode("overwrite").parquet(business_path)
        print(f"✅ Đã ghi dữ liệu Business vào: {business_path}")
        return df

    except Exception as e:
        print(f"❌ Lỗi Core → Business: {e}")
        return None
    finally:
        spark.stop()
