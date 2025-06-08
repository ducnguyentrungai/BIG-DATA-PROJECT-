from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, round, log, exp, year, month, dayofmonth, weekofyear,
    avg, stddev, sum as spark_sum, max as spark_max
)
import os

def transform_core_to_business(core_path: str, business_path: str):
    spark = SparkSession.builder.appName("CoreToBusinessStockETL").getOrCreate()

    try:
        if not os.path.exists(core_path):
            raise FileNotFoundError(f"❌ Không tìm thấy thư mục core: {core_path}")
        print(f"📥 Đọc dữ liệu từ Core: {core_path}")

        df = spark.read.parquet(core_path)
        original_count = df.count()
        print(f"📊 Số dòng từ Core: {original_count}")
        if original_count == 0:
            raise ValueError("❌ Dữ liệu Core rỗng.")

        # Làm sạch nhẹ và sắp xếp
        df = df.filter(
            col("Date").isNotNull() &
            col("Open").isNotNull() &
            col("Close").isNotNull() &
            col("Adj Close").isNotNull()
        ).orderBy("Date")

        # Khung thời gian
        window_lag = Window.orderBy("Date")
        window_rolling = Window.orderBy("Date").rowsBetween(-19, 0)

        # Tính toán các chỉ số kỹ thuật
        df = df \
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

        final_count = df.count()
        print(f"✅ Đã tính toán và làm giàu dữ liệu Business: {final_count} dòng")

        df.select(spark_max("Date").alias("Max_Date")).show()

        df.write.mode("overwrite").parquet(business_path)
        print(f"✅ Đã ghi dữ liệu Business vào: {business_path}")
        return df

    except Exception as e:
        print(f"❌ Lỗi Core → Business: {e}")
        return None
    finally:
        spark.stop()
