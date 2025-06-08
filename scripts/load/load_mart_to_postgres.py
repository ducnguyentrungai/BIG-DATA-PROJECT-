import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def load_mart_to_postgres(mart_parquet_path: str, table_name: str = "stock_monthly_summary"):
    """
    Load dữ liệu từ Parquet tầng Mart vào PostgreSQL. Ghi đè bảng nếu đã tồn tại.
    """
    try:
        print(f"📥 Đọc dữ liệu từ parquet: {mart_parquet_path}")
        if not os.path.exists(mart_parquet_path):
            raise FileNotFoundError(f"❌ Không tìm thấy file parquet: {mart_parquet_path}")

        df = pd.read_parquet(mart_parquet_path)
        if df.empty:
            raise ValueError("⚠️ Dữ liệu parquet rỗng.")

        # ✅ Kiểm tra đầy đủ các cột mong muốn từ tầng Mart
        expected_columns = {
            "Year", "Month", "Average_Open", "Avg_Adj_Close", "Average_VWAP", "Average_MA20",
            "Average_STD20", "Average_Return", "Average_GapPercent", "Average_CumulativeReturn",
            "Total_Volume", "Max_UpperBand", "Min_LowerBand", "Last_Date"
        }

        missing = expected_columns - set(df.columns)
        if missing:
            raise ValueError(f"❌ Schema sai: thiếu cột {missing}")

        print(f"📊 Dữ liệu: {len(df)} dòng")
        print(f"📋 Schema: {list(df.columns)}")

        # ✅ Ghi vào PostgreSQL
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"✅ Đã ghi vào bảng '{table_name}' trong PostgreSQL")

        return True

    except FileNotFoundError as fnf:
        print(fnf)
    except SQLAlchemyError as db_err:
        print(f"❌ Lỗi PostgreSQL: {db_err}")
    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")

    return False

