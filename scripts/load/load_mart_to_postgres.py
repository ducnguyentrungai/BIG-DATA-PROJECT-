# import pandas as pd
# from sqlalchemy import create_engine

# def load_mart_to_postgres(mart_parquet_path: str, table_name: str = "stock_monthly_summary"):
#     print("📥 Đang ghi dữ liệu từ mart vào PostgreSQL...")

#     df = pd.read_parquet(mart_parquet_path)
#     engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")  # tên host: postgres

#     df.to_sql(table_name, engine, if_exists="replace", index=False)

#     print(f"✅ Đã ghi dữ liệu vào bảng '{table_name}' trong PostgreSQL")

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# def load_mart_to_postgres(mart_parquet_path: str, table_name: str = "stock_monthly_summary"):
#     """
#     Load dữ liệu từ Parquet tầng Mart vào bảng PostgreSQL.
#     Ghi đè bảng nếu đã tồn tại.

#     Args:
#         mart_parquet_path (str): Đường dẫn file parquet mart.
#         table_name (str): Tên bảng PostgreSQL đích (mặc định: stock_monthly_summary).

#     Returns:
#         bool: True nếu thành công, False nếu có lỗi.
#     """

#     try:
#         print(f"📥 Đọc dữ liệu từ parquet: {mart_parquet_path}")

#         # 1. Kiểm tra file tồn tại
#         if not os.path.exists(mart_parquet_path):
#             raise FileNotFoundError(f"❌ Không tìm thấy file parquet: {mart_parquet_path}")

#         # 2. Đọc dữ liệu parquet
#         df = pd.read_parquet(mart_parquet_path)
#         if df.empty:
#             raise ValueError("⚠️ Dữ liệu parquet rỗng.")

#         print(f"📊 Dữ liệu: {len(df)} dòng, {len(df.columns)} cột. Schema: {list(df.columns)}")

#         # 3. Kết nối tới PostgreSQL (mặc định dùng user airflow, DB airflow, host postgres)
#         engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

#         # 4. Ghi vào PostgreSQL (overwrite)
#         df.to_sql(table_name, engine, if_exists="replace", index=False)
#         print(f"✅ Đã ghi dữ liệu vào bảng '{table_name}' trong PostgreSQL")
#         return True

#     except FileNotFoundError as fnf:
#         print(fnf)
#     except SQLAlchemyError as db_err:
#         print(f"❌ Lỗi kết nối hoặc ghi dữ liệu vào PostgreSQL: {db_err}")
#     except Exception as e:
#         print(f"❌ Lỗi không xác định: {e}")

#     return False

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

        expected_columns = {"Year", "Month", "Avg_Adj_Close", "Total_Volume"}
        if not expected_columns.issubset(df.columns):
            raise ValueError(f"❌ Schema sai: thiếu cột {expected_columns - set(df.columns)}")

        print(f"📊 {len(df)} dòng, schema: {list(df.columns)}")

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
