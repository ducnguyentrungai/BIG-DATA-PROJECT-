import pandas as pd
from sqlalchemy import create_engine

def load_mart_to_postgres(mart_parquet_path: str, table_name: str = "stock_monthly_summary"):
    print("📥 Đang ghi dữ liệu từ mart vào PostgreSQL...")

    df = pd.read_parquet(mart_parquet_path)
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")  # tên host: postgres

    df.to_sql(table_name, engine, if_exists="replace", index=False)

    print(f"✅ Đã ghi dữ liệu vào bảng '{table_name}' trong PostgreSQL")
