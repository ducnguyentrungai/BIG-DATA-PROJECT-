import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from extract.download_stock import extract_stock_data
from transform.clean_and_save_csv import transform_stock_data
from transform.staging_to_core import transform_staging_to_core
from transform.core_to_business import transform_core_to_business
from transform.business_to_mart import transform_business_to_mart
from load.send_to_kafka import load_to_kafka
from load.save_parquet import save_to_parquet
from load.load_mart_to_postgres import load_mart_to_postgres


def run_pipeline(symbol: str, csv_path: str, kafka_topic: str, 
                 staging_parquet_path: str, core_parquet_path: str, 
                 business_parquet_path: str, mart_parquet_path: str):
    
    # Bước 1: Extract
    print("🔍 Extracting stock data...")
    data = extract_stock_data(symbol)
    
    # Bước 2: Transform → CSV thô + chuẩn hoá staging
    print("🧹 Transform to staging...")
    data = transform_stock_data(data, csv_path)
    save_to_parquet(data, staging_parquet_path)
    
    # Bước 3: Staging → Core
    print("⚙️ Transform staging → core...")
    df_core = transform_staging_to_core(staging_parquet_path, core_parquet_path)
    
    # Bước 4: Core → Business
    print("📊 Transform core → business...")
    transform_core_to_business(core_parquet_path, business_parquet_path)
    
    # Bước 5: Business → Mart
    print("📈 Transform business → mart...")
    transform_business_to_mart(business_parquet_path, mart_parquet_path)

    # Bước 6: Load to Kafka
    print("📤 Sending to Kafka...")
    load_to_kafka(data, kafka_topic)

    # Bước 7: Load Mart → PostgreSQL
    print("🗄️ Load mart → PostgreSQL...")
    load_mart_to_postgres(mart_parquet_path)
        
    print("✅ Pipeline hoàn tất!")

if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    run_pipeline(
        symbol="AAPL",
        csv_path=os.path.join(base_dir, "warehouse/raw/stock_aapl.csv"),
        kafka_topic="stock_topic",
        staging_parquet_path=os.path.join(base_dir, "warehouse/staging/stock_aapl.parquet"),
        core_parquet_path=os.path.join(base_dir, "warehouse/core/stock_aapl_core.parquet"),
        business_parquet_path=os.path.join(base_dir, "warehouse/business/stock_aapl_business.parquet"),
        mart_parquet_path=os.path.join(base_dir, "warehouse/mart/stock_aapl_monthly_summary.parquet")
    )
