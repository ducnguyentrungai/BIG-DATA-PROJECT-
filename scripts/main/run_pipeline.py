    # # import os
    # # import sys
    # # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # # from extract.download_stock import extract_stock_data
    # # from transform.clean_and_save_csv import transform_stock_data
    # # from transform.staging_to_core import transform_staging_to_core
    # # from transform.core_to_business import transform_core_to_business
    # # from transform.business_to_mart import transform_business_to_mart
    # # from load.send_to_kafka import load_to_kafka
    # # from load.save_parquet import save_to_parquet
    # # from load.load_mart_to_postgres import load_mart_to_postgres


    # # def run_pipeline(symbol: str, csv_path: str, kafka_topic: str, 
    # #                  staging_parquet_path: str, core_parquet_path: str, 
    # #                  business_parquet_path: str, mart_parquet_path: str):
        
    # #     # Bước 1: Extract
    # #     print("🔍 Extracting stock data...")
    # #     data = extract_stock_data(symbol)
        
    # #     # Bước 2: Transform → CSV thô + chuẩn hoá staging
    # #     print("🧹 Transform to staging...")
    # #     data = transform_stock_data(data, csv_path)
    # #     save_to_parquet(data, staging_parquet_path)
        
    # #     # Bước 3: Staging → Core
    # #     print("⚙️ Transform staging → core...")
    # #     df_core = transform_staging_to_core(staging_parquet_path, core_parquet_path)
        
    # #     # Bước 4: Core → Business
    # #     print("📊 Transform core → business...")
    # #     transform_core_to_business(core_parquet_path, business_parquet_path)
        
    # #     # Bước 5: Business → Mart
    # #     print("📈 Transform business → mart...")
    # #     transform_business_to_mart(business_parquet_path, mart_parquet_path)

    # #     # Bước 6: Load to Kafka
    # #     print("📤 Sending to Kafka...")
    # #     load_to_kafka(data, kafka_topic)

    # #     # Bước 7: Load Mart → PostgreSQL
    # #     print("🗄️ Load mart → PostgreSQL...")
    # #     load_mart_to_postgres(mart_parquet_path)
            
    # #     print("✅ Pipeline hoàn tất!")

    # # if __name__ == "__main__":
    # #     base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

    # #     run_pipeline(
    # #         symbol="AAPL",
    # #         csv_path=os.path.join(base_dir, "warehouse/raw/stock_aapl.csv"),
    # #         kafka_topic="stock_topic",
    # #         staging_parquet_path=os.path.join(base_dir, "warehouse/staging/stock_aapl.parquet"),
    # #         core_parquet_path=os.path.join(base_dir, "warehouse/core/stock_aapl_core.parquet"),
    # #         business_parquet_path=os.path.join(base_dir, "warehouse/business/stock_aapl_business.parquet"),
    # #         mart_parquet_path=os.path.join(base_dir, "warehouse/mart/stock_aapl_monthly_summary.parquet")
    # #     )


    # import os
    # import sys
    # import traceback
    # import pandas as pd
    # from datetime import datetime

    # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # # Import các bước trong pipeline
    # from extract.download_stock import extract_stock_data
    # from transform.clean_and_save_csv import transform_stock_data
    # from transform.staging_to_core import transform_staging_to_core
    # from transform.core_to_business import transform_core_to_business
    # from transform.business_to_mart import transform_business_to_mart
    # from load.send_to_kafka import load_to_kafka
    # from load.save_parquet import save_to_parquet
    # from load.load_mart_to_postgres import load_mart_to_postgres


    # def run_pipeline(symbol: str, csv_path: str, kafka_topic: str,
    #                 staging_parquet_path: str, core_parquet_path: str,
    #                 business_parquet_path: str, mart_parquet_path: str) -> bool:
    #     """
    #     Pipeline xử lý dữ liệu chứng khoán từ extract đến load lên PostgreSQL & Kafka.

    #     Args:
    #         symbol (str): Mã chứng khoán (ví dụ: 'AAPL')
    #         csv_path (str): File CSV lưu dữ liệu thô
    #         kafka_topic (str): Kafka topic để gửi dữ liệu
    #         staging_parquet_path, core_parquet_path, ...: Các path cho tầng lưu

    #     Returns:
    #         bool: True nếu pipeline thành công, False nếu có lỗi
    #     """
    #     try:
    #         print(f"🚀 [START] Pipeline bắt đầu cho symbol: {symbol}")

    #         # Bước 1: Extract
    #         print("🔍 B1: Extract stock data...")
    #         data = extract_stock_data(symbol)
    #         if data.empty:
    #             raise ValueError("❌ B1: Dữ liệu extract rỗng.")

    #         print(f"✅ B1: Extracted {len(data)} dòng dữ liệu.")

    #         # Bước 2: Clean + Save CSV & Parquet
    #         print("🧹 B2: Clean + Lưu CSV + Parquet Staging...")
    #         data = transform_stock_data(data, csv_path)
    #         if data.empty:
    #             raise ValueError("❌ B2: Dữ liệu staging sau khi clean bị rỗng.")
    #         save_to_parquet(data, staging_parquet_path)

    #         # Bước 3: Staging → Core
    #         print("⚙️ B3: Transform staging → core...")
    #         df_core = transform_staging_to_core(staging_parquet_path, core_parquet_path)
    #         if df_core is None:
    #             raise ValueError("❌ B3: Không nhận được DataFrame từ transform_staging_to_core.")

    #         # Bước 4: Core → Business
    #         print("📊 B4: Transform core → business...")
    #         transform_core_to_business(core_parquet_path, business_parquet_path)

    #         # Bước 5: Business → Mart
    #         print("📈 B5: Transform business → mart...")
    #         transform_business_to_mart(business_parquet_path, mart_parquet_path)

    #         # Bước 6: Gửi lên Kafka
    #         print("📤 B6: Sending raw data to Kafka...")
    #         load_to_kafka(data, kafka_topic)

    #         # Bước 7: Ghi mart vào PostgreSQL
    #         print("🗄️ B7: Load mart → PostgreSQL...")
    #         load_mart_to_postgres(mart_parquet_path)

    #         print("✅ [SUCCESS] Pipeline hoàn tất!")

    #         return True

    #     except Exception as e:
    #         print("❌ [ERROR] Pipeline thất bại!")
    #         print(f"Lý do: {e}")
    #         print("📋 Stack Trace:")
    #         traceback.print_exc()
    #         return False


    # # Run trực tiếp
    # if __name__ == "__main__":
    #     base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    #     today = datetime.today().strftime("%Y-%m-%d")
    #     symbol = "AAPL"

    #     run_pipeline(
    #         symbol=symbol,
    #         csv_path=os.path.join(base_dir, f"warehouse/raw/{symbol.lower()}_{today}.csv"),
    #         kafka_topic="stock_topic",
    #         staging_parquet_path=os.path.join(base_dir, f"warehouse/staging/{symbol.lower()}.parquet"),
    #         core_parquet_path=os.path.join(base_dir, f"warehouse/core/{symbol.lower()}_core.parquet"),
    #         business_parquet_path=os.path.join(base_dir, f"warehouse/business/{symbol.lower()}_business.parquet"),
    #         mart_parquet_path=os.path.join(base_dir, f"warehouse/mart/{symbol.lower()}_monthly_summary.parquet")
    #     )

import os
import sys
import traceback
from datetime import datetime

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import các bước trong pipeline
from extract.download_stock import extract_stock_data
from transform.clean_and_save_csv import transform_stock_data
from transform.staging_to_core import transform_staging_to_core
from transform.core_to_business import transform_core_to_business
from transform.business_to_mart import transform_business_to_mart
from load.save_parquet import save_to_parquet
from load.load_mart_to_postgres import load_mart_to_postgres


def run_pipeline(symbol: str, csv_path: str,
                 staging_parquet_path: str, core_parquet_path: str,
                 business_parquet_path: str, mart_parquet_path: str) -> bool:
    """
    Pipeline xử lý dữ liệu chứng khoán từ extract đến load lên PostgreSQL.

    Args:
        symbol (str): Mã chứng khoán (ví dụ: 'AAPL')
        csv_path (str): File CSV lưu dữ liệu thô
        staging_parquet_path, core_parquet_path, ...: Các path cho tầng lưu

    Returns:
        bool: True nếu pipeline thành công, False nếu có lỗi
    """
    try:
        print(f"🚀 [START] Pipeline bắt đầu cho symbol: {symbol}")

        # Bước 1: Extract
        print("🔍 B1: Extract stock data...")
        data = extract_stock_data(symbol)
        if data.empty:
            raise ValueError("❌ B1: Dữ liệu extract rỗng.")
        print(f"✅ B1: Extracted {len(data)} dòng dữ liệu.")

        # Bước 2: Clean + Save CSV & Parquet
        print("🧹 B2: Clean + Lưu CSV + Parquet Staging...")
        data = transform_stock_data(data, csv_path)
        if data.empty:
            raise ValueError("❌ B2: Dữ liệu staging sau khi clean bị rỗng.")
        save_to_parquet(data, staging_parquet_path)

        # Bước 3: Staging → Core
        print("⚙️ B3: Transform staging → core...")
        df_core = transform_staging_to_core(staging_parquet_path, core_parquet_path)
        if df_core is None:
            raise ValueError("❌ B3: Không nhận được DataFrame từ transform_staging_to_core.")

        # Bước 4: Core → Business
        print("📊 B4: Transform core → business...")
        transform_core_to_business(core_parquet_path, business_parquet_path)

        # Bước 5: Business → Mart
        print("📈 B5: Transform business → mart...")
        transform_business_to_mart(business_parquet_path, mart_parquet_path)

        # Bước 6: Load Mart → PostgreSQL
        print("🗄️ B6: Load mart → PostgreSQL...")
        load_mart_to_postgres(mart_parquet_path)

        print("✅ [SUCCESS] Pipeline hoàn tất!")
        return True

    except Exception as e:
        print("❌ [ERROR] Pipeline thất bại!")
        print(f"Lý do: {e}")
        traceback.print_exc()
        return False


# Run trực tiếp
if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    today = datetime.today().strftime("%Y-%m-%d")
    symbol = "AAPL"

    run_pipeline(
        symbol=symbol,
        csv_path=os.path.join(base_dir, f"warehouse/raw/{symbol.lower()}_{today}.csv"),
        staging_parquet_path=os.path.join(base_dir, f"warehouse/staging/{symbol.lower()}.parquet"),
        core_parquet_path=os.path.join(base_dir, f"warehouse/core/{symbol.lower()}_core.parquet"),
        business_parquet_path=os.path.join(base_dir, f"warehouse/business/{symbol.lower()}_business.parquet"),
        mart_parquet_path=os.path.join(base_dir, f"warehouse/mart/{symbol.lower()}_monthly_summary.parquet")
    )
