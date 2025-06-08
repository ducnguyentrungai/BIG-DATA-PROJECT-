import os
import sys
import traceback
from datetime import datetime
import pandas as pd

from extract.download_stock import extract_stock_data
from transform.clean_and_save_csv import transform_stock_data
from transform.staging_to_core import transform_staging_to_core
from transform.core_to_business import transform_core_to_business
from transform.business_to_mart import transform_business_to_mart

from modeling.train.train_model import train_xgb_model
from modeling.predict.predict_next_month import predict_next_month

from load.save_parquet import save_to_parquet
from load.load_mart_to_postgres import load_mart_to_postgres
from load.save_forecast_to_postgres import save_forecast_to_postgres


def run_pipeline(symbol: str, csv_path: str,
                 staging_parquet_path: str, core_parquet_path: str,
                 business_parquet_path: str, mart_parquet_path: str,
                 model_path: str = "models/xgb_model.pkl") -> bool:
    """
    Pipeline ETL + ML cho dữ liệu chứng khoán.
    """
    try:
        print(f"🚀 [START] Pipeline bắt đầu cho symbol: {symbol}")

        
        data = extract_stock_data(symbol)
        if data.empty:
            raise ValueError("❌ B1: Dữ liệu extract rỗng.")
        print(f"✅ B1: Extracted {len(data)} dòng dữ liệu.")

        print("🧹 B2: Clean + Lưu CSV + Parquet Staging...")
        data = transform_stock_data(data, csv_path)
        if data.empty:
            raise ValueError("❌ B2: Dữ liệu staging sau khi clean bị rỗng.")
        save_to_parquet(data, staging_parquet_path)

        print("⚙️ B3: Transform staging → core...")
        df_core = transform_staging_to_core(staging_parquet_path, core_parquet_path)
        if df_core is None:
            raise ValueError("❌ B3: Không nhận được DataFrame từ transform_staging_to_core.")

        print("📊 B4: Transform core → business...")
        transform_core_to_business(core_parquet_path, business_parquet_path)

        print("📈 B5: Transform business → mart...")
        df_mart = transform_business_to_mart(business_parquet_path, mart_parquet_path)

        print("🗄️ B6: Load mart → PostgreSQL...")
        load_mart_to_postgres(mart_parquet_path)

        # ✅ Bước 7: ML training + forecasting
        print("🤖 B7: Huấn luyện mô hình XGBoost...")
        train_xgb_model(mart_parquet_path, model_path)

        print("📤 Dự đoán tháng tiếp theo...")
        predicted_return, current_price, predicted_price = predict_next_month(mart_parquet_path, model_path)
        
        if predicted_price is not None:
            print(f"📈 Dự đoán giá Avg_Adj_Close tháng tới: {predicted_price:.2f}")
            save_forecast_to_postgres(symbol, current_price, predicted_return, predicted_price)
        else:
            raise ValueError("❌ Dự đoán tháng tới thất bại.")


        print("✅ [SUCCESS] Pipeline + Model hoàn tất!")
        return True

    except Exception as e:
        print("❌ [ERROR] Pipeline thất bại!")
        print(f"Lý do: {e}")
        traceback.print_exc()
        return False


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
        mart_parquet_path=os.path.join(base_dir, f"warehouse/mart/{symbol.lower()}_monthly_summary.parquet"),
        model_path=os.path.join(base_dir, "models/xgb_model.pkl")
    )
