import pandas as pd
import joblib
from modeling.features.build_features import build_features

# def predict_next_month(parquet_path: str, model_path: str) -> float:
#     print("📤 Đang dự đoán tháng tiếp theo...")

#     df = pd.read_parquet(parquet_path)
#     model = joblib.load(model_path)

#     # Lưu giá hiện tại trước khi transform
#     original_latest = df.tail(1).copy()

#     # Tạo đặc trưng
#     df = build_features(df)
#     latest = df.tail(1)

#     model_features = model.get_booster().feature_names
#     X_latest = latest[model_features]

#     # Dự đoán
#     predicted_return = model.predict(X_latest)[0]
#     current_price = original_latest["Avg_Adj_Close"].values[0]
#     next_price_est = current_price * (1 + predicted_return)

#     print(f"📈 Dự đoán Target_Return: {predicted_return:.4f}")
#     print(f"💰 Avg_Adj_Close hiện tại: {current_price:.2f}")
#     print(f"➡️ Ước lượng Avg_Adj_Close tháng tới: {next_price_est:.2f}")
#     return next_price_est
import pandas as pd
import joblib
from modeling.features.build_features import build_features

def predict_next_month(parquet_path: str, model_path: str):
    df = pd.read_parquet(parquet_path)
    model = joblib.load(model_path)

    original_latest = df.tail(1).copy()
    df = build_features(df)
    latest = df.tail(1)

    model_features = model.get_booster().feature_names
    X_latest = latest[model_features]

    predicted_return = model.predict(X_latest)[0]
    current_price = original_latest["Avg_Adj_Close"].values[0]
    predicted_price = current_price * (1 + predicted_return)

    return predicted_return, current_price, predicted_price
