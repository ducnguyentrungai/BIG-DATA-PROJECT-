import os
import joblib
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from modeling.features.build_features import build_features 


def train_xgb_model(parquet_path: str, model_output_path: str):
    print("📥 Đọc dữ liệu từ tầng Mart để huấn luyện mô hình...")
    df = pd.read_parquet(parquet_path)
    print(f"🔢 Số dòng trước khi tạo feature: {len(df)}")

    # Tạo feature từ Mart
    df = build_features(df)

    print("🧠 Các cột sau khi tạo feature:")
    print(df.columns.tolist())

    # Xác định đặc trưng đầu vào và biến mục tiêu
    target = "Target_Return"
    features = [col for col in df.columns if col != target]

    if not features:
        raise ValueError("❌ Không tìm thấy các đặc trưng đầu vào.")
    if target not in df.columns:
        raise ValueError(f"❌ Thiếu cột mục tiêu '{target}' trong dữ liệu.")

    # Tách dữ liệu train/test
    X = df[features]
    y = df[target]
    print(f"🧪 Tách tập train/test: {len(X)} mẫu, {len(features)} features.")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

    # Huấn luyện mô hình
    model = XGBRegressor(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=4,
        random_state=42
    )
    model.fit(X_train, y_train)

    # Dự đoán và đánh giá
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"📊 Đánh giá mô hình:")
    print(f"   - RMSE (Root Mean Squared Error): {rmse:.4f}")
    print(f"   - MAE  (Mean Absolute Error):     {mae:.4f}")
    print(f"   - R²   (R-squared score):         {r2:.4f}")

    # Lưu mô hình
    os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
    joblib.dump(model, model_output_path)
    print(f"✅ Đã lưu mô hình tại: {model_output_path}")
