import os
import pandas as pd

def transform_stock_data(data: pd.DataFrame, file_name: str) -> pd.DataFrame:
    # Chuẩn hóa dữ liệu mới
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')
    data.dropna(subset=['Date'], inplace=True)
    data.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    
    # Tạo thư mục nếu chưa có
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    if os.path.exists(file_name):
        print("🔄 Merging with existing CSV...")
        existing = pd.read_csv(file_name)
        existing['Date'] = pd.to_datetime(existing['Date'], errors='coerce')
        existing.dropna(subset=['Date'], inplace=True)
        existing.drop_duplicates(subset=['Date'], keep='last', inplace=True)

        # Gộp và xóa trùng theo 'Date'
        combined = pd.concat([existing, data])
        combined.drop_duplicates(subset=['Date'], keep='last', inplace=True)
        combined.sort_values(by='Date', inplace=True)

        # Ghi đè vào cùng 1 file
        combined.to_csv(file_name, index=False)
        print(f"✅ CSV updated: {file_name}")
        return combined
    else:
        # Nếu chưa tồn tại, ghi mới
        data.sort_values(by='Date', inplace=True)
        data.to_csv(file_name, index=False)
        print(f"✅ CSV created: {file_name}")
        return data
